"""Weekly demand forecast derived from fact_sales_line, with injected anomaly divergence."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec
from .anomalies import SUPPLY_PERIOD, week_calendar

FORECAST_VERSIONS: tuple[str, str] = ("baseline", "improved")
_INTERVAL_BAND = 0.15  # ±15% prediction interval

FACT_SALES_FORECAST_SPEC = SparkTableSpec(
    schema="ai",
    name="fact_sales_forecast",
    grain="one row per product x store x fiscal week x forecast version",
    columns=[
        SparkColumn("date_sk", "long", "Week-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("forecast_version", "string", "Forecast model version", nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_week", "int", "Retail fiscal week"),
        SparkColumn("forecast_qty", "double", "Projected units"),
        SparkColumn("forecast_amount", "double", "Projected net sales amount"),
        SparkColumn("lower_bound", "double", "Lower prediction bound (qty)"),
        SparkColumn("upper_bound", "double", "Upper prediction bound (qty)"),
        SparkColumn("model_name", "string", "Forecast model name"),
        SparkColumn("forecast_generated_date", "date", "As-of date the forecast was produced"),
    ],
)


def build_fact_sales_forecast(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile
    wc = week_calendar(spark, dim_date)  # fiscal_year, fiscal_week, date_sk, fiscal_period, is_holiday_week

    # --- weekly actuals for active products ---
    fw = dim_date.select("date_sk", "fiscal_year", "fiscal_week")
    agg = (
        fact_sales_line
        .filter(F.col("product_sk") <= F.lit(sp.forecast_active_products))
        .join(fw, "date_sk")
        .groupBy("product_sk", "store_sk", "fiscal_year", "fiscal_week")
        .agg(F.sum("quantity").alias("actual_qty"),
             F.sum("net_sales_amount").alias("actual_net"))
    )

    # --- restrict to the most recent forecast_horizon_weeks distinct weeks ---
    weeks = (
        agg.select("fiscal_year", "fiscal_week").distinct()
        .orderBy(F.col("fiscal_year").desc(), F.col("fiscal_week").desc())
        .limit(sp.forecast_horizon_weeks)
    )
    agg = agg.join(F.broadcast(weeks), ["fiscal_year", "fiscal_week"])

    # --- attach week-end date_sk + anomaly flags ---
    base = agg.join(F.broadcast(wc), ["fiscal_year", "fiscal_week"])
    max_fy = dim_date.agg(F.max("fiscal_year")).first()[0]
    is_holiday = F.col("is_holiday_week") & (F.col("fiscal_year") == F.lit(max_fy))
    is_supply = (
        (F.col("fiscal_period") == F.lit(SUPPLY_PERIOD))
        & (F.col("fiscal_year") == F.lit(max_fy))
        & (F.col("product_sk") <= F.lit(max(1, sp.forecast_active_products // 4)))
    )
    anomaly_mult = F.when(is_holiday, F.lit(0.6)).when(is_supply, F.lit(1.4)).otherwise(F.lit(1.0))
    bias = uniform_hash(F.col("product_sk"), F.col("store_sk"), F.col("fiscal_week"),
                        salt="bias") * F.lit(0.10) - F.lit(0.05)  # ±5%

    # --- explode into forecast versions ---
    versioned = base.withColumn("forecast_version",
                                F.explode(F.array(*[F.lit(v) for v in FORECAST_VERSIONS])))
    avg_price = F.col("actual_net") / F.greatest(F.col("actual_qty"), F.lit(1))
    qty = F.when(
        F.col("forecast_version") == F.lit("baseline"),
        F.col("actual_qty") * (F.lit(1.0) + bias) * anomaly_mult,
    ).otherwise(F.col("actual_qty") * (F.lit(1.0) + bias * F.lit(0.4)))

    df = (
        versioned
        .withColumn("forecast_qty", F.round(F.greatest(qty, F.lit(0.0)), 2))
        .withColumn("forecast_amount", F.round(F.col("forecast_qty") * avg_price, 2))
        .withColumn("lower_bound", F.round(F.col("forecast_qty") * F.lit(1.0 - _INTERVAL_BAND), 2))
        .withColumn("upper_bound", F.round(F.col("forecast_qty") * F.lit(1.0 + _INTERVAL_BAND), 2))
        .withColumn("model_name", F.when(F.col("forecast_version") == F.lit("baseline"),
                                         F.lit("seasonal_naive_v1")).otherwise(F.lit("gbt_v2")))
        .withColumn("forecast_generated_date", F.lit(config.end_date).cast("date"))
    )
    return FACT_SALES_FORECAST_SPEC.select_ordered(df)
