"""Anomaly windows (resolved against dim_date) and the ai_anomaly_catalog table."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec

# The supply disruption is realized in this fiscal period of the latest fiscal year.
SUPPLY_PERIOD = 6

AI_ANOMALY_CATALOG_SPEC = SparkTableSpec(
    schema="ai",
    name="ai_anomaly_catalog",
    grain="one row per documented anomaly",
    columns=[
        SparkColumn("anomaly_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("anomaly_type", "string", "Anomaly category", nullable=False),
        SparkColumn("description", "string", "Human-readable narrative"),
        SparkColumn("start_date_sk", "long", "Window start date FK (dim_date)", is_key=True),
        SparkColumn("end_date_sk", "long", "Window end date FK (dim_date)", is_key=True),
        SparkColumn("affected_dimension", "string", "Dimension the anomaly acts on"),
        SparkColumn("expected_signal", "string", "What a detector should observe"),
        SparkColumn("realized_in", "string", "Where the signal is materialized"),
    ],
)


def week_calendar(spark: SparkSession, dim_date: DataFrame) -> DataFrame:
    """Per (fiscal_year, fiscal_week): week-end date_sk, fiscal_period, is_holiday_week."""
    return (
        dim_date.groupBy("fiscal_year", "fiscal_week").agg(
            F.max("date_sk").alias("date_sk"),
            F.max("fiscal_period").alias("fiscal_period"),
            F.max(F.when(F.col("selling_season") == "Holiday", F.lit(1)).otherwise(F.lit(0)))
                .alias("_hol"),
        )
        .withColumn("is_holiday_week", F.col("_hol") == F.lit(1))
        .drop("_hol")
    )


def build_ai_anomaly_catalog(
    spark: SparkSession, config: TechmartConfig, *, dim_date: DataFrame
) -> DataFrame:
    max_fy = dim_date.agg(F.max("fiscal_year")).first()[0]

    # Holiday-demand-spike window: the Holiday selling season of the latest fiscal year.
    hol = dim_date.filter(
        (F.col("fiscal_year") == F.lit(max_fy)) & (F.col("selling_season") == "Holiday")
    ).agg(F.min("date_sk").alias("s"), F.max("date_sk").alias("e")).first()

    # Vendor-supply-disruption window: SUPPLY_PERIOD of the latest fiscal year.
    sup = dim_date.filter(
        (F.col("fiscal_year") == F.lit(max_fy)) & (F.col("fiscal_period") == F.lit(SUPPLY_PERIOD))
    ).agg(F.min("date_sk").alias("s"), F.max("date_sk").alias("e")).first()

    rows = [
        (1, "holiday-demand-spike",
         "Under-forecast demand during the holiday selling season.",
         int(hol["s"]), int(hol["e"]), "product/category",
         "forecast_qty << actual for holiday weeks (baseline)", "fact_sales_forecast"),
        (2, "vendor-supply-disruption",
         "Stockouts suppress actual demand; naive forecast over-forecasts.",
         int(sup["s"]), int(sup["e"]), "vendor/product-band",
         "forecast_qty >> actual for the disruption period (baseline)", "fact_sales_forecast"),
        (3, "pricing-error",
         "A margin dip from a mispriced subcategory (documented; core-fact injection deferred).",
         int(sup["s"]), int(sup["e"]), "product/subcategory",
         "gross_margin dip localized to a subcategory", "catalog-only"),
        (4, "return-fraud-cluster",
         "A cluster of suspicious returns (documented; core-fact injection deferred).",
         int(sup["s"]), int(sup["e"]), "customer/store",
         "elevated is_fraud_suspected returns in a store", "catalog-only"),
        (5, "data-quality-blemish",
         "A data-quality blemish for cleansing demos (documented; injection deferred).",
         int(sup["s"]), int(sup["e"]), "misc",
         "null/So outlier rows in a narrow window", "catalog-only"),
    ]
    df = spark.createDataFrame(
        rows,
        "anomaly_id long, anomaly_type string, description string, start_date_sk long, "
        "end_date_sk long, affected_dimension string, expected_signal string, realized_in string",
    )
    return AI_ANOMALY_CATALOG_SPEC.select_ordered(df)
