"""forecast_override: human-in-the-loop overrides seeded from fact_sales_forecast."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn
from .pg_write import PgTableSpec

_REASONS = ("Local promotion", "Competitor closeout", "Weather event", "Known stockout recovery")
_PLANNERS = ("planner_amir", "planner_bianca", "planner_chen", "planner_dana")

FORECAST_OVERRIDE_SPEC = PgTableSpec(
    schema="ops",
    name="forecast_override",
    grain="one row per human override of a forecast cell",
    columns=[
        SparkColumn("override_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year", nullable=False),
        SparkColumn("fiscal_week", "int", "Retail fiscal week", nullable=False),
        SparkColumn("ai_forecast_qty", "double", "AI forecast units being overridden", nullable=False),
        SparkColumn("override_qty", "double", "Planner-overridden units", nullable=False),
        SparkColumn("override_reason", "string", "Reason for the override", nullable=False),
        SparkColumn("planner_id", "string", "Planner who made the override", nullable=False),
        SparkColumn("created_at", "timestamp", "Row creation time (deterministic)", nullable=False),
        SparkColumn("updated_at", "timestamp", "Last update time (deterministic)", nullable=False),
    ],
    primary_key=("override_id",),
)


def build_forecast_override(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_forecast: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    pick = uniform_hash(F.col("product_sk"), F.col("store_sk"), F.col("date_sk"), salt="override_pick")
    cand = (
        fact_sales_forecast
        .filter(F.col("forecast_version") == F.lit("improved"))
        .select("date_sk", "product_sk", "store_sk", "fiscal_year", "fiscal_week", "forecast_qty")
        .withColumn("_r", pick)
        .orderBy("_r", "product_sk", "store_sk", "date_sk")
        .limit(sp.num_forecast_overrides)
    )

    dd = dim_date.select("date_sk", "date")
    j = cand.join(dd, "date_sk")

    keys = (F.col("product_sk"), F.col("store_sk"), F.col("date_sk"))
    delta = uniform_hash(*keys, salt="override_delta") * F.lit(0.6) - F.lit(0.3)  # ±30%
    reason_idx = bounded_int(*keys, salt="reason", lo=1, hi=len(_REASONS))
    planner_idx = bounded_int(*keys, salt="planner", lo=1, hi=len(_PLANNERS))

    df = (
        j
        .withColumn("override_id", F.xxhash64(F.col("product_sk"), F.col("store_sk"),
                                              F.col("date_sk"), F.lit("override")))
        .withColumn("ai_forecast_qty", F.col("forecast_qty"))
        .withColumn("override_qty",
                    F.greatest(F.round(F.col("forecast_qty") * (F.lit(1.0) + delta), 2), F.lit(0.0)))
        .withColumn("override_reason", F.element_at(F.array(*[F.lit(r) for r in _REASONS]), reason_idx))
        .withColumn("planner_id", F.element_at(F.array(*[F.lit(p) for p in _PLANNERS]), planner_idx))
        .withColumn("created_at", F.col("date").cast("timestamp"))
        .withColumn("updated_at", F.expr("created_at + INTERVAL 1 DAY"))
    )
    return FORECAST_OVERRIDE_SPEC.select_ordered(df)
