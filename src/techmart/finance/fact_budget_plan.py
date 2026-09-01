"""fact_budget_plan: budget/forecast plans derived from actuals with variance."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..facts.gen import uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec

_BUDGET_SALT = 730_007

FACT_BUDGET_PLAN_SPEC = SparkTableSpec(
    schema="finance",
    name="fact_budget_plan",
    grain="one row per department × store × GL account × fiscal period × plan version",
    columns=[
        SparkColumn("date_sk", "long", "Period-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("gl_account_sk", "long", "GL account FK (dim_gl_account)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("department_sk", "long", "Department FK (dim_department)", is_key=True, nullable=False),
        SparkColumn("plan_version", "string", "Budget/Forecast/Latest-Estimate", is_key=True, nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("plan_amount", "double", "Planned amount"),
        SparkColumn("plan_units", "long", "Planned units (proxy)"),
        SparkColumn("scenario", "string", "Planning scenario"),
    ],
)

_VERSIONS = ["Budget", "Forecast", "Latest-Estimate"]


def build_fact_budget_plan(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_gl_actuals: DataFrame,
    dim_gl_account: DataFrame,
) -> DataFrame:
    var = config.scale_profile.budget_variance
    pl_sks = dim_gl_account.filter(F.col("statement") == "P&L").select("gl_account_sk")
    base = fact_gl_actuals.join(pl_sks, "gl_account_sk")
    exploded = base.withColumn("plan_version", F.explode(F.array(*[F.lit(v) for v in _VERSIONS])))
    variance = (
        uniform_hash("store_sk", "gl_account_sk", "plan_version", salt=_BUDGET_SALT) * F.lit(2.0) - F.lit(1.0)
    ) * F.lit(var)
    out = (
        exploded.withColumn("plan_amount", F.round(F.col("actual_amount") * (F.lit(1.0) + variance), 2))
        .withColumn("plan_units", F.floor(F.abs("plan_amount") / F.lit(50.0)).cast("long"))
        .withColumn("scenario", F.lit("Base"))
    )
    return FACT_BUDGET_PLAN_SPEC.select_ordered(out)
