from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.ai.service_case import (
    SERVICE_CASE_SPEC, SERVICE_CASE_STAGING_SPEC, build_service_case_staging,
)

_P = ScaleProfile("t", 20, 60, 2, 40000, 2000, 20, num_service_cases=120)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 20, "customer": 2000, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 60}


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=40000)
    return build_service_case_staging(spark, _CFG, fact_sales_line=sales, dim_date=dd)


def test_staging_schema_bounded_and_prompts(spark):
    df = _build(spark)
    assert df.columns == SERVICE_CASE_STAGING_SPEC.column_names
    assert df.count() <= _P.num_service_cases
    assert df.filter((F.length("notes_prompt") == 0) | F.col("notes_prompt").isNull()).count() == 0
    # resolution_prompt is null exactly when status is Open/In-Progress
    open_like = F.col("status").isin("Open", "In-Progress")
    assert df.filter(open_like & F.col("resolution_prompt").isNotNull()).count() == 0
    assert df.filter(~open_like & F.col("resolution_prompt").isNull()).count() == 0


def test_domain_values(spark):
    df = _build(spark)
    assert df.filter(~F.col("case_type").isin("Repair", "Warranty", "Support")).count() == 0
    assert df.filter((F.col("csat_score") < 1) | (F.col("csat_score") > 5)).count() == 0


def test_csat_correlates_with_status(spark):
    df = _build(spark)
    resolved = df.filter(F.col("status").isin("Resolved", "Closed"))
    unresolved = df.filter(F.col("status").isin("Open", "In-Progress"))
    # Resolved/Closed skew high (4-5); Open/In-Progress skew low (1-3).
    assert resolved.filter(F.col("csat_score") < 4).count() == 0
    assert unresolved.filter(F.col("csat_score") > 3).count() == 0
