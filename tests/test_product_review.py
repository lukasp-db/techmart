from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.ai.product_review import (
    PRODUCT_REVIEW_SPEC, PRODUCT_REVIEW_STAGING_SPEC, build_product_review_staging,
)

_P = ScaleProfile("t", 20, 60, 2, 40000, 2000, 20, num_reviews=150)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 20, "customer": 2000, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 60}


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=40000)
    return build_product_review_staging(spark, _CFG, fact_sales_line=sales,
                                        dim_product=dp, dim_date=dd), dp


def test_staging_schema_bounded_count_and_prompts(spark):
    df, _ = _build(spark)
    assert df.columns == PRODUCT_REVIEW_STAGING_SPEC.column_names
    # bounded by num_reviews
    assert df.count() <= _P.num_reviews
    # prompts are non-empty; final text columns are NOT present in staging
    assert df.filter((F.length("prompt") == 0) | F.col("prompt").isNull()).count() == 0
    assert "review_text" not in df.columns
    # the staging columns minus the two prompts equal the final columns minus the two text cols
    assert set(PRODUCT_REVIEW_STAGING_SPEC.column_names) - {"prompt", "title_prompt"} == \
           set(PRODUCT_REVIEW_SPEC.column_names) - {"review_text", "review_title"}


def test_verified_purchase_and_ri(spark):
    df, dp = _build(spark)
    assert df.filter(~F.col("verified_purchase")).count() == 0  # all tied to real sales
    assert df.select("product_sk").distinct() \
        .join(dp.select("product_sk"), "product_sk", "left_anti").count() == 0
    assert df.filter((F.col("rating") < 1) | (F.col("rating") > 5)).count() == 0
