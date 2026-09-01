# tests/test_fact_sales_forecast.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.ai.fact_sales_forecast import (
    FACT_SALES_FORECAST_SPEC, FORECAST_VERSIONS, build_fact_sales_forecast,
)

_P = ScaleProfile("t", 20, 60, 3, 60000, 2000, 20,
                  forecast_active_products=60, forecast_horizon_weeks=26)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 20, "customer": 2000, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 60}


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=60000)
    return build_fact_sales_forecast(spark, _CFG, fact_sales_line=sales, dim_date=dd), dd


def test_schema_grain_and_versions(spark):
    df, _ = _build(spark)
    assert df.columns == FACT_SALES_FORECAST_SPEC.column_names
    grain = ["product_sk", "store_sk", "date_sk", "forecast_version"]
    assert df.groupBy(*grain).count().filter(F.col("count") > 1).count() == 0
    assert {r[0] for r in df.select("forecast_version").distinct().collect()} == set(FORECAST_VERSIONS)


def test_interval_ordering_and_ri(spark):
    df, dd = _build(spark)
    assert df.filter(~((F.col("lower_bound") <= F.col("forecast_qty")) &
                       (F.col("forecast_qty") <= F.col("upper_bound")))).count() == 0
    assert df.select("date_sk").distinct().join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*"), F.round(F.sum("forecast_qty"), 2)).first()
    b = _build(spark)[0].agg(F.count("*"), F.round(F.sum("forecast_qty"), 2)).first()
    assert a == b
