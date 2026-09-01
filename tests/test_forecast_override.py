from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.ops.forecast_override import (
    FORECAST_OVERRIDE_SPEC, build_forecast_override,
)

_P = ScaleProfile("t", 5, 500, 1, 50000, 1000, 20, num_forecast_overrides=30)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def _forecast(spark):
    d = 20260131
    rows = []
    for p in range(1, 41):
        for s in range(1, 6):
            rows.append((d, p, s, "improved", 2026, 5, 100.0 + p))
            rows.append((d, p, s, "baseline", 2026, 5, 90.0 + p))
    return spark.createDataFrame(
        rows,
        "date_sk long, product_sk long, store_sk long, forecast_version string, "
        "fiscal_year int, fiscal_week int, forecast_qty double",
    )


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    fc = _forecast(spark)
    return build_forecast_override(spark, _CFG, fact_sales_forecast=fc, dim_date=dd), fc


def test_schema_and_grain(spark):
    df, _ = _build(spark)
    assert df.columns == FORECAST_OVERRIDE_SPEC.column_names
    assert df.groupBy("override_id").count().filter(F.col("count") > 1).count() == 0


def test_bounded_and_invariants(spark):
    df, _ = _build(spark)
    assert 0 < df.count() <= _P.num_forecast_overrides
    assert df.filter(F.col("override_qty") < 0).count() == 0
    assert df.filter((F.col("override_reason") == "") | F.col("override_reason").isNull()).count() == 0
    assert df.filter((F.col("planner_id") == "") | F.col("planner_id").isNull()).count() == 0


def test_ri_only_improved(spark):
    df, fc = _build(spark)
    imp = fc.filter(F.col("forecast_version") == "improved").select("product_sk", "store_sk").distinct()
    assert df.select("product_sk", "store_sk").distinct() \
        .join(imp, ["product_sk", "store_sk"], "left_anti").count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*"), F.round(F.sum("override_qty"), 2)).first()
    b = _build(spark)[0].agg(F.count("*"), F.round(F.sum("override_qty"), 2)).first()
    assert a == b
