from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.ops.replenishment_order import (
    REPLENISHMENT_ORDER_SPEC, build_replenishment_order,
)

_P = ScaleProfile("t", 5, 500, 1, 50000, 1000, 20, num_replen_orders=40)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def _snapshot(spark):
    d = 20260131  # exists in dim_date (contiguous up to end_date)
    rows = [
        (d, s, p, p % 10, 5, 3)  # available_qty = p%10, reorder_point 5, safety_stock 3
        for p in range(1, 61)
        for s in range(1, 6)
    ]
    return spark.createDataFrame(
        rows,
        "date_sk long, store_sk long, product_sk long, "
        "available_qty int, reorder_point int, safety_stock_qty int",
    )


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    snap = _snapshot(spark)
    return build_replenishment_order(spark, _CFG, fact_inventory_snapshot=snap, dim_date=dd), snap


def test_schema_and_grain(spark):
    df, _ = _build(spark)
    assert df.columns == REPLENISHMENT_ORDER_SPEC.column_names
    assert df.groupBy("replen_id").count().filter(F.col("count") > 1).count() == 0


def test_bounded_and_ri(spark):
    df, snap = _build(spark)
    assert 0 < df.count() <= _P.num_replen_orders
    src = snap.select("product_sk", "store_sk").distinct()
    assert df.select("product_sk", "store_sk").distinct() \
        .join(src, ["product_sk", "store_sk"], "left_anti").count() == 0


def test_invariants(spark):
    df, _ = _build(spark)
    assert df.filter(F.col("suggested_qty") < 0).count() == 0
    bad = df.filter(
        ((F.col("status") == "Suggested") &
         (F.col("approved_qty").isNotNull() | F.col("approved_by").isNotNull()))
        | ((F.col("status") != "Suggested") &
           (F.col("approved_qty").isNull() | F.col("approved_by").isNull()))
    )
    assert bad.count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*"), F.sum("suggested_qty")).first()
    b = _build(spark)[0].agg(F.count("*"), F.sum("suggested_qty")).first()
    assert a == b
