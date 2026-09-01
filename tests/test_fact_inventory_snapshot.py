from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.spark.dimensions.dim_store import build_dim_store
from techmart.facts.fact_inventory_snapshot import (
    FACT_INVENTORY_SNAPSHOT_SPEC,
    build_fact_inventory_snapshot,
)

_P = ScaleProfile("t", 3, 20, 1, 3000, 200, 20, inventory_snapshot_days=10)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def _build(spark):
    return build_fact_inventory_snapshot(
        spark, _CFG,
        dim_store=build_dim_store(spark, _CFG),
        dim_product=build_dim_product(spark, _CFG),
        dim_date=build_dim_date(spark, _CFG),
    )


def test_schema_and_grain(spark):
    df = _build(spark)
    assert df.columns == FACT_INVENTORY_SNAPSHOT_SPEC.column_names
    # grain = store x sku x day; exactly one row per combination
    assert df.count() == 3 * 20 * 10
    assert df.groupBy("store_sk", "product_sk", "date_sk").count().filter("count > 1").count() == 0


def test_referential_integrity(spark):
    df = _build(spark)
    r = df.agg(
        F.min("store_sk").alias("slo"), F.max("store_sk").alias("shi"),
        F.min("product_sk").alias("plo"), F.max("product_sk").alias("phi"),
    ).first()
    assert 1 <= r["slo"] and r["shi"] <= 3
    assert 1 <= r["plo"] and r["phi"] <= 20
    # every date_sk is a real dim_date key
    dd = build_dim_date(spark, _CFG).select("date_sk")
    assert df.select("date_sk").distinct().join(dd, "date_sk", "left_anti").count() == 0


def test_measure_invariants(spark):
    df = _build(spark)
    bad = df.filter(
        (F.col("on_hand_qty") < 0)
        | (F.col("available_qty") < 0)
        | (F.col("available_qty") > F.col("on_hand_qty"))
        | (F.col("is_out_of_stock") != (F.col("on_hand_qty") == 0))
        | (F.abs(F.col("on_hand_cost_value") - F.round(F.col("on_hand_qty") * F.col("unit_cost"), 2)) > 0.01)
    ).count()
    assert bad == 0


def test_deterministic(spark):
    a = _build(spark).agg(F.sum("on_hand_qty").alias("q"), F.round(F.sum("on_hand_cost_value"), 2).alias("v")).first()
    b = _build(spark).agg(F.sum("on_hand_qty").alias("q"), F.round(F.sum("on_hand_cost_value"), 2).alias("v")).first()
    assert a == b
