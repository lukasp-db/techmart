from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_inventory_movement import (
    FACT_INVENTORY_MOVEMENT_SPEC,
    build_fact_inventory_movement,
)

_P = ScaleProfile("t", 5, 40, 1, 3000, 200, 20, inventory_movements_target=4000)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 5, "vendor": 20, "product": 40}


def _build(spark, rows=4000):
    return build_fact_inventory_movement(
        spark, _CFG, dim_date=build_dim_date(spark, _CFG),
        dim_product=build_dim_product(spark, _CFG), dim_counts=_COUNTS, rows=rows,
    )


def test_schema_and_count(spark):
    df = _build(spark)
    assert df.columns == FACT_INVENTORY_MOVEMENT_SPEC.column_names
    assert df.count() == 4000
    assert df.select("movement_id").distinct().count() == 4000


def test_referential_integrity(spark):
    df = _build(spark)
    r = df.agg(
        F.min("store_sk").alias("slo"), F.max("store_sk").alias("shi"),
        F.min("product_sk").alias("plo"), F.max("product_sk").alias("phi"),
    ).first()
    assert 1 <= r["slo"] and r["shi"] <= 5
    assert 1 <= r["plo"] and r["phi"] <= 40
    dd = build_dim_date(spark, _CFG).select("date_sk")
    assert df.select("date_sk").distinct().join(dd, "date_sk", "left_anti").count() == 0
    # vendor_sk populated only for Receipt / Return-to-Vendor, and in range there
    vend = df.filter(F.col("vendor_sk").isNotNull())
    assert vend.filter(~F.col("movement_type").isin("Receipt", "Return-to-Vendor")).count() == 0
    rv = vend.agg(F.min("vendor_sk").alias("lo"), F.max("vendor_sk").alias("hi")).first()
    assert rv["lo"] >= 1 and rv["hi"] <= 20


def test_movement_types_and_sign(spark):
    df = _build(spark)
    assert df.select("movement_type").distinct().count() == 5
    # Shrink and Return-to-Vendor reduce stock (negative qty); Receipt is positive
    assert df.filter(F.col("movement_type").isin("Shrink", "Return-to-Vendor") & (F.col("quantity") > 0)).count() == 0
    assert df.filter((F.col("movement_type") == "Receipt") & (F.col("quantity") < 0)).count() == 0


def test_deterministic(spark):
    a = _build(spark).agg(F.sum("quantity").alias("q")).first()
    b = _build(spark).agg(F.sum("quantity").alias("q")).first()
    assert a == b
