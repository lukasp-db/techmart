from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.spark.dimensions.dim_store import build_dim_store
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_inventory_snapshot import build_fact_inventory_snapshot
from techmart.finance.fact_inventory_valuation import (
    FACT_INVENTORY_VALUATION_SPEC, build_fact_inventory_valuation,
)

_P = ScaleProfile("t", 6, 40, 1, 4000, 300, 20)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 6, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 40}


def _build(spark):
    dd = build_dim_date(spark, _CFG); dp = build_dim_product(spark, _CFG)
    ds = build_dim_store(spark, _CFG)
    snap = build_fact_inventory_snapshot(spark, _CFG, dim_store=ds, dim_product=dp, dim_date=dd)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=4000)
    val = build_fact_inventory_valuation(spark, _CFG, fact_inventory_snapshot=snap,
                                         fact_sales_line=sales, dim_product=dp, dim_date=dd)
    return val, snap, dp, dd


def test_schema_and_grain(spark):
    val, *_ = _build(spark)
    assert val.columns == FACT_INVENTORY_VALUATION_SPEC.column_names
    g = ["store_sk", "category_id", "date_sk"]
    assert val.groupBy(*g).count().filter(F.col("count") > 1).count() == 0


def test_referential_integrity(spark):
    val, snap, dp, dd = _build(spark)
    cats = dp.select("category_id").distinct()
    assert val.select("date_sk").distinct().join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
    assert val.select("category_id").distinct().join(cats, "category_id", "left_anti").count() == 0


def test_measures_valid(spark):
    val, *_ = _build(spark)
    assert val.filter((F.col("on_hand_cost_value") < 0) | (F.col("shrink_amount") < 0)
                      | (F.col("markdown_amount") < 0)).count() == 0
    # gmroi finite (no divide-by-zero blowups)
    assert val.filter(F.col("gmroi").isNull() | F.isnan("gmroi")).count() == 0


def test_cost_value_ties_to_snapshot(spark):
    val, snap, dp, dd = _build(spark)
    from techmart.finance.periods import period_end_lookup
    pe = period_end_lookup(dd).select(F.col("period_end_date_sk").alias("date_sk"))
    snap_pe_total = snap.join(pe, "date_sk").agg(F.round(F.sum("on_hand_cost_value"), 2)).first()[0]
    val_total = val.agg(F.round(F.sum("on_hand_cost_value"), 2)).first()[0]
    assert abs(snap_pe_total - val_total) < 1.0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("on_hand_cost_value"), 2).alias("s")).first()
    b = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("on_hand_cost_value"), 2).alias("s")).first()
    assert a == b
