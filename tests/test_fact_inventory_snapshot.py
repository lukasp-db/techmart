from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.spark.dimensions.dim_store import build_dim_store
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_inventory_snapshot import (
    FACT_INVENTORY_SNAPSHOT_SPEC,
    build_fact_inventory_snapshot,
)
from techmart.finance.periods import period_end_lookup

_P = ScaleProfile("t", 4, 30, 1, 6000, 300, 20)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 4, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 30}


def _dims(spark):
    return (
        build_dim_store(spark, _CFG),
        build_dim_product(spark, _CFG),
        build_dim_date(spark, _CFG),
    )


def _build(spark):
    ds, dp, dd = _dims(spark)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=6000)
    return build_fact_inventory_snapshot(
        spark, _CFG, dim_store=ds, dim_product=dp, dim_date=dd, fact_sales_line=sales,
    ), dd


def test_schema_and_grain(spark):
    df, _ = _build(spark)
    assert df.columns == FACT_INVENTORY_SNAPSHOT_SPEC.column_names
    # one row per (store, sku, period-end)
    assert df.groupBy("store_sk", "product_sk", "date_sk").count().filter("count > 1").count() == 0


def test_dates_are_period_ends_and_span_history(spark):
    df, dd = _build(spark)
    pe = period_end_lookup(dd).select(F.col("period_end_date_sk").alias("date_sk"))
    # every snapshot date is a fiscal period-end
    assert df.select("date_sk").distinct().join(pe, "date_sk", "left_anti").count() == 0
    # spans (approximately) one row per fiscal period across the year
    n_periods = pe.count()
    assert df.select("date_sk").distinct().count() == n_periods
    assert n_periods >= 10  # ~12 fiscal periods in a 1-year window


def test_assortment_is_sparse_and_popularity_ordered(spark):
    df, _ = _build(spark)
    pairs = df.select("store_sk", "product_sk").distinct()
    # each store carries a strict subset of the 30-SKU catalog
    assert pairs.count() < 4 * 30
    # hot (low product_sk) SKUs are carried in >= as many stores as the cold tail
    half = 15
    hot = pairs.filter(F.col("product_sk") <= half).count()
    cold = pairs.filter(F.col("product_sk") > half).count()
    assert hot >= cold


def test_measure_invariants(spark):
    df, _ = _build(spark)
    bad = df.filter(
        (F.col("on_hand_qty") < 0)
        | (F.col("available_qty") < 0)
        | (F.col("available_qty") > F.col("on_hand_qty"))
        | (F.col("is_out_of_stock") != (F.col("on_hand_qty") == 0))
        | (F.abs(F.col("on_hand_cost_value") - F.round(F.col("on_hand_qty") * F.col("unit_cost"), 2)) > 0.01)
    ).count()
    assert bad == 0


def test_days_of_supply_in_sane_band(spark):
    df, _ = _build(spark)
    # median-ish days-of-supply should sit near target_wos_weeks * 7 (56d), not the
    # old pathological range; check the in-stock population stays bounded.
    r = df.filter(~F.col("is_out_of_stock")).agg(
        F.expr("percentile_approx(days_of_supply, 0.5)").alias("p50")
    ).first()
    assert 7.0 <= r["p50"] <= 200.0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.sum("on_hand_qty").alias("q"),
                             F.round(F.sum("on_hand_cost_value"), 2).alias("v")).first()
    b = _build(spark)[0].agg(F.sum("on_hand_qty").alias("q"),
                             F.round(F.sum("on_hand_cost_value"), 2).alias("v")).first()
    assert a == b
