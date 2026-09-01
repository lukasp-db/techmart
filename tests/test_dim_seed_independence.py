from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_store import build_dim_store
from techmart.spark.dimensions.dim_vendor import build_dim_vendor
from techmart.spark.dimensions.dim_promotion import build_dim_promotion
from techmart.spark.dimensions.dim_employee import build_dim_employee
from techmart.spark.dimensions.dim_customer import build_dim_customer


def _cfg(**kw):
    base = dict(num_stores=300, num_skus=40, history_years=2, sales_lines_target=3000,
                num_customers=600, num_vendors=200)
    base.update(kw)
    return TechmartConfig(scale_profile=ScaleProfile("t", **base), seed=42,
                          output_dir=Path("data"), catalog="c", schema_prefix="techmart_",
                          end_date=date(2026, 1, 31))


def test_store_city_and_state_independent(spark):
    df = build_dim_store(spark, _cfg(num_stores=300))
    combos = df.select("city", "state").distinct().count()
    # fixed: ~15 diagonal combos; decorrelated: ~190 of the 15x15 grid.
    assert combos > 60, f"store city/state combos collapsed to {combos}"


def test_vendor_name_not_collapsed(spark):
    df = build_dim_vendor(spark, _cfg(num_vendors=200))
    n = df.select("vendor_name").distinct().count()
    # stem(14) x tail(6): fixed ~14 names; decorrelated ~80.
    assert n > 30, f"vendor_name distinct collapsed to {n}"


def test_promotion_attrs_independent(spark):
    df = build_dim_promotion(spark, _cfg(history_years=2))  # 120 promotions
    combos = df.select("promo_type", "discount_method", "channel_scope",
                       "funding_source").distinct().count()
    # fixed: ~10 combos; decorrelated: ~57 of the 5x3x3x2 grid.
    assert combos > 30, f"promotion attr combos collapsed to {combos}"


def test_employee_full_name_not_collapsed(spark):
    df = build_dim_employee(spark, _cfg(num_stores=10))  # 400 employees
    n = df.select("full_name").distinct().count()
    # first(20) x last(20): fixed ~20 names; decorrelated ~350.
    assert n > 60, f"employee full_name distinct collapsed to {n}"


def test_customer_name_not_collapsed(spark):
    df = build_dim_customer(spark, _cfg(num_customers=600))
    combos = df.select("first_name", "last_name").distinct().count()
    # first(20) x last(20): fixed ~20 combos; decorrelated ~350.
    assert combos > 60, f"customer name combos collapsed to {combos}"
