from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_date import build_dim_date
from techmart.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line
from techmart.facts.lookups import (
    date_seasonality_weights,
    polars_to_spark,
    product_economics,
)

_PROFILE = ScaleProfile(
    name="test",
    num_stores=10,
    num_skus=40,
    history_years=1,
    sales_lines_target=3000,
    num_customers=200,
    num_vendors=20,
)
_CONFIG = TechmartConfig(
    scale_profile=_PROFILE,
    seed=42,
    output_dir=Path("data"),
    catalog="techmart",
    schema_prefix="techmart_",
    end_date=date(2026, 1, 31),
)


def _lookups(spark):
    dim_product = build_dim_product(_CONFIG)
    dim_date = build_dim_date(_CONFIG.start_date, _CONFIG.end_date)
    econ = product_economics(spark, dim_product)
    dd = polars_to_spark(
        spark,
        dim_date.select(
            ["date_sk", "is_weekend", "selling_season", "holiday_name", "year"]
        ),
    )
    return econ, date_seasonality_weights(dd)


def test_schema_and_rowcount(spark):
    econ, dw = _lookups(spark)
    df = build_fact_sales_line(spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000)
    assert df.columns == FACT_SALES_LINE_SPEC.column_names
    assert df.count() == 3000


def test_referential_integrity(spark):
    econ, dw = _lookups(spark)
    df = build_fact_sales_line(spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000)
    r = df.agg(
        F.min("product_sk").alias("p_lo"),
        F.max("product_sk").alias("p_hi"),
        F.min("store_sk").alias("s_lo"),
        F.max("store_sk").alias("s_hi"),
        F.min("customer_sk").alias("cu_lo"),
        F.max("customer_sk").alias("cu_hi"),
        F.min("employee_sk").alias("em_lo"),
        F.max("employee_sk").alias("em_hi"),
        F.min("channel_sk").alias("c_lo"),
        F.max("channel_sk").alias("c_hi"),
        F.count(F.when(F.col("unit_price").isNull(), 1)).alias("null_price"),
    ).collect()[0]
    assert r["p_lo"] >= 1 and r["p_hi"] <= _PROFILE.num_skus
    assert r["s_lo"] >= 1 and r["s_hi"] <= _PROFILE.num_stores
    assert r["cu_lo"] >= 1 and r["cu_hi"] <= _PROFILE.num_customers
    assert r["em_lo"] >= 1 and r["em_hi"] <= _PROFILE.num_employees
    assert r["c_lo"] >= 1 and r["c_hi"] <= 5
    assert r["null_price"] == 0  # every product_sk joined to economics
    promo = df.filter(F.col("promotion_sk").isNotNull()).agg(
        F.min("promotion_sk").alias("lo"), F.max("promotion_sk").alias("hi")
    ).collect()[0]
    assert promo["lo"] >= 1 and promo["hi"] <= _PROFILE.num_promotions


def test_measure_invariants(spark):
    econ, dw = _lookups(spark)
    df = build_fact_sales_line(spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000)
    bad = df.filter(
        (F.col("quantity") < 1)
        | (F.abs(F.col("net_sales_amount") - (F.col("gross_sales_amount") - F.col("discount_amount"))) > 0.01)
        | (F.abs(F.col("gross_margin_amount") - (F.col("net_sales_amount") - F.col("cogs_amount"))) > 0.01)
        | (F.col("discount_amount") < 0)
    ).count()
    assert bad == 0
    # Discount only when a promotion is present.
    assert df.filter(
        (F.col("promotion_sk").isNull()) & (F.col("discount_amount") > 0)
    ).count() == 0
    # is_marketplace iff channel_sk == 4.
    assert df.filter(
        (F.col("channel_sk") == 4) != F.col("is_marketplace")
    ).count() == 0


def test_deterministic(spark):
    econ, dw = _lookups(spark)
    agg = lambda: build_fact_sales_line(
        spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000
    ).agg(
        F.round(F.sum("net_sales_amount"), 2).alias("net"),
        F.sum("quantity").alias("qty"),
        F.count(F.when(F.col("promotion_sk").isNull(), 1)).alias("no_promo"),
    ).collect()[0]
    assert agg() == agg()


def test_fact_carries_column_comments(spark):
    econ, dw = _lookups(spark)
    df = build_fact_sales_line(spark, _CONFIG, product_econ=econ, date_weights=dw, rows=500)
    # A representative sample of columns must carry non-empty comments.
    for name in ["date_sk", "product_sk", "net_sales_amount", "is_marketplace"]:
        assert df.schema[name].metadata.get("comment", "") != ""
