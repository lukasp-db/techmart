from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line

_P = ScaleProfile("t", 10, 40, 1, 3000, 200, 20)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 10, "customer": 200, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 40}


def _build(spark, rows=3000):
    dp = build_dim_product(spark, _CFG)
    dd = build_dim_date(spark, _CFG)
    return build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=rows)


def test_schema_and_columns(spark):
    df = _build(spark)
    assert df.columns == FACT_SALES_LINE_SPEC.column_names
    # rows=3000 is a line-count target; basket expansion (avg ~2.9) keeps it in a sane band.
    assert 1500 < df.count() < 5000


def test_referential_integrity(spark):
    df = _build(spark)
    r = df.agg(
        F.min("product_sk").alias("plo"), F.max("product_sk").alias("phi"),
        F.min("store_sk").alias("slo"), F.max("store_sk").alias("shi"),
        F.min("customer_sk").alias("culo"), F.max("customer_sk").alias("cuhi"),
        F.min("employee_sk").alias("elo"), F.max("employee_sk").alias("ehi"),
        F.min("channel_sk").alias("chlo"), F.max("channel_sk").alias("chhi"),
        F.count(F.when(F.col("unit_price").isNull(), 1)).alias("nullprice"),
    ).first()
    assert 1 <= r["plo"] and r["phi"] <= 40
    assert 1 <= r["slo"] and r["shi"] <= 10
    assert 1 <= r["culo"] and r["cuhi"] <= 200
    assert 1 <= r["elo"] and r["ehi"] <= _P.num_employees
    assert 1 <= r["chlo"] and r["chhi"] <= 5
    assert r["nullprice"] == 0
    promo = df.filter(F.col("promotion_sk").isNotNull()).agg(
        F.min("promotion_sk").alias("lo"), F.max("promotion_sk").alias("hi")).first()
    assert promo["lo"] >= 1 and promo["hi"] <= _P.num_promotions


def test_measures_and_marketplace(spark):
    df = _build(spark)
    bad = df.filter(
        (F.col("quantity") < 1)
        | (F.abs(F.col("net_sales_amount") - (F.col("gross_sales_amount") - F.col("discount_amount"))) > 0.01)
        | (F.abs(F.col("gross_margin_amount") - (F.col("net_sales_amount") - F.col("cogs_amount"))) > 0.01)
        | ((F.col("channel_sk") == 4) != F.col("is_marketplace"))
    ).count()
    assert bad == 0
    assert df.filter(F.col("promotion_sk").isNull() & (F.col("discount_amount") > 0)).count() == 0


def test_basket_coherence(spark):
    df = _build(spark)
    incoherent = df.groupBy("transaction_id").agg(
        F.countDistinct("store_sk").alias("s"), F.countDistinct("date_sk").alias("d"),
        F.countDistinct("customer_sk").alias("c"), F.countDistinct("channel_sk").alias("ch"),
        F.countDistinct("employee_sk").alias("e"),
        F.count("*").alias("n"), F.max("line_number").alias("mx"), F.min("line_number").alias("mn"),
    ).filter("s > 1 or d > 1 or c > 1 or ch > 1 or e > 1 or n <> mx or mn <> 1").count()
    assert incoherent == 0


def test_deterministic(spark):
    agg = lambda: _build(spark).agg(
        F.round(F.sum("net_sales_amount"), 2).alias("net"), F.sum("quantity").alias("q"),
        F.count(F.when(F.col("promotion_sk").isNull(), 1)).alias("np")).first()
    assert agg() == agg()
