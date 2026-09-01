from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_returns import FACT_RETURNS_SPEC, build_fact_returns

_P = ScaleProfile("t", 10, 40, 1, 3000, 200, 20)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 10, "customer": 200, "employee": _P.num_employees, "promotion": _P.num_promotions, "product": 40}


def _sales(spark):
    return build_fact_sales_line(
        spark, _CFG, dim_product=build_dim_product(spark, _CFG),
        dim_date=build_dim_date(spark, _CFG), dim_counts=_COUNTS, rows=3000,
    )


def _build(spark):
    return build_fact_returns(spark, _CFG, fact_sales_line=_sales(spark), dim_date=build_dim_date(spark, _CFG))


def test_schema_and_return_rate(spark):
    sales = _sales(spark)
    returns = build_fact_returns(spark, _CFG, fact_sales_line=sales, dim_date=build_dim_date(spark, _CFG))
    assert returns.columns == FACT_RETURNS_SPEC.column_names
    frac = returns.count() / sales.count()
    assert 0.04 < frac < 0.13  # ~8%


def test_links_to_real_sales(spark):
    sales = _sales(spark)
    returns = build_fact_returns(spark, _CFG, fact_sales_line=sales, dim_date=build_dim_date(spark, _CFG))
    orphans = returns.select("original_transaction_id").distinct().join(
        sales.select(F.col("transaction_id").alias("original_transaction_id")).distinct(),
        "original_transaction_id", "left_anti",
    ).count()
    assert orphans == 0


def test_return_invariants(spark):
    df = _build(spark)
    bad = df.filter(
        (F.col("quantity") < 1)
        | (F.col("refund_amount") < 0)
        | (F.col("restocking_fee") < 0)
    ).count()
    assert bad == 0
    dd = build_dim_date(spark, _CFG).select("date_sk")
    assert df.select("date_sk").distinct().join(dd, "date_sk", "left_anti").count() == 0
    assert df.select("rma_id").distinct().count() == df.count()


def test_deterministic(spark):
    a = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("refund_amount"), 2).alias("r")).first()
    b = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("refund_amount"), 2).alias("r")).first()
    assert a == b
