from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_customer import build_dim_customer
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_loyalty_activity import (
    FACT_LOYALTY_ACTIVITY_SPEC,
    build_fact_loyalty_activity,
)

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
    return build_fact_loyalty_activity(
        spark, _CFG, fact_sales_line=_sales(spark),
        dim_customer=build_dim_customer(spark, _CFG), dim_date=build_dim_date(spark, _CFG),
    )


def test_schema_and_members_only(spark):
    df = _build(spark)
    assert df.columns == FACT_LOYALTY_ACTIVITY_SPEC.column_names
    members = build_dim_customer(spark, _CFG).filter(F.col("loyalty_member_flag")).select("customer_sk")
    non_members = df.select("customer_sk").distinct().join(members, "customer_sk", "left_anti").count()
    assert non_members == 0


def test_earn_links_to_real_receipts(spark):
    sales = _sales(spark)
    df = build_fact_loyalty_activity(
        spark, _CFG, fact_sales_line=sales,
        dim_customer=build_dim_customer(spark, _CFG), dim_date=build_dim_date(spark, _CFG),
    )
    earn = df.filter(F.col("activity_type") == "Earn")
    assert earn.filter(F.col("points") <= 0).count() == 0
    orphans = earn.select("related_transaction_id").distinct().join(
        sales.select(F.col("transaction_id").alias("related_transaction_id")).distinct(),
        "related_transaction_id", "left_anti",
    ).count()
    assert orphans == 0


def test_activity_types_and_balance(spark):
    df = _build(spark)
    types = {r["activity_type"] for r in df.select("activity_type").distinct().collect()}
    assert "Earn" in types
    # Redeem / Expire reduce points (negative)
    assert df.filter(F.col("activity_type").isin("Redeem", "Expire") & (F.col("points") > 0)).count() == 0
    dd = build_dim_date(spark, _CFG).select("date_sk")
    assert df.select("date_sk").distinct().join(dd, "date_sk", "left_anti").count() == 0
    assert df.select("loyalty_event_id").distinct().count() == df.count()


def test_deterministic(spark):
    a = _build(spark).agg(F.count("*").alias("n"), F.sum("points").alias("p")).first()
    b = _build(spark).agg(F.count("*").alias("n"), F.sum("points").alias("p")).first()
    assert a == b
