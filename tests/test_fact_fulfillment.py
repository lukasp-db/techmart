from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_fulfillment import FACT_FULFILLMENT_SPEC, build_fact_fulfillment

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


def _build(spark, sales=None):
    sales = sales if sales is not None else _sales(spark)
    return build_fact_fulfillment(spark, _CFG, fact_sales_line=sales, dim_date=build_dim_date(spark, _CFG))


def test_schema_and_online_only(spark):
    sales = _sales(spark)
    ful = _build(spark, sales)
    assert ful.columns == FACT_FULFILLMENT_SPEC.column_names
    # only online channels fulfilled
    assert ful.filter(~F.col("channel_sk").isin(2, 3, 4)).count() == 0
    assert ful.count() == sales.filter(F.col("channel_sk").isin(2, 3, 4)).count()


def test_dates_and_sla(spark):
    df = _build(spark)
    dd = build_dim_date(spark, _CFG).select("date_sk")
    for c in ("date_sk", "promised_date_sk", "actual_ship_date_sk", "delivery_date_sk"):
        assert df.select(F.col(c).alias("date_sk")).distinct().join(dd, "date_sk", "left_anti").count() == 0
    # sla_met_flag == (delivered on or before promised)
    assert df.filter(F.col("sla_met_flag") != (F.col("delivery_date_sk") <= F.col("promised_date_sk"))).count() == 0
    # BOPIS / Curbside have no shipping cost
    assert df.filter(F.col("fulfillment_type").isin("BOPIS", "Curbside") & (F.col("shipping_cost") > 0)).count() == 0


def test_deterministic(spark):
    a = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("shipping_cost"), 2).alias("s")).first()
    b = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("shipping_cost"), 2).alias("s")).first()
    assert a == b
