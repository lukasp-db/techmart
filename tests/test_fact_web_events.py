from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.facts.fact_web_events import FACT_WEB_EVENTS_SPEC, build_fact_web_events

_P = ScaleProfile("t", 5, 40, 1, 3000, 200, 20, web_events_target=8000)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"customer": 200, "product": 40}


def _build(spark, rows=8000):
    return build_fact_web_events(spark, _CFG, dim_date=build_dim_date(spark, _CFG), dim_counts=_COUNTS, rows=rows)


def test_schema_and_volume(spark):
    df = _build(spark)
    assert df.columns == FACT_WEB_EVENTS_SPEC.column_names
    # explode of session headers lands near the target line count
    assert 4000 < df.count() < 16000


def test_referential_integrity_and_nullability(spark):
    df = _build(spark)
    dd = build_dim_date(spark, _CFG).select("date_sk")
    assert df.select("date_sk").distinct().join(dd, "date_sk", "left_anti").count() == 0
    cust = df.filter(F.col("customer_sk").isNotNull()).agg(F.min("customer_sk").alias("lo"), F.max("customer_sk").alias("hi")).first()
    assert cust["lo"] >= 1 and cust["hi"] <= 200
    prod = df.filter(F.col("product_sk").isNotNull()).agg(F.min("product_sk").alias("lo"), F.max("product_sk").alias("hi")).first()
    assert prod["lo"] >= 1 and prod["hi"] <= 40
    # anonymous sessions exist (nullable customer_sk) and channels are digital
    assert df.filter(F.col("customer_sk").isNull()).count() > 0
    assert df.filter(~F.col("channel_sk").isin(2, 3)).count() == 0


def test_event_types_and_ts(spark):
    df = _build(spark)
    assert df.select("event_type").distinct().count() >= 4
    # every event_ts falls on its date_sk day
    mismatched = df.filter(
        F.date_format(F.col("event_ts"), "yyyyMMdd").cast("long") != F.col("date_sk")
    ).count()
    assert mismatched == 0


def test_deterministic(spark):
    a = _build(spark).agg(F.count("*").alias("n"), F.countDistinct("session_id").alias("s")).first()
    b = _build(spark).agg(F.count("*").alias("n"), F.countDistinct("session_id").alias("s")).first()
    assert a == b
