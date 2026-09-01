from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.facts.fact_web_events import build_fact_web_events

_P = ScaleProfile("t", num_stores=5, num_skus=40, history_years=2,
                  sales_lines_target=3000, num_customers=200, num_vendors=20,
                  web_events_target=40000)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"customer": 200, "product": 40}


def test_device_is_decorrelated_from_date(spark):
    df = build_fact_web_events(spark, _CFG, dim_date=build_dim_date(spark, _CFG),
                               dim_counts=_COUNTS, rows=40000)
    avg_dev = (df.groupBy("date_sk").agg(F.countDistinct("device_type").alias("d"))
                 .agg(F.avg("d").alias("a")).first()["a"])
    # With ~28 sessions/day, the correlated ("fixed") build ties ~one device band to
    # each date (~1 distinct device/day); decorrelated shows all three (~3).
    assert avg_dev > 2.0, f"device/day distinct collapsed to {avg_dev:.2f}"
