import dataclasses
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line


def _cfg(history_years: int) -> TechmartConfig:
    sp = ScaleProfile("t", num_stores=200, num_skus=60, history_years=history_years,
                      sales_lines_target=60000, num_customers=2000, num_vendors=20)
    return TechmartConfig(scale_profile=sp, seed=42, output_dir=Path("data"),
                          catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def test_dates_are_decorrelated_from_store(spark):
    cfg = _cfg(history_years=3)
    dd = build_dim_date(spark, cfg)
    dp = build_dim_product(spark, cfg)
    counts = {"store": 200, "customer": 2000, "employee": cfg.scale_profile.num_employees,
              "promotion": cfg.scale_profile.num_promotions, "product": 60}
    df = build_fact_sales_line(spark, cfg, dim_product=dp, dim_date=dd,
                               dim_counts=counts, rows=60000)
    per_store = (df.groupBy("store_sk")
                   .agg(F.countDistinct("date_sk").alias("d"))
                   .agg(F.avg("d").alias("avg_d")).first()["avg_d"])
    # ~100 txns/store over ~1097 dates: correlated build gives ~2; decorrelated gives dozens.
    assert per_store > 10, f"per-store distinct dates collapsed to {per_store:.1f} (date/store correlation)"
