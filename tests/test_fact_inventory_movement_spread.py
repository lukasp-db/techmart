# tests/test_fact_inventory_movement_spread.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_inventory_movement import build_fact_inventory_movement

_P = ScaleProfile("t", num_stores=5, num_skus=40, history_years=2,
                  sales_lines_target=3000, num_customers=200, num_vendors=20,
                  inventory_movements_target=10000)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 100, "vendor": 20, "product": 40}


def test_dates_are_decorrelated_from_store(spark):
    df = build_fact_inventory_movement(
        spark, _CFG, dim_date=build_dim_date(spark, _CFG),
        dim_product=build_dim_product(spark, _CFG), dim_counts=_COUNTS, rows=10000,
    )
    avg_d = (df.groupBy("store_sk").agg(F.countDistinct("date_sk").alias("d"))
               .agg(F.avg("d").alias("a")).first()["a"])
    # ~100 movements/store over ~730 dates: the correlated ("fixed") build collapses
    # each store to ~8 distinct dates (avg_d ≈ 8.16, below the >10 threshold);
    # decorrelated gives dozens.
    assert avg_d > 10, f"per-store distinct dates collapsed to {avg_d:.1f}"
