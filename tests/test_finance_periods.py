from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.finance.periods import date_periods, period_end_lookup

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 40, 1, 3000, 200, 20), seed=42,
    output_dir=Path("data"), catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_period_end_one_row_per_period(spark):
    dd = build_dim_date(spark, _CFG)
    pe = period_end_lookup(dd)
    assert pe.count() == dd.select("fiscal_year", "fiscal_period").distinct().count()
    assert pe.select("pidx").distinct().count() == pe.count()


def test_period_end_is_real_and_max(spark):
    dd = build_dim_date(spark, _CFG)
    pe = period_end_lookup(dd)
    # every period_end_date_sk is a real dim_date row
    orphans = pe.select(F.col("period_end_date_sk").alias("date_sk")).join(
        dd.select("date_sk"), "date_sk", "left_anti"
    ).count()
    assert orphans == 0
    # it is the max date_sk within its period
    joined = date_periods(dd).groupBy("fiscal_year", "fiscal_period").agg(
        F.max("date_sk").alias("mx")
    ).join(pe, ["fiscal_year", "fiscal_period"])
    assert joined.filter(F.col("mx") != F.col("period_end_date_sk")).count() == 0


def test_pidx_formula(spark):
    dd = build_dim_date(spark, _CFG)
    dp = date_periods(dd)
    bad = dp.filter(
        F.col("pidx") != (F.col("fiscal_year") * F.lit(12) + (F.col("fiscal_period") - F.lit(1)))
    ).count()
    assert bad == 0
