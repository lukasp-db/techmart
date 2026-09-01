from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 50, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_promotion(spark):
    df = build_dim_promotion(spark, _CFG)
    validate_spark_schema(df, DIM_PROMOTION_SPEC)
    assert df.count() == _CFG.scale_profile.num_promotions
    r_sk = df.agg(F.min("promotion_sk").alias("lo"), F.max("promotion_sk").alias("hi"),
                  F.countDistinct("promotion_sk").alias("d")).first()
    n = _CFG.scale_profile.num_promotions
    assert r_sk["lo"] == 1 and r_sk["hi"] == n and r_sk["d"] == n
    # discount_value rounded to 2 decimals (matches the retired Polars builder).
    assert df.filter(F.round("discount_value", 2) != F.col("discount_value")).count() == 0
    # end never after the history window, and not before start.
    assert df.filter(F.col("end_date") > F.lit(_CFG.end_date)).count() == 0
    assert df.filter(F.col("end_date") < F.col("start_date")).count() == 0
