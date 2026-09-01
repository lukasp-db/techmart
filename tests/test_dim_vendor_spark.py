from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_vendor import DIM_VENDOR_SPEC, build_dim_vendor
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 50, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_vendor(spark):
    df = build_dim_vendor(spark, _CFG)
    validate_spark_schema(df, DIM_VENDOR_SPEC)
    assert df.count() == 20
    r_sk = df.agg(F.min("vendor_sk").alias("lo"), F.max("vendor_sk").alias("hi"),
                  F.countDistinct("vendor_sk").alias("d")).first()
    assert r_sk["lo"] == 1 and r_sk["hi"] == 20 and r_sk["d"] == 20
    r = df.agg(F.min("vendor_scorecard_rating").alias("lo"),
               F.max("vendor_scorecard_rating").alias("hi")).first()
    assert r["lo"] >= 1 and r["hi"] <= 5
    assert df.filter(~F.col("active_flag")).count() == 0
