from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_store import DIM_STORE_SPEC, build_dim_store
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 50, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_store(spark):
    df = build_dim_store(spark, _CFG)
    validate_spark_schema(df, DIM_STORE_SPEC)
    assert df.count() == 50
    r = df.agg(F.min("store_sk").alias("lo"), F.max("store_sk").alias("hi"),
               F.countDistinct("store_sk").alias("d")).first()
    assert r["lo"] == 1 and r["hi"] == 50 and r["d"] == 50
    assert df.filter(~F.col("is_current")).count() == 0
    assert df.filter(F.col("country") != "US").count() == 0
