from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 300, 1, 50000, 500, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_product(spark):
    df = build_dim_product(spark, _CFG)
    validate_spark_schema(df, DIM_PRODUCT_SPEC)
    assert df.count() == 300
    r = df.agg(F.min("product_sk").alias("lo"), F.max("product_sk").alias("hi"),
               F.countDistinct("product_sk").alias("d")).first()
    assert r["lo"] == 1 and r["hi"] == 300 and r["d"] == 300
    # Hierarchy always populated; primary_vendor_sk in range; JSON specs parse.
    assert df.filter(F.col("division_name").isNull() | F.col("brand_name").isNull()).count() == 0
    assert df.filter((F.col("primary_vendor_sk") < 1) | (F.col("primary_vendor_sk") > 20)).count() == 0
    assert df.filter(F.get_json_object(F.col("spec_attributes"), "$.brand").isNull()).count() == 0
    # discontinue_date only when discontinued, and within the window.
    assert df.filter((F.col("lifecycle_status") != "Discontinued") & F.col("discontinue_date").isNotNull()).count() == 0
    assert df.filter(F.col("discontinue_date") > F.lit(_CFG.end_date)).count() == 0
