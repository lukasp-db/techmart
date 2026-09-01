from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_customer import DIM_CUSTOMER_SPEC, build_dim_customer
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 500, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_customer(spark):
    df = build_dim_customer(spark, _CFG)
    validate_spark_schema(df, DIM_CUSTOMER_SPEC)
    assert df.count() == 500
    sk = df.agg(F.min("customer_sk").alias("lo"), F.max("customer_sk").alias("hi"),
                F.countDistinct("customer_sk").alias("d")).first()
    assert sk["lo"] == 1 and sk["hi"] == 500 and sk["d"] == 500
    # Non-members have no tier and no enroll date; members do (both directions).
    assert df.filter((~F.col("loyalty_member_flag")) & (F.col("loyalty_tier") != "None")).count() == 0
    assert df.filter((~F.col("loyalty_member_flag")) & F.col("loyalty_enroll_date").isNotNull()).count() == 0
    assert df.filter(F.col("loyalty_member_flag") & F.col("loyalty_enroll_date").isNull()).count() == 0
    assert df.filter(F.col("loyalty_member_flag") & (F.col("loyalty_tier") == "None")).count() == 0
    assert df.filter(~F.col("email").contains("@")).count() == 0
