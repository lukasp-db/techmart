from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_employee import DIM_EMPLOYEE_SPEC, build_dim_employee
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 1000, 20),  # num_employees = 40*5 = 200
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_employee(spark):
    df = build_dim_employee(spark, _CFG)
    validate_spark_schema(df, DIM_EMPLOYEE_SPEC)
    assert df.count() == _CFG.scale_profile.num_employees  # 200
    n = _CFG.scale_profile.num_employees
    r_sk = df.agg(F.min("employee_sk").alias("lo"), F.max("employee_sk").alias("hi"),
                  F.countDistinct("employee_sk").alias("d")).first()
    assert r_sk["lo"] == 1 and r_sk["hi"] == n and r_sk["d"] == n
    r = df.agg(F.min("store_sk").alias("lo"), F.max("store_sk").alias("hi")).first()
    assert r["lo"] >= 1 and r["hi"] <= _CFG.scale_profile.num_stores
    # Managers have no manager; non-managers do.
    assert df.filter((F.col("role") == "Manager") & F.col("manager_employee_sk").isNotNull()).count() == 0
    assert df.filter((F.col("role") != "Manager") & F.col("manager_employee_sk").isNull()).count() == 0
