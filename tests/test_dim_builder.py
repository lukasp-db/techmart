from datetime import date

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dim_builder import build_scd2_dim, sql_array


def test_sql_array_quotes_and_joins():
    assert sql_array(["a", "b", "c"]) == "array('a', 'b', 'c')"
    assert sql_array(["Solo"]) == "array('Solo')"


def test_build_scd2_dim_shape(spark):
    from techmart.spark.framework import SparkColumn, SparkTableSpec, validate_spark_schema
    from techmart.spark.scd2 import scd2_columns

    spec = SparkTableSpec(
        schema="core", name="dim_probe", grain="probe",
        columns=[
            SparkColumn("probe_sk", "long", "SK", is_key=True, nullable=False),
            SparkColumn("label", "string", "label"),
        ] + scd2_columns(),
    )
    cfg = TechmartConfig(
        scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 1000, 20),
        seed=42, output_dir=__import__("pathlib").Path("data"),
        catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
    )

    def add_columns(gen):
        return (
            gen.withColumn("probe_sk", "long", expr="id + 1", baseColumn="id")
            .withColumn("label", "string", values=["x", "y"], random=True)
        )

    df = build_scd2_dim(spark, cfg, spec, rows=10, add_columns=add_columns)
    validate_spark_schema(df, spec)
    assert df.count() == 10
    assert df.columns == spec.column_names
    assert df.filter(~df.is_current).count() == 0
    assert df.selectExpr("min(probe_sk) lo", "max(probe_sk) hi").first().asDict() == {"lo": 1, "hi": 10}
