from datetime import date

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_date_schema_and_span(spark):
    df = build_dim_date(spark, _CFG)
    validate_spark_schema(df, DIM_DATE_SPEC)
    assert df.columns == DIM_DATE_SPEC.column_names
    # One row per day across the history window.
    span_days = (_CFG.end_date - _CFG.start_date).days + 1
    assert df.count() == span_days


def test_dim_date_known_values(spark):
    df = build_dim_date(spark, _CFG)
    from pyspark.sql import functions as F
    row = df.filter(F.col("date_sk") == 20251225).first()
    assert row["is_holiday"] is True and row["holiday_name"] == "Christmas Day"
    assert row["month_name"] == "December"
