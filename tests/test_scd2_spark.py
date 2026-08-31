from datetime import date

from pyspark.sql.types import BooleanType, LongType, TimestampType

from techmart.spark.scd2 import scd2_columns, with_scd2_current


def test_scd2_columns_shape():
    cols = scd2_columns()
    assert [c.name for c in cols] == [
        "effective_start_ts", "effective_end_ts", "is_current", "version"
    ]
    assert cols[0].nullable is False  # effective_start_ts
    assert cols[1].nullable is True   # effective_end_ts
    assert cols[2].nullable is False  # is_current
    assert cols[3].nullable is False  # version


def test_with_scd2_current_values(spark):
    df = spark.createDataFrame([(1,), (2,)], "store_sk long")
    out = with_scd2_current(df, date(2023, 2, 1))
    assert set(["effective_start_ts", "effective_end_ts", "is_current", "version"]).issubset(out.columns)
    assert isinstance(out.schema["effective_start_ts"].dataType, TimestampType)
    assert isinstance(out.schema["effective_end_ts"].dataType, TimestampType)
    assert isinstance(out.schema["is_current"].dataType, BooleanType)
    assert isinstance(out.schema["version"].dataType, LongType)
    row = out.orderBy("store_sk").first()
    assert row["is_current"] is True and row["version"] == 1
    assert row["effective_end_ts"] is None
    assert row["effective_start_ts"].date() == date(2023, 2, 1)
