import pytest
from pyspark.sql.types import LongType, StringType, StructType

from techmart.spark.framework import (
    FactColumn,
    FactSchemaMismatchError,
    FactSpec,
    validate_fact_schema,
)

SPEC = FactSpec(
    schema="core",
    name="fact_demo",
    grain="one row per demo event",
    columns=[
        FactColumn("id_sk", "long", "Surrogate key", is_key=True, nullable=False),
        FactColumn("label", "string", "A label"),
    ],
)


def test_column_names():
    assert SPEC.column_names == ["id_sk", "label"]


def test_struct_type_maps_dtypes():
    st = SPEC.struct_type()
    assert isinstance(st, StructType)
    assert [f.name for f in st.fields] == ["id_sk", "label"]
    assert isinstance(st["id_sk"].dataType, LongType)
    assert isinstance(st["label"].dataType, StringType)
    assert st["id_sk"].nullable is False
    assert st["label"].nullable is True


def test_validate_accepts_matching_dataframe(spark):
    df = spark.createDataFrame([(1, "a")], SPEC.struct_type())
    validate_fact_schema(df, SPEC)  # no raise


def test_validate_rejects_missing_column(spark):
    df = spark.createDataFrame([(1,)], StructType([SPEC.struct_type()["id_sk"]]))
    with pytest.raises(FactSchemaMismatchError):
        validate_fact_schema(df, SPEC)


def test_validate_rejects_wrong_type(spark):
    # label is string in the spec; supply long instead.
    df = spark.createDataFrame([(1, 2)], "id_sk long, label long")
    with pytest.raises(FactSchemaMismatchError):
        validate_fact_schema(df, SPEC)


def test_validate_rejects_extra_column(spark):
    df = spark.createDataFrame([(1, "a", "x")], "id_sk long, label string, bonus string")
    with pytest.raises(FactSchemaMismatchError):
        validate_fact_schema(df, SPEC)


def test_struct_type_carries_comment_metadata():
    st = SPEC.struct_type()
    assert st["id_sk"].metadata.get("comment") == "Surrogate key"
    assert st["label"].metadata.get("comment") == "A label"


def test_select_ordered_attaches_comment_metadata(spark):
    df = spark.createDataFrame([("a", 1)], "label string, id_sk long")  # wrong order on purpose
    out = SPEC.select_ordered(df)
    assert out.columns == ["id_sk", "label"]  # reordered to spec order
    assert out.schema["id_sk"].metadata.get("comment") == "Surrogate key"
    assert out.schema["label"].metadata.get("comment") == "A label"
