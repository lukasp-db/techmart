from pathlib import Path

import polars as pl
import pytest

from techmart.framework.schema import Column, TableSpec
from techmart.framework.writer import (
    SchemaMismatchError,
    validate_schema,
    write_table,
)

SPEC = TableSpec(
    schema="core",
    name="dim_demo",
    grain="one row per demo id",
    columns=[
        Column("demo_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
        Column("label", "Utf8", "Human label"),
    ],
)


def test_column_names_preserves_order():
    assert SPEC.column_names == ["demo_sk", "label"]


def test_validate_schema_passes_on_match():
    df = pl.DataFrame({"demo_sk": [1, 2], "label": ["a", "b"]})
    validate_schema(df, SPEC)  # no raise


def test_validate_schema_rejects_wrong_columns():
    df = pl.DataFrame({"demo_sk": [1], "wrong": ["a"]})
    with pytest.raises(SchemaMismatchError):
        validate_schema(df, SPEC)


def test_write_table_roundtrips(tmp_path: Path):
    df = pl.DataFrame({"demo_sk": [1, 2], "label": ["a", "b"]})
    dest = write_table(df, SPEC, tmp_path)
    assert dest == tmp_path / "core" / "dim_demo.parquet"
    assert dest.exists()
    back = pl.read_parquet(dest)
    assert back.columns == SPEC.column_names
    assert back.height == 2


def test_polars_schema_maps_declared_dtypes():
    schema = SPEC.polars_schema()
    assert schema == {"demo_sk": pl.Int64, "label": pl.Utf8}


def test_validate_schema_rejects_wrong_dtype():
    # Right column names, wrong dtype for demo_sk (Utf8 instead of Int64).
    df = pl.DataFrame({"demo_sk": ["1", "2"], "label": ["a", "b"]})
    with pytest.raises(SchemaMismatchError):
        validate_schema(df, SPEC)
