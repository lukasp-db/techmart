from __future__ import annotations

from pathlib import Path

import polars as pl

from .schema import TableSpec


class SchemaMismatchError(ValueError):
    """Raised when a DataFrame's columns do not match its TableSpec."""


def validate_schema(df: pl.DataFrame, spec: TableSpec) -> None:
    if df.columns != spec.column_names:
        raise SchemaMismatchError(
            f"{spec.name}: expected columns {spec.column_names}, got {df.columns}"
        )


def write_table(df: pl.DataFrame, spec: TableSpec, output_dir: Path) -> Path:
    validate_schema(df, spec)
    dest_dir = Path(output_dir) / spec.schema
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{spec.name}.parquet"
    df.write_parquet(dest)
    return dest
