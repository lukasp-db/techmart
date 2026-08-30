from __future__ import annotations

from datetime import date, datetime

import polars as pl

from .schema import Column


def scd2_columns() -> list[Column]:
    """The four SCD Type 2 control columns appended to every SCD2 dimension."""
    return [
        Column("effective_start_ts", "Datetime", "SCD2 effective start timestamp", nullable=False),
        Column("effective_end_ts", "Datetime", "SCD2 effective end timestamp; null when current"),
        Column("is_current", "Boolean", "True for the current version of the row", nullable=False),
        Column("version", "Int64", "SCD2 version number (1-based)", nullable=False),
    ]


def with_scd2_current(df: pl.DataFrame, start: date) -> pl.DataFrame:
    """Append SCD2 columns marking every row as the current (version 1) record."""
    start_ts = datetime(start.year, start.month, start.day)
    return df.with_columns(
        pl.lit(start_ts, dtype=pl.Datetime).alias("effective_start_ts"),
        pl.lit(None, dtype=pl.Datetime).alias("effective_end_ts"),
        pl.lit(True, dtype=pl.Boolean).alias("is_current"),
        pl.lit(1, dtype=pl.Int64).alias("version"),
    )
