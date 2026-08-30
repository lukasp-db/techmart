from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, functions as F

from .framework import SparkColumn


def scd2_columns() -> list[SparkColumn]:
    """The four SCD Type 2 control columns appended to every SCD2 dimension."""
    return [
        SparkColumn("effective_start_ts", "timestamp", "SCD2 effective start timestamp", nullable=False),
        SparkColumn("effective_end_ts", "timestamp", "SCD2 effective end timestamp; null when current"),
        SparkColumn("is_current", "boolean", "True for the current version of the row", nullable=False),
        SparkColumn("version", "int", "SCD2 version number (1-based)", nullable=False),
    ]


def with_scd2_current(df: DataFrame, start: date) -> DataFrame:
    """Append SCD2 columns marking every row as the current (version 1) record."""
    start_ts = datetime(start.year, start.month, start.day)
    return (
        df.withColumn("effective_start_ts", F.lit(start_ts).cast("timestamp"))
        .withColumn("effective_end_ts", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("version", F.lit(1).cast("int"))
    )
