"""Fiscal 4-5-4 period helpers shared by the finance facts."""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def _pidx() -> "F.Column":
    return (F.col("fiscal_year") * F.lit(12) + (F.col("fiscal_period") - F.lit(1))).cast("int")


def date_periods(dim_date: DataFrame) -> DataFrame:
    """date_sk -> (fiscal_year, fiscal_period, fiscal_week, pidx)."""
    return dim_date.select(
        "date_sk", "fiscal_year", "fiscal_period", "fiscal_week"
    ).withColumn("pidx", _pidx())


def period_end_lookup(dim_date: DataFrame) -> DataFrame:
    """One row per fiscal period with its period-end date_sk and max fiscal week."""
    return (
        dim_date.groupBy("fiscal_year", "fiscal_period")
        .agg(
            F.max("date_sk").alias("period_end_date_sk"),
            F.max("fiscal_week").alias("period_max_week"),
        )
        .withColumn("pidx", _pidx())
    )
