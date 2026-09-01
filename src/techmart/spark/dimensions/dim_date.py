from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ..calendar import (
    _DAY_NAMES, _MONTH_NAMES, _MONTH_SEASON, fiscal_attrs, holiday_name,
)
from ..framework import SparkColumn, SparkTableSpec

DIM_DATE_SPEC = SparkTableSpec(
    schema="core",
    name="dim_date",
    grain="one row per calendar day",
    columns=[
        SparkColumn("date_sk", "long", "Surrogate key in yyyymmdd form", is_key=True, nullable=False),
        SparkColumn("date", "date", "Calendar date", nullable=False),
        SparkColumn("day_of_week", "int", "ISO day of week (1=Mon..7=Sun)"),
        SparkColumn("day_name", "string", "Full day name (e.g. 'Monday')"),
        SparkColumn("week", "int", "ISO week number"),
        SparkColumn("month", "int", "Calendar month (1-12)"),
        SparkColumn("month_name", "string", "Full month name (e.g. 'January')"),
        SparkColumn("quarter", "int", "Calendar quarter (1-4)"),
        SparkColumn("year", "int", "Calendar year"),
        SparkColumn("fiscal_year", "int", "Retail 4-5-4 fiscal year"),
        SparkColumn("fiscal_week", "int", "Retail fiscal week (1-53)"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("fiscal_quarter", "int", "Retail fiscal quarter (1-4)"),
        SparkColumn("is_weekend", "boolean", "True on Saturday or Sunday"),
        SparkColumn("is_holiday", "boolean", "True on a recognized US holiday"),
        SparkColumn("holiday_name", "string", "Holiday name, else null"),
        SparkColumn("selling_season", "string", "Retail selling-season label"),
    ],
)


def build_dim_date(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    start, end = config.start_date, config.end_date
    rows = []
    d = start
    while d <= end:
        fy, fw, fp, fq = fiscal_attrs(d)
        hn = holiday_name(d)
        rows.append((
            d.year * 10000 + d.month * 100 + d.day,
            d,  # native date -> Spark DateType (enables date math for BI/Genie)
            d.isoweekday(), _DAY_NAMES[d.weekday()], d.isocalendar()[1],
            d.month, _MONTH_NAMES[d.month - 1], (d.month - 1) // 3 + 1, d.year,
            fy, fw, fp, fq,
            d.weekday() >= 5, hn is not None, hn, _MONTH_SEASON[d.month],
        ))
        d += timedelta(days=1)
    return spark.createDataFrame(rows, schema=DIM_DATE_SPEC.struct_type())
