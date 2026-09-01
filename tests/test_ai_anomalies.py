import dataclasses
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.ai.anomalies import (
    AI_ANOMALY_CATALOG_SPEC, week_calendar, build_ai_anomaly_catalog,
)

_P = ScaleProfile("t", 8, 40, 3, 4000, 300, 20)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def test_week_calendar_gives_real_week_end_date_sks(spark):
    dd = build_dim_date(spark, _CFG)
    wc = week_calendar(spark, dd)
    # every week-end date_sk is a real dim_date row
    assert wc.select("date_sk").join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
    # holiday weeks exist over a 3-year horizon
    assert wc.filter(F.col("is_holiday_week")).count() > 0


def test_catalog_documents_five_anomalies_with_two_realized(spark):
    dd = build_dim_date(spark, _CFG)
    cat = build_ai_anomaly_catalog(spark, _CFG, dim_date=dd)
    assert cat.columns == AI_ANOMALY_CATALOG_SPEC.column_names
    assert cat.count() == 5
    assert cat.filter(F.col("realized_in") == "fact_sales_forecast").count() == 2
    # windows are real dim_date rows
    assert cat.select("start_date_sk").withColumnRenamed("start_date_sk", "date_sk") \
        .join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
