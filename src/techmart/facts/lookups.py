from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def product_economics(dim_product_spark: DataFrame) -> DataFrame:
    """Per-SKU price/cost lookup for deriving realistic fact measures."""
    return dim_product_spark.select("product_sk", "list_price", "standard_cost", "msrp")


def date_seasonality_weights(dim_date: DataFrame) -> tuple[list[int], list[int]]:
    """Integer sampling weights per ``date_sk`` with a pronounced retail cycle.

    Baseline 100, modulated by weekends, Back-to-School, a December ramp toward
    Christmas, a sharp Cyber-5 peak (Black Friday / Cyber Monday), a mid-July sale
    event, and a post-holiday trough, plus mild YoY growth. Returned as two
    parallel lists (ordered by ``date_sk``) for dbldatagen ``values=``/``weights=``.
    """
    min_year = dim_date.agg(F.min("year")).collect()[0][0]
    dom = F.dayofmonth(F.col("date"))

    # Seasonal multiplier relative to baseline 100 (max stack ~7.0: Dec late weekend).
    season_mult = (
        F.when(F.col("is_weekend"), 1.4).otherwise(1.0)
        * F.when(F.col("selling_season") == "Back-to-School", 2.2)
        .when(F.col("selling_season") == "Holiday", 2.0)
        .when(F.col("selling_season") == "Post-Holiday", 0.7)
        .otherwise(1.0)
        # December ramp toward Christmas.
        * F.when(F.col("month") == 12, 1.0 + 0.06 * F.least(dom, F.lit(25))).otherwise(1.0)
        # Mid-July sale event (~Prime Day window).
        * F.when((F.col("month") == 7) & (dom >= 10) & (dom <= 16), 2.0).otherwise(1.0)
    )
    # Cyber-5 events as a MULTIPLIER above the max seasonal stack (8.0 > 7.0), so the
    # spike is the year's peak at any scale; growth below applies to both uniformly.
    event_mult = (
        F.when(F.col("holiday_name") == "Black Friday", 8.0)
        .when(F.col("holiday_name") == "Cyber Monday", 8.0)
        .when(F.col("holiday_name") == "Thanksgiving", 4.0)
        .otherwise(F.lit(0.0))
    )
    growth = F.lit(1.0) + F.lit(0.08) * (F.col("year") - F.lit(min_year))

    weighted = (
        dim_date.select(
            "date_sk",
            (F.lit(100.0) * F.greatest(season_mult, event_mult) * growth).alias("w"),
        )
        .withColumn("w", F.greatest(F.round("w").cast("int"), F.lit(1)))
        .orderBy("date_sk")
        .collect()
    )
    date_sks = [r["date_sk"] for r in weighted]
    weights = [r["w"] for r in weighted]
    return date_sks, weights
