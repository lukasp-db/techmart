from __future__ import annotations

import polars as pl
from pyspark.sql import DataFrame, SparkSession, functions as F


def polars_to_spark(spark: SparkSession, pl_df: pl.DataFrame) -> DataFrame:
    """Convert a Polars DataFrame to a Spark DataFrame (via pandas)."""
    return spark.createDataFrame(pl_df.to_pandas())


def product_economics(spark: SparkSession, dim_product_pl: pl.DataFrame) -> DataFrame:
    """Per-SKU price/cost lookup for deriving realistic fact measures."""
    econ = dim_product_pl.select(["product_sk", "list_price", "standard_cost", "msrp"])
    return polars_to_spark(spark, econ)


def date_seasonality_weights(dim_date: DataFrame) -> tuple[list[int], list[int]]:
    """Integer sampling weights per ``date_sk`` from seasonality signals.

    Baseline 100, lifted by weekends, the Holiday and Back-to-School selling
    seasons, Black Friday, and a mild year-over-year growth trend. Returned as
    two parallel lists (ordered by ``date_sk``) suitable for dbldatagen
    ``values=`` / ``weights=``.
    """
    min_year = dim_date.agg(F.min("year")).collect()[0][0]
    weighted = (
        dim_date.select(
            "date_sk",
            (
                F.lit(100.0)
                * F.when(F.col("is_weekend"), 1.5).otherwise(1.0)
                * F.when(F.col("selling_season") == "Holiday", 2.5)
                .when(F.col("selling_season") == "Back-to-School", 1.8)
                .otherwise(1.0)
                * F.when(F.col("holiday_name") == "Black Friday", 3.0).otherwise(1.0)
                * (1.0 + 0.08 * (F.col("year") - F.lit(min_year)))
            ).alias("w"),
        )
        .withColumn("w", F.greatest(F.round("w").cast("int"), F.lit(1)))
        .orderBy("date_sk")
        .collect()
    )
    date_sks = [r["date_sk"] for r in weighted]
    weights = [r["w"] for r in weighted]
    return date_sks, weights
