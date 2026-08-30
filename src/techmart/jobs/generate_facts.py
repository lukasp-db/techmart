from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl
from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig, load_config
from ..facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line
from ..facts.lookups import date_seasonality_weights, polars_to_spark, product_economics
from ..spark.framework import validate_fact_schema
from ..spark.session import get_spark

_DATE_WEIGHT_COLS = ["date_sk", "is_weekend", "selling_season", "holiday_name", "year"]


def generate_sales_line_local(
    spark: SparkSession,
    config: TechmartConfig,
    dim_product_pl: pl.DataFrame,
    dim_date_pl: pl.DataFrame,
    *,
    rows: int | None = None,
) -> DataFrame:
    """Assemble lookups from in-memory Polars dims and build the sales fact.

    Used by tests and local runs. On serverless, ``main`` reads the dims from
    Unity Catalog instead (see below).
    """
    econ = product_economics(spark, dim_product_pl)
    dd = polars_to_spark(spark, dim_date_pl.select(_DATE_WEIGHT_COLS))
    weights = date_seasonality_weights(dd)
    df = build_fact_sales_line(spark, config, product_econ=econ, date_weights=weights, rows=rows)
    validate_fact_schema(df, FACT_SALES_LINE_SPEC)
    return df


def main(argv: list[str] | None = None) -> int:
    """Serverless DAB entrypoint: read dims from UC, write fact_sales_line to UC."""
    parser = argparse.ArgumentParser(description="Generate Techmart fact_sales_line.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", default="techmart_")
    parser.add_argument("--profile", default=None, help="Scale profile name.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--profiles-path", default="config/scale_profiles.yaml")
    args = parser.parse_args(argv)

    config = load_config(
        Path(args.profiles_path), args.profile,
        seed=args.seed, catalog=args.catalog, schema_prefix=args.schema_prefix,
    )
    spark = get_spark("techmart-generate-facts")
    core = f"{config.catalog}.{config.schema_prefix}core"

    dim_product = spark.read.table(f"{core}.dim_product")
    dim_date = spark.read.table(f"{core}.dim_date")

    econ = dim_product.select("product_sk", "list_price", "standard_cost", "msrp")
    weights = date_seasonality_weights(dim_date.select(*_DATE_WEIGHT_COLS))
    df = build_fact_sales_line(spark, config, product_econ=econ, date_weights=weights)
    validate_fact_schema(df, FACT_SALES_LINE_SPEC)

    target = f"{core}.{FACT_SALES_LINE_SPEC.name}"
    df.write.mode("overwrite").saveAsTable(target)
    print(f"wrote {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
