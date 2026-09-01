from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig, load_config
from ..facts.fact_fulfillment import FACT_FULFILLMENT_SPEC, build_fact_fulfillment
from ..facts.fact_inventory_movement import FACT_INVENTORY_MOVEMENT_SPEC, build_fact_inventory_movement
from ..facts.fact_inventory_snapshot import FACT_INVENTORY_SNAPSHOT_SPEC, build_fact_inventory_snapshot
from ..facts.fact_loyalty_activity import FACT_LOYALTY_ACTIVITY_SPEC, build_fact_loyalty_activity
from ..facts.fact_returns import FACT_RETURNS_SPEC, build_fact_returns
from ..facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line
from ..facts.fact_web_events import FACT_WEB_EVENTS_SPEC, build_fact_web_events
from ..spark.framework import validate_spark_schema
from ..spark.session import get_spark
from ..spark.uc_write import write_table_uc

# config/ sits at the repo/bundle root, three levels up from this file
# (jobs -> techmart -> src -> root). Works locally and when synced into a DAB.
_DEFAULT_PROFILES_PATH = Path(__file__).resolve().parents[3] / "config" / "scale_profiles.yaml"


def generate_sales_line_local(
    spark: SparkSession,
    config: TechmartConfig,
    dim_product: DataFrame,
    dim_date: DataFrame,
    dim_counts: dict,
    *,
    rows: int | None = None,
) -> DataFrame:
    """Build the sales fact from Spark dim DataFrames.

    Used by tests and local runs. ``dim_counts`` gives the actual row counts
    of each dimension table, guaranteeing RI by construction.  On serverless,
    ``main`` reads all dims from Unity Catalog and derives these counts there.
    """
    df = build_fact_sales_line(
        spark, config,
        dim_product=dim_product,
        dim_date=dim_date,
        dim_counts=dim_counts,
        rows=rows,
    )
    validate_spark_schema(df, FACT_SALES_LINE_SPEC)
    return df


def main(argv: list[str] | None = None) -> int:
    """Serverless DAB entrypoint: read dims from UC, write all core facts to UC."""
    parser = argparse.ArgumentParser(description="Generate Techmart core facts.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", default="techmart_")
    parser.add_argument("--profile", default=None, help="Scale profile name.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--profiles-path", default=str(_DEFAULT_PROFILES_PATH))
    args = parser.parse_args(argv)

    config = load_config(
        Path(args.profiles_path), args.profile,
        seed=args.seed, catalog=args.catalog, schema_prefix=args.schema_prefix,
    )
    spark = get_spark("techmart-generate-facts")
    core = f"{config.catalog}.{config.schema_prefix}core"

    # Read all dims from UC; derive FK cardinality from actual row counts.
    dim_product = spark.read.table(f"{core}.dim_product")
    dim_date = spark.read.table(f"{core}.dim_date")
    dim_store = spark.read.table(f"{core}.dim_store")
    dim_customer = spark.read.table(f"{core}.dim_customer")
    dim_employee = spark.read.table(f"{core}.dim_employee")
    dim_promotion = spark.read.table(f"{core}.dim_promotion")
    dim_vendor = spark.read.table(f"{core}.dim_vendor")

    dim_counts = {
        "store": dim_store.count(),
        "customer": dim_customer.count(),
        "employee": dim_employee.count(),
        "promotion": dim_promotion.count(),
        "vendor": dim_vendor.count(),
        "product": dim_product.count(),
    }

    # --- sales (anchor) ---
    sales = build_fact_sales_line(
        spark, config,
        dim_product=dim_product,
        dim_date=dim_date,
        dim_counts=dim_counts,
    )
    target = write_table_uc(spark, sales, FACT_SALES_LINE_SPEC, config.catalog, config.schema_prefix)
    print(f"wrote {target}")
    # Read back the persisted table so sales-linked facts operate on the
    # deterministic, written data (mirrors the notebook pattern).
    sales = spark.read.table(f"{core}.fact_sales_line")

    # --- standalone facts ---
    target = write_table_uc(
        spark,
        build_fact_inventory_snapshot(spark, config, dim_store=dim_store, dim_product=dim_product, dim_date=dim_date),
        FACT_INVENTORY_SNAPSHOT_SPEC, config.catalog, config.schema_prefix,
    )
    print(f"wrote {target}")

    target = write_table_uc(
        spark,
        build_fact_inventory_movement(spark, config, dim_date=dim_date, dim_product=dim_product, dim_counts=dim_counts),
        FACT_INVENTORY_MOVEMENT_SPEC, config.catalog, config.schema_prefix,
    )
    print(f"wrote {target}")

    target = write_table_uc(
        spark,
        build_fact_web_events(spark, config, dim_date=dim_date, dim_counts=dim_counts),
        FACT_WEB_EVENTS_SPEC, config.catalog, config.schema_prefix,
    )
    print(f"wrote {target}")

    # --- sales-linked facts ---
    target = write_table_uc(
        spark,
        build_fact_returns(spark, config, fact_sales_line=sales, dim_date=dim_date),
        FACT_RETURNS_SPEC, config.catalog, config.schema_prefix,
    )
    print(f"wrote {target}")

    target = write_table_uc(
        spark,
        build_fact_fulfillment(spark, config, fact_sales_line=sales, dim_date=dim_date),
        FACT_FULFILLMENT_SPEC, config.catalog, config.schema_prefix,
    )
    print(f"wrote {target}")

    target = write_table_uc(
        spark,
        build_fact_loyalty_activity(spark, config, fact_sales_line=sales, dim_customer=dim_customer, dim_date=dim_date),
        FACT_LOYALTY_ACTIVITY_SPEC, config.catalog, config.schema_prefix,
    )
    print(f"wrote {target}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
