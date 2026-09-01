"""fact_inventory_snapshot: store x SKU x day stock position (standalone)."""
from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import uniform_hash
from .lookups import product_economics

FACT_INVENTORY_SNAPSHOT_SPEC = SparkTableSpec(
    schema="core",
    name="fact_inventory_snapshot",
    grain="one row per store, SKU, and day (stock position)",
    columns=[
        SparkColumn("date_sk", "long", "Snapshot date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("on_hand_qty", "int", "Units physically on hand", nullable=False),
        SparkColumn("on_order_qty", "int", "Units on open purchase orders"),
        SparkColumn("in_transit_qty", "int", "Units in transit to the store"),
        SparkColumn("reserved_qty", "int", "Units reserved for orders"),
        SparkColumn("available_qty", "int", "On hand minus reserved", nullable=False),
        SparkColumn("safety_stock_qty", "int", "Safety-stock threshold"),
        SparkColumn("reorder_point", "int", "Reorder-point threshold"),
        SparkColumn("unit_cost", "double", "Standard cost per unit"),
        SparkColumn("on_hand_retail_value", "double", "on_hand_qty * list_price"),
        SparkColumn("on_hand_cost_value", "double", "on_hand_qty * unit_cost"),
        SparkColumn("days_of_supply", "double", "On hand divided by average daily demand"),
        SparkColumn("is_out_of_stock", "boolean", "True when on_hand_qty is zero", nullable=False),
    ],
)


def build_fact_inventory_snapshot(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    dim_store: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Cross join store x product x recent days; derive deterministic stock levels."""
    n_days = config.scale_profile.inventory_snapshot_days
    start_snap = config.end_date - timedelta(days=n_days - 1)

    dates = dim_date.filter(F.col("date") >= F.lit(start_snap)).select("date_sk")
    stores = dim_store.select("store_sk")
    prods = product_economics(dim_product).select(
        "product_sk",
        F.round(F.col("list_price"), 2).alias("list_price"),
        F.round(F.col("standard_cost"), 2).alias("unit_cost"),
    )

    grid = stores.crossJoin(prods).crossJoin(dates)

    def cell(salt: str):
        return uniform_hash(F.col("store_sk"), F.col("product_sk"), F.col("date_sk"), salt=salt)

    # Per (store, SKU) baseline is stable across days (keyed without date_sk).
    base = (F.floor(uniform_hash(F.col("store_sk"), F.col("product_sk"), salt="base") * F.lit(180)) + F.lit(20)).cast("int")

    df = (
        grid
        .withColumn("_base", base)
        .withColumn("_avg_daily", F.greatest(F.floor(F.col("_base") / F.lit(30.0)).cast("int"), F.lit(1)))
        .withColumn(
            "on_hand_qty",
            F.greatest((F.col("_base") + (F.floor((cell("oh") - F.lit(0.5)) * F.lit(40))).cast("int")).cast("int"), F.lit(0)),
        )
        .withColumn("reserved_qty", F.floor(cell("rs") * F.lit(5)).cast("int"))
        .withColumn("available_qty", F.greatest(F.col("on_hand_qty") - F.col("reserved_qty"), F.lit(0)))
        .withColumn("safety_stock_qty", (F.col("_avg_daily") * F.lit(3)).cast("int"))
        .withColumn("reorder_point", (F.col("_avg_daily") * F.lit(7)).cast("int"))
        .withColumn(
            "on_order_qty",
            F.when(F.col("on_hand_qty") < F.col("reorder_point"), (F.col("_avg_daily") * F.lit(14)).cast("int")).otherwise(F.lit(0)),
        )
        .withColumn(
            "in_transit_qty",
            F.when(F.col("on_order_qty") > F.lit(0), F.floor(cell("it") * F.col("on_order_qty")).cast("int")).otherwise(F.lit(0)),
        )
        .withColumn("on_hand_retail_value", F.round(F.col("on_hand_qty") * F.col("list_price"), 2))
        .withColumn("on_hand_cost_value", F.round(F.col("on_hand_qty") * F.col("unit_cost"), 2))
        .withColumn("days_of_supply", F.round(F.col("on_hand_qty") / F.col("_avg_daily"), 1))
        .withColumn("is_out_of_stock", F.col("on_hand_qty") == F.lit(0))
        .drop("list_price", "_base", "_avg_daily")
    )
    return FACT_INVENTORY_SNAPSHOT_SPEC.select_ordered(df)
