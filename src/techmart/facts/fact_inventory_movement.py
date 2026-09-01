"""fact_inventory_movement: stock-ledger movement events (standalone)."""
from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .lookups import product_economics

FACT_INVENTORY_MOVEMENT_SPEC = SparkTableSpec(
    schema="core",
    name="fact_inventory_movement",
    grain="one row per inventory movement event",
    columns=[
        SparkColumn("movement_id", "long", "Degenerate movement id", nullable=False),
        SparkColumn("date_sk", "long", "Movement date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("vendor_sk", "long", "Vendor FK (dim_vendor); null for non-vendor movements", is_key=True),
        SparkColumn("movement_type", "string", "Receipt/Transfer/Adjustment/Shrink/Return-to-Vendor", nullable=False),
        SparkColumn("quantity", "int", "Signed unit quantity (negative reduces stock)", nullable=False),
        SparkColumn("unit_cost", "double", "Standard cost per unit"),
        SparkColumn("reference_doc_id", "string", "Source document reference"),
        SparkColumn("reason_code", "string", "Reason/category code for the movement"),
    ],
)

_MOVEMENT_TYPES = ["Receipt", "Transfer", "Adjustment", "Shrink", "Return-to-Vendor"]
_TYPE_WEIGHTS = [45, 25, 15, 10, 5]


def build_fact_inventory_movement(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    dim_date: DataFrame,
    dim_product: DataFrame,
    dim_counts: dict,
    rows: int | None = None,
    seed: int | None = None,
) -> DataFrame:
    n = rows if rows is not None else config.scale_profile.inventory_movements_target
    seed = seed if seed is not None else config.seed
    partitions = max(1, min(64, n // 500_000))

    date_sks = [r["date_sk"] for r in dim_date.select("date_sk").orderBy("date_sk").collect()]

    gen = (
        dg.DataGenerator(
            spark, name="fact_inventory_movement", rows=n, partitions=partitions,
            randomSeed=seed, randomSeedMethod="hash_fieldname",
        )
        .withIdOutput()
        .withColumn("movement_id", "long", expr="id + 1", baseColumn="id")
        .withColumn("date_sk", "long", values=date_sks, random=True)
        .withColumn("product_sk", "long", minValue=1, maxValue=dim_counts["product"], random=True)
        .withColumn("store_sk", "long", minValue=1, maxValue=dim_counts["store"], random=True)
        .withColumn("movement_type", "string", values=_MOVEMENT_TYPES, weights=_TYPE_WEIGHTS, random=True)
        .withColumn("abs_qty", "int", minValue=1, maxValue=200, random=True)
    )
    df = gen.build().drop("id")

    # Signed quantity by type: Shrink / Return-to-Vendor reduce stock.
    df = (
        df
        .withColumn(
            "quantity",
            F.when(F.col("movement_type").isin("Shrink", "Return-to-Vendor"), -F.col("abs_qty"))
            .otherwise(F.col("abs_qty")),
        )
        # vendor_sk only for vendor-facing movements
        .withColumn(
            "vendor_sk",
            F.when(
                F.col("movement_type").isin("Receipt", "Return-to-Vendor"),
                (F.pmod(F.hash(F.col("movement_id"), F.lit("vend")), F.lit(dim_counts["vendor"])) + F.lit(1)).cast("long"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn("reference_doc_id", F.concat(F.lit("MOV-"), F.col("movement_id").cast("string")))
        .withColumn(
            "reason_code",
            F.element_at(
                F.array(F.lit("PO"), F.lit("XFER"), F.lit("CYCLE-COUNT"), F.lit("DAMAGE"), F.lit("RTV")),
                (F.pmod(F.hash(F.col("movement_id"), F.lit("rc")), F.lit(5)) + F.lit(1)),
            ),
        )
        .drop("abs_qty")
    )

    econ = product_economics(dim_product).select(
        F.col("product_sk").alias("_econ_sk"), F.round(F.col("standard_cost"), 2).alias("unit_cost"),
    )
    df = df.join(econ, df["product_sk"] == econ["_econ_sk"], "left").drop("_econ_sk")

    return FACT_INVENTORY_MOVEMENT_SPEC.select_ordered(df)
