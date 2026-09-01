"""replenishment_order: operational replenishment suggestions seeded from inventory snapshots."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn
from .pg_write import PgTableSpec

_PLANNERS = ("planner_amir", "planner_bianca", "planner_chen", "planner_dana")

REPLENISHMENT_ORDER_SPEC = PgTableSpec(
    schema="ops",
    name="replenishment_order",
    grain="one row per suggested replenishment (product x store, latest snapshot)",
    columns=[
        SparkColumn("replen_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", nullable=False),
        SparkColumn("suggested_qty", "int", "System-suggested reorder units", nullable=False),
        SparkColumn("approved_qty", "int", "Planner-approved units (null while Suggested)"),
        SparkColumn("status", "string", "Suggested/Approved/Rejected/Ordered", nullable=False),
        SparkColumn("reorder_point", "int", "Reorder-point threshold from the snapshot", nullable=False),
        SparkColumn("created_by", "string", "Creator (system)", nullable=False),
        SparkColumn("approved_by", "string", "Approving planner (null while Suggested)"),
        SparkColumn("created_at", "timestamp", "Row creation time (deterministic)", nullable=False),
        SparkColumn("updated_at", "timestamp", "Last update time (deterministic)", nullable=False),
    ],
    primary_key=("replen_id",),
)


def build_replenishment_order(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_inventory_snapshot: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    max_date_sk = fact_inventory_snapshot.agg(F.max("date_sk")).first()[0]
    snap = (
        fact_inventory_snapshot
        .filter(F.col("date_sk") == F.lit(max_date_sk))
        .filter(F.col("available_qty") <= F.col("reorder_point"))
        .select("date_sk", "store_sk", "product_sk",
                "available_qty", "reorder_point", "safety_stock_qty")
    )

    pick = uniform_hash(F.col("product_sk"), F.col("store_sk"), salt="replen_pick")
    cand = (
        snap.withColumn("_r", pick)
        .orderBy("_r", "product_sk", "store_sk")
        .limit(sp.num_replen_orders)
    )

    dd = dim_date.select("date_sk", "date")
    j = cand.join(dd, "date_sk")

    keys = (F.col("product_sk"), F.col("store_sk"))
    suggested = F.greatest(
        F.col("reorder_point") + F.col("safety_stock_qty") - F.col("available_qty"), F.lit(0)
    ).cast("int")
    u = uniform_hash(*keys, salt="status")
    status = (
        F.when(u < F.lit(0.6), F.lit("Suggested"))
        .when(u < F.lit(0.8), F.lit("Approved"))
        .when(u < F.lit(0.9), F.lit("Ordered"))
        .otherwise(F.lit("Rejected"))
    )
    planner_idx = bounded_int(*keys, salt="planner", lo=1, hi=len(_PLANNERS))
    planner = F.element_at(F.array(*[F.lit(p) for p in _PLANNERS]), planner_idx)
    approved = F.greatest(
        F.col("suggested_qty") + bounded_int(*keys, salt="appr", lo=-2, hi=2), F.lit(0)
    ).cast("int")

    df = (
        j
        .withColumn("suggested_qty", suggested)
        .withColumn("status", status)
        .withColumn("replen_id", F.xxhash64(F.col("product_sk"), F.col("store_sk"),
                                            F.col("date_sk"), F.lit("replen")))
        .withColumn("approved_qty",
                    F.when(F.col("status") == F.lit("Suggested"), F.lit(None).cast("int"))
                    .otherwise(approved))
        .withColumn("created_by", F.lit("system"))
        .withColumn("approved_by",
                    F.when(F.col("status") == F.lit("Suggested"), F.lit(None).cast("string"))
                    .otherwise(planner))
        .withColumn("created_at", F.col("date").cast("timestamp"))
        .withColumn("updated_at",
                    F.expr("CASE WHEN status = 'Suggested' THEN created_at "
                           "ELSE created_at + INTERVAL 2 DAYS END"))
    )
    return REPLENISHMENT_ORDER_SPEC.select_ordered(df)
