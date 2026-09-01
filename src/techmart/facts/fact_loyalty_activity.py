"""fact_loyalty_activity: loyalty ledger; Earn events tie to real receipts."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import bounded_int, shifted_date_sk, uniform_hash

FACT_LOYALTY_ACTIVITY_SPEC = SparkTableSpec(
    schema="core",
    name="fact_loyalty_activity",
    grain="one row per loyalty ledger event",
    columns=[
        SparkColumn("loyalty_event_id", "long", "Degenerate loyalty event id", nullable=False),
        SparkColumn("date_sk", "long", "Event date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("channel_sk", "long", "Channel FK (dim_channel)", is_key=True, nullable=False),
        SparkColumn("activity_type", "string", "Earn/Redeem/Expire/Adjust", nullable=False),
        SparkColumn("points", "long", "Signed points delta (negative for Redeem/Expire)", nullable=False),
        SparkColumn("points_balance", "long", "Running per-customer points balance", nullable=False),
        SparkColumn("tier_at_event", "string", "Loyalty tier at the time of the event"),
        SparkColumn("related_transaction_id", "long", "Originating sales transaction id (Earn only); null otherwise"),
    ],
)


def build_fact_loyalty_activity(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_customer: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    max_date = dim_date.agg(F.max("date")).first()[0]
    members = dim_customer.filter(F.col("loyalty_member_flag")).select(
        "customer_sk", F.col("loyalty_tier").alias("tier_at_event"),
    )

    # --- Earn: one event per member receipt ---
    receipts = (
        fact_sales_line.join(members, "customer_sk")
        .groupBy("transaction_id", "customer_sk", "date_sk", "store_sk", "channel_sk", "tier_at_event")
        .agg(F.sum("loyalty_points_earned").alias("points"))
        .filter(F.col("points") > 0)
    )
    # Carry `_src` (source transaction id) on both branches so the event id can be
    # made provably unique before related_transaction_id is nulled for non-Earn.
    earn = (
        receipts
        .withColumn("activity_type", F.lit("Earn"))
        .withColumn("points", F.col("points").cast("long"))
        .withColumn("_src", F.col("transaction_id"))
        .drop("transaction_id")
    )

    # --- Redeem / Expire / Adjust: deterministic minority spun off member receipts ---
    keyed = (F.col("_src"), F.col("customer_sk"))
    extra = (
        earn.select("customer_sk", "store_sk", "channel_sk", "tier_at_event", "date_sk",
                    F.col("_src"), F.col("points").alias("_earned"))
        .filter(uniform_hash(*keyed, salt="extra") < F.lit(0.15))
        .join(dim_date.select("date_sk", "date"), "date_sk")
        .withColumn("_lag", bounded_int(*keyed, salt="elag", lo=1, hi=45))
        .withColumn("date_sk", shifted_date_sk(F.col("date"), F.col("_lag"), max_date))
        .withColumn("_k", bounded_int(*keyed, salt="ekind", lo=0, hi=2))  # 0=Redeem 1=Expire 2=Adjust
        .withColumn(
            "activity_type",
            F.when(F.col("_k") == 0, F.lit("Redeem")).when(F.col("_k") == 1, F.lit("Expire")).otherwise(F.lit("Adjust")),
        )
        .withColumn(
            "points",
            F.when(F.col("_k") == 0, -F.least(F.col("_earned"), F.lit(500).cast("long")))
            .when(F.col("_k") == 1, -F.floor(F.col("_earned") * F.lit(0.25)).cast("long"))
            .otherwise(bounded_int(*keyed, salt="adj", lo=-50, hi=50).cast("long")),
        )
        .select("customer_sk", "date_sk", "store_sk", "channel_sk", "tier_at_event", "activity_type", "points", "_src")
    )

    events = earn.select(
        "customer_sk", "date_sk", "store_sk", "channel_sk", "tier_at_event", "activity_type", "points", "_src",
    ).unionByName(extra)

    # (customer_sk, activity_type, _src) is unique: one Earn per receipt and at most
    # one non-Earn kind per source receipt, so the hashed id is collision-free.
    events = (
        events
        .withColumn("loyalty_event_id", F.abs(F.hash("customer_sk", "activity_type", "_src")).cast("long"))
        .withColumn(
            "related_transaction_id",
            F.when(F.col("activity_type") == F.lit("Earn"), F.col("_src")).otherwise(F.lit(None).cast("long")),
        )
        .drop("_src")
    )
    bal_win = Window.partitionBy("customer_sk").orderBy("date_sk", "loyalty_event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    events = events.withColumn("points_balance", F.sum("points").over(bal_win).cast("long"))

    return FACT_LOYALTY_ACTIVITY_SPEC.select_ordered(events)
