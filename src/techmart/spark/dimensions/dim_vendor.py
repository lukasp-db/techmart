"""Spark dim_vendor builder using dbldatagen + SCD2."""
from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ..framework import SparkColumn, SparkTableSpec
from ..scd2 import scd2_columns, with_scd2_current

_VENDOR_TYPES = ["Manufacturer", "Distributor", "Marketplace-Seller"]
_CATEGORIES = ["Computing", "Consumer Electronics", "Appliances", "Networking & DIY", "Services"]
_PAYMENT_TERMS = ["NET30", "NET45", "NET60", "NET90"]
_VENDOR_NAME_STEMS = [
    "Apex", "Summit", "Vertex", "Pioneer", "Horizon", "Nimbus", "Quantum",
    "Beacon", "Cascade", "Meridian", "Atlas", "Catalyst", "Orbit", "Vanguard",
]
_VENDOR_NAME_TAILS = ["Electronics", "Supply", "Distribution", "Trading", "Technologies", "Brands"]

_BASE_COLUMNS: list[SparkColumn] = [
    SparkColumn("vendor_sk", "long", "Surrogate key", is_key=True, nullable=False),
    SparkColumn("vendor_id", "string", "Business key", nullable=False),
    SparkColumn("vendor_name", "string", "Vendor name"),
    SparkColumn("vendor_type", "string", "Manufacturer/Distributor/Marketplace-Seller"),
    SparkColumn("primary_category", "string", "Primary product category supplied"),
    SparkColumn("country", "string", "Country code"),
    SparkColumn("relationship_start_date", "date", "Date the vendor relationship began"),
    SparkColumn("preferred_flag", "boolean", "Preferred vendor"),
    SparkColumn("vendor_scorecard_rating", "long", "Scorecard rating 1-5"),
    SparkColumn("avg_lead_time_days", "long", "Average lead time in days"),
    SparkColumn("payment_terms", "string", "Payment terms"),
    SparkColumn("active_flag", "boolean", "Active vendor"),
]

DIM_VENDOR_SPEC = SparkTableSpec(
    schema="core",
    name="dim_vendor",
    grain="one current row per vendor (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)

# Pre-build SQL array literals for element_at expressions.
_STEMS_ARR = "array(" + ", ".join(f"'{s}'" for s in _VENDOR_NAME_STEMS) + ")"
_TAILS_ARR = "array(" + ", ".join(f"'{t}'" for t in _VENDOR_NAME_TAILS) + ")"


def build_dim_vendor(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    """Generate dim_vendor rows with dbldatagen; mark all rows as SCD2 current."""
    n = config.scale_profile.num_vendors

    df = (
        dg.DataGenerator(
            spark,
            name="dim_vendor",
            rows=n,
            partitions=max(1, min(64, n // 100_000)),
            randomSeed=config.seed,
            randomSeedMethod="fixed",
        )
        .withIdOutput()
        # --- surrogate / business keys ---
        .withColumn("vendor_sk", "long", expr="id + 1", baseColumn="id")
        .withColumn(
            "vendor_id", "string",
            expr="concat('VEND', lpad(cast(id + 1 as string), 5, '0'))",
            baseColumn="id",
        )
        # --- vendor name: stem + tail from independent index columns ---
        .withColumn(
            "stem_idx", "int",
            minValue=1, maxValue=len(_VENDOR_NAME_STEMS),
            random=True, omit=True,
        )
        .withColumn(
            "tail_idx", "int",
            minValue=1, maxValue=len(_VENDOR_NAME_TAILS),
            random=True, omit=True,
        )
        .withColumn(
            "vendor_name", "string",
            expr=f"concat(element_at({_STEMS_ARR}, stem_idx), ' ', element_at({_TAILS_ARR}, tail_idx))",
            baseColumn=["stem_idx", "tail_idx"],
        )
        # --- classification ---
        .withColumn("vendor_type", "string", values=_VENDOR_TYPES, random=True)
        .withColumn("primary_category", "string", values=_CATEGORIES, random=True)
        .withColumn("country", "string", expr="'US'")
        # --- relationship start date: 2000-01-01 + 0..7304 days (up to ~2020-01-01) ---
        .withColumn("rel_off", "int", minValue=0, maxValue=7304, random=True, omit=True)
        .withColumn(
            "relationship_start_date", "date",
            expr="date_add(to_date('2000-01-01'), rel_off)",
            baseColumn="rel_off",
        )
        # --- flags and ratings ---
        .withColumn(
            "preferred_flag", "boolean",
            expr="pmod(abs(hash(id, 'pref')), 2) = 0",
            baseColumn="id",
        )
        .withColumn(
            "vendor_scorecard_rating", "long",
            minValue=1, maxValue=5, random=True,
        )
        .withColumn(
            "avg_lead_time_days", "long",
            minValue=3, maxValue=44, random=True,
        )
        .withColumn("payment_terms", "string", values=_PAYMENT_TERMS, random=True)
        .withColumn("active_flag", "boolean", expr="true")
        .build()
        .drop("id")
    )
    df = with_scd2_current(df, config.start_date)
    return DIM_VENDOR_SPEC.select_ordered(df)
