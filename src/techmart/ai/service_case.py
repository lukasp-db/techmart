"""service_case: deterministic structure + ai_query prompts (text filled by the SQL task)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec

SERVICE_CASE_SPEC = SparkTableSpec(
    schema="ai",
    name="service_case",
    grain="one row per service case",
    columns=[
        SparkColumn("case_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Case open date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("case_type", "string", "Repair/Warranty/Support", nullable=False),
        SparkColumn("channel", "string", "Phone/In-Store/Online", nullable=False),
        SparkColumn("status", "string", "Open/In-Progress/Resolved/Closed", nullable=False),
        SparkColumn("case_notes", "string", "LLM-generated case notes"),
        SparkColumn("resolution_notes", "string", "LLM-generated resolution (null if unresolved)"),
        SparkColumn("csat_score", "int", "Customer satisfaction 1-5", nullable=False),
    ],
)

SERVICE_CASE_STAGING_SPEC = SparkTableSpec(
    schema="ai",
    name="_service_case_staging",
    grain="staging: service case structure + ai_query prompts",
    columns=[
        SparkColumn("case_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Case open date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("case_type", "string", "Repair/Warranty/Support", nullable=False),
        SparkColumn("channel", "string", "Phone/In-Store/Online", nullable=False),
        SparkColumn("status", "string", "Open/In-Progress/Resolved/Closed", nullable=False),
        SparkColumn("csat_score", "int", "Customer satisfaction 1-5", nullable=False),
        SparkColumn("notes_prompt", "string", "ai_query prompt for case notes", nullable=False),
        SparkColumn("resolution_prompt", "string", "ai_query prompt for resolution (null if unresolved)"),
    ],
)

_CASE_TYPES = ("Repair", "Warranty", "Support")
_CHANNELS = ("Phone", "In-Store", "Online")
_STATUSES = ("Open", "In-Progress", "Resolved", "Closed")


def build_service_case_staging(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    # Sample real (customer, product, store, date) tuples from sales lines up to num_service_cases.
    total = fact_sales_line.count()
    frac = min(1.0, (sp.num_service_cases * 3.0) / max(total, 1))
    pick = uniform_hash(F.col("transaction_id"), F.col("line_number"), salt="case_pick")
    src = (
        fact_sales_line
        .select("transaction_id", "line_number", "product_sk", "customer_sk", "store_sk", "date_sk")
        .withColumn("_r", pick)
        .filter(F.col("_r") < F.lit(frac))
        .orderBy("_r", "transaction_id", "line_number")
        .limit(sp.num_service_cases)
    )

    def _pick(salt: str, values: tuple[str, ...]):
        idx = bounded_int(F.col("transaction_id"), F.col("line_number"), salt=salt,
                          lo=1, hi=len(values))
        return F.element_at(F.array(*[F.lit(v) for v in values]), idx)

    df = (
        src
        .withColumn("case_id", F.xxhash64(F.col("transaction_id"), F.col("line_number"), F.lit("case")))
        .withColumn("case_type", _pick("ctype", _CASE_TYPES))
        .withColumn("channel", _pick("cchan", _CHANNELS))
        .withColumn("status", _pick("cstat", _STATUSES))
        .withColumn("csat_score",
                    bounded_int(F.col("transaction_id"), F.col("line_number"), salt="csat", lo=1, hi=5))
        .withColumn(
            "notes_prompt",
            F.concat(
                F.lit("Write brief support-case notes (1-3 sentences) for a "),
                F.col("case_type"),
                F.lit(" case opened via "), F.col("channel"),
                F.lit(". Describe the customer's reported issue for a consumer-electronics product."),
            ),
        )
        .withColumn(
            "resolution_prompt",
            F.when(
                F.col("status").isin("Resolved", "Closed"),
                F.concat(
                    F.lit("Write a brief resolution note (1-2 sentences) for a resolved "),
                    F.col("case_type"), F.lit(" case. State how it was fixed."),
                ),
            ).otherwise(F.lit(None).cast("string")),
        )
    )
    return SERVICE_CASE_STAGING_SPEC.select_ordered(df)
