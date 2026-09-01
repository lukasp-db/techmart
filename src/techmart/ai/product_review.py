"""product_review: deterministic structure + ai_query prompts (text filled by the SQL task)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec

# Final table (post ai_query fill).
PRODUCT_REVIEW_SPEC = SparkTableSpec(
    schema="ai",
    name="product_review",
    grain="one row per product review",
    columns=[
        SparkColumn("review_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Review date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("rating", "int", "Star rating 1-5", nullable=False),
        SparkColumn("review_title", "string", "LLM-generated review title"),
        SparkColumn("review_text", "string", "LLM-generated review body"),
        SparkColumn("verified_purchase", "boolean", "Tied to a real purchase", nullable=False),
        SparkColumn("helpful_votes", "int", "Helpful-vote count"),
    ],
)

# Staging table (structure + prompts; text columns absent).
PRODUCT_REVIEW_STAGING_SPEC = SparkTableSpec(
    schema="ai",
    name="_product_review_staging",
    grain="staging: product review structure + ai_query prompts",
    columns=[
        SparkColumn("review_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Review date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("rating", "int", "Star rating 1-5", nullable=False),
        SparkColumn("verified_purchase", "boolean", "Tied to a real purchase", nullable=False),
        SparkColumn("helpful_votes", "int", "Helpful-vote count"),
        SparkColumn("prompt", "string", "ai_query prompt for the review body", nullable=False),
        SparkColumn("title_prompt", "string", "ai_query prompt for the review title", nullable=False),
    ],
)


def build_product_review_staging(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    # Deterministically sample real sales lines up to num_reviews.
    total = fact_sales_line.count()
    frac = min(1.0, (sp.num_reviews * 3.0) / max(total, 1))
    pick = uniform_hash(F.col("transaction_id"), F.col("line_number"), salt="review_pick")
    candidates = (
        fact_sales_line
        .select("transaction_id", "line_number", "product_sk", "customer_sk", "date_sk")
        .withColumn("_r", pick)
        .filter(F.col("_r") < F.lit(frac))
        .orderBy("_r", "transaction_id", "line_number")
        .limit(sp.num_reviews)
    )

    prod = dim_product.select(
        F.col("product_sk").alias("_p"), "product_name", "category_name"
    )
    j = candidates.join(prod, candidates["product_sk"] == prod["_p"], "left").drop("_p")

    rating = bounded_int(F.col("transaction_id"), F.col("line_number"), salt="rating", lo=1, hi=5)
    # skew toward 4-5: map a second uniform through a simple weighting
    skew = uniform_hash(F.col("transaction_id"), F.col("line_number"), salt="skew")
    rating = F.when(skew < F.lit(0.7), F.greatest(rating, F.lit(4))).otherwise(rating)

    # Deterministic review_id from stable keys (never monotonically_increasing_id / uuid).
    df = (
        j
        .withColumn("rating", rating)
        .withColumn("review_id", F.xxhash64(F.col("transaction_id"), F.col("line_number"), F.lit("review")))
        .withColumn("verified_purchase", F.lit(True))
        .withColumn("helpful_votes",
                    bounded_int(F.col("transaction_id"), F.col("line_number"), salt="votes", lo=0, hi=50))
        .withColumn(
            "prompt",
            F.concat(
                F.lit("Write a concise, realistic customer review body (2-4 sentences) for the product '"),
                F.coalesce(F.col("product_name"), F.lit("this product")),
                F.lit("' in the category '"),
                F.coalesce(F.col("category_name"), F.lit("electronics")),
                F.lit("'. The reviewer gave "), F.col("rating").cast("string"),
                F.lit(" out of 5 stars. Match the sentiment to the rating. Do not include a title or rating."),
            ),
        )
        .withColumn(
            "title_prompt",
            F.concat(
                F.lit("Write a short review title (max 8 words) for a "),
                F.col("rating").cast("string"),
                F.lit("-star review of '"),
                F.coalesce(F.col("product_name"), F.lit("this product")),
                F.lit("'. Title only, no quotes."),
            ),
        )
    )
    return PRODUCT_REVIEW_STAGING_SPEC.select_ordered(df)
