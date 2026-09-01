"""fact_inventory_valuation: finance view of inventory, ties to fact_inventory_snapshot."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .periods import date_periods, period_end_lookup

FACT_INVENTORY_VALUATION_SPEC = SparkTableSpec(
    schema="finance",
    name="fact_inventory_valuation",
    grain="one row per store × category × fiscal period",
    columns=[
        SparkColumn("date_sk", "long", "Period-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("category_id", "string", "Product category FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("category_name", "string", "Product category name"),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("on_hand_cost_value", "double", "Period-end inventory at cost"),
        SparkColumn("on_hand_retail_value", "double", "Period-end inventory at retail"),
        SparkColumn("cogs_amount", "double", "Category COGS for the period"),
        SparkColumn("markdown_amount", "double", "Injected markdown value"),
        SparkColumn("shrink_amount", "double", "Category-level shrink proxy"),
        SparkColumn("gmroi", "double", "Gross-margin return on inventory investment"),
    ],
)


def build_fact_inventory_valuation(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_inventory_snapshot: DataFrame,
    fact_sales_line: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    markdown_rate = config.scale_profile.markdown_rate
    periods = date_periods(dim_date)
    pe = period_end_lookup(dim_date)
    cat = dim_product.select("product_sk", "category_id", "category_name")

    # period-end inventory position by (store, category, period-end date_sk)
    pe_sk = pe.select(
        F.col("period_end_date_sk").alias("date_sk"), "pidx", "fiscal_year", "fiscal_period"
    )
    base = (
        fact_inventory_snapshot.select("store_sk", "product_sk", "date_sk", "on_hand_cost_value", "on_hand_retail_value")
        .join(pe_sk, "date_sk")
        .join(cat, "product_sk")
        .groupBy("store_sk", "category_id", "category_name", "date_sk", "pidx", "fiscal_year", "fiscal_period")
        .agg(
            F.sum("on_hand_cost_value").alias("on_hand_cost_value"),
            F.sum("on_hand_retail_value").alias("on_hand_retail_value"),
        )
    )

    # sales rolled to (store, category, pidx)
    sales_cat = (
        fact_sales_line.select("store_sk", "product_sk", "date_sk", "gross_sales_amount", "net_sales_amount", "cogs_amount")
        .join(periods.select("date_sk", "pidx"), "date_sk")
        .join(cat.select("product_sk", "category_id"), "product_sk")
        .groupBy("store_sk", "category_id", "pidx")
        .agg(
            F.sum("gross_sales_amount").alias("cat_gross"),
            F.sum("net_sales_amount").alias("cat_net"),
            F.sum("cogs_amount").alias("cat_cogs"),
        )
    )

    out = (
        base.join(sales_cat, ["store_sk", "category_id", "pidx"], "left")
        .fillna(0.0, ["cat_gross", "cat_net", "cat_cogs"])
        .withColumn("cogs_amount", F.round("cat_cogs", 2))
        .withColumn("markdown_amount", F.round(F.lit(markdown_rate) * F.col("cat_gross"), 2))
        .withColumn("shrink_amount", F.round(F.lit(0.005) * F.col("on_hand_cost_value"), 2))
        .withColumn(
            "gmroi",
            F.round((F.col("cat_net") - F.col("cat_cogs")) / F.greatest(F.col("on_hand_cost_value"), F.lit(1.0)), 4),
        )
        .withColumn("on_hand_cost_value", F.round("on_hand_cost_value", 2))
        .withColumn("on_hand_retail_value", F.round("on_hand_retail_value", 2))
    )
    return FACT_INVENTORY_VALUATION_SPEC.select_ordered(out)
