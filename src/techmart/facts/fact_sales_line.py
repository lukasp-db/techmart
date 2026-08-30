from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import FactColumn, FactSpec

FACT_SALES_LINE_SPEC = FactSpec(
    schema="core",
    name="fact_sales_line",
    grain="one row per sales transaction line",
    columns=[
        FactColumn("transaction_id", "long", "Degenerate transaction id", nullable=False),
        FactColumn("line_number", "int", "Line number within the transaction", nullable=False),
        FactColumn("receipt_id", "string", "Degenerate receipt id"),
        FactColumn("date_sk", "long", "Date FK (dim_date, yyyymmdd)", is_key=True, nullable=False),
        FactColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        FactColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        FactColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        FactColumn("employee_sk", "long", "Selling associate FK (dim_employee)", is_key=True, nullable=False),
        FactColumn("promotion_sk", "long", "Promotion FK (dim_promotion); null if unpromoted", is_key=True),
        FactColumn("channel_sk", "long", "Channel FK (dim_channel)", is_key=True, nullable=False),
        FactColumn("quantity", "int", "Units sold on the line", nullable=False),
        FactColumn("unit_price", "double", "Selling price per unit"),
        FactColumn("unit_cost", "double", "Standard cost per unit"),
        FactColumn("gross_sales_amount", "double", "quantity * unit_price"),
        FactColumn("discount_amount", "double", "Promotional discount applied"),
        FactColumn("net_sales_amount", "double", "gross_sales_amount - discount_amount"),
        FactColumn("tax_amount", "double", "Sales tax on net sales"),
        FactColumn("cogs_amount", "double", "quantity * unit_cost"),
        FactColumn("gross_margin_amount", "double", "net_sales_amount - cogs_amount"),
        FactColumn("loyalty_points_earned", "long", "Loyalty points earned (floor of net sales)"),
        FactColumn("is_return", "boolean", "Always false in the sales fact", nullable=False),
        FactColumn("is_marketplace", "boolean", "Sold via the marketplace channel", nullable=False),
        FactColumn("tender_type", "string", "Payment tender type"),
    ],
)

# dim_channel surrogate order: In-Store, Web, Mobile-App, Marketplace, Call-Center.
_CHANNEL_SKS = [1, 2, 3, 4, 5]
_CHANNEL_WEIGHTS = [50, 28, 15, 5, 2]
_TENDERS = ["Card", "Card", "Card", "Cash", "Gift Card", "Mobile Pay"]

_DISCOUNT_RATE = 0.12
_TAX_RATE = 0.07


def build_fact_sales_line(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    product_econ: DataFrame,
    date_weights: tuple[list[int], list[int]],
    rows: int | None = None,
    seed: int | None = None,
    promo_fraction: float = 0.22,
) -> DataFrame:
    sp = config.scale_profile
    rows = rows if rows is not None else sp.sales_lines_target
    seed = seed if seed is not None else config.seed
    date_sks, weights = date_weights
    partitions = max(1, min(256, rows // 1_000_000))

    gen = (
        dg.DataGenerator(
            spark,
            name="fact_sales_line",
            rows=rows,
            partitions=partitions,
            randomSeed=seed,
            randomSeedMethod="fixed",
        )
        .withColumn("transaction_id", "long", minValue=1, maxValue=max(rows // 3, 1), random=True)
        .withColumn("line_number", "int", minValue=1, maxValue=8, random=True)
        .withColumn("date_sk", "long", values=date_sks, weights=weights, random=True)
        .withColumn(
            "product_sk", "long",
            minValue=1, maxValue=sp.num_skus,
            distribution=dg.distributions.Gamma(1.0, 2.0), random=True,
        )
        .withColumn("store_sk", "long", minValue=1, maxValue=sp.num_stores, random=True)
        .withColumn("customer_sk", "long", minValue=1, maxValue=sp.num_customers, random=True)
        .withColumn("employee_sk", "long", minValue=1, maxValue=sp.num_employees, random=True)
        .withColumn("channel_sk", "long", values=_CHANNEL_SKS, weights=_CHANNEL_WEIGHTS, random=True)
        .withColumn(
            "promotion_sk", "long",
            minValue=1, maxValue=sp.num_promotions,
            random=True, percentNulls=1.0 - promo_fraction,
        )
        .withColumn("quantity", "int", minValue=1, maxValue=5, random=True)
        .withColumn("tender_type", "string", values=_TENDERS, random=True)
    )
    base = gen.build()

    econ = product_econ.select(
        F.col("product_sk").alias("_econ_sk"),
        F.col("list_price"),
        F.col("standard_cost"),
    )
    joined = base.join(econ, base["product_sk"] == econ["_econ_sk"], "left").drop("_econ_sk")

    df = (
        joined
        .withColumn("unit_price", F.round(F.col("list_price"), 2))
        .withColumn("unit_cost", F.round(F.col("standard_cost"), 2))
        .withColumn("receipt_id", F.concat(F.lit("RCPT-"), F.col("transaction_id").cast("string")))
        .withColumn("gross_sales_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn(
            "discount_amount",
            F.when(
                F.col("promotion_sk").isNotNull(),
                F.round(F.col("gross_sales_amount") * F.lit(_DISCOUNT_RATE), 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("net_sales_amount", F.round(F.col("gross_sales_amount") - F.col("discount_amount"), 2))
        .withColumn("tax_amount", F.round(F.col("net_sales_amount") * F.lit(_TAX_RATE), 2))
        .withColumn("cogs_amount", F.round(F.col("quantity") * F.col("unit_cost"), 2))
        .withColumn("gross_margin_amount", F.round(F.col("net_sales_amount") - F.col("cogs_amount"), 2))
        .withColumn("loyalty_points_earned", F.floor(F.col("net_sales_amount")).cast("long"))
        .withColumn("is_return", F.lit(False))
        .withColumn("is_marketplace", F.col("channel_sk") == F.lit(4))
    )
    return FACT_SALES_LINE_SPEC.select_ordered(df)
