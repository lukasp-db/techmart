from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import uniform_hash
from .lookups import date_seasonality_weights, product_economics

FACT_SALES_LINE_SPEC = SparkTableSpec(
    schema="core",
    name="fact_sales_line",
    grain="one row per sales transaction line",
    columns=[
        SparkColumn("transaction_id", "long", "Degenerate transaction id", nullable=False),
        SparkColumn("line_number", "int", "Line number within the transaction", nullable=False),
        SparkColumn("receipt_id", "string", "Degenerate receipt id"),
        SparkColumn("date_sk", "long", "Date FK (dim_date, yyyymmdd)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("employee_sk", "long", "Selling associate FK (dim_employee)", is_key=True, nullable=False),
        SparkColumn("promotion_sk", "long", "Promotion FK (dim_promotion); null if unpromoted", is_key=True),
        SparkColumn("channel_sk", "long", "Channel FK (dim_channel)", is_key=True, nullable=False),
        SparkColumn("quantity", "int", "Units sold on the line", nullable=False),
        SparkColumn("unit_price", "double", "Selling price per unit"),
        SparkColumn("unit_cost", "double", "Standard cost per unit"),
        SparkColumn("gross_sales_amount", "double", "quantity * unit_price"),
        SparkColumn("discount_amount", "double", "Promotional discount applied"),
        SparkColumn("net_sales_amount", "double", "gross_sales_amount - discount_amount"),
        SparkColumn("tax_amount", "double", "Sales tax on net sales"),
        SparkColumn("cogs_amount", "double", "quantity * unit_cost"),
        SparkColumn("gross_margin_amount", "double", "net_sales_amount - cogs_amount"),
        SparkColumn("loyalty_points_earned", "long", "Loyalty points earned (floor of net sales)"),
        SparkColumn("is_return", "boolean", "Always false in the sales fact", nullable=False),
        SparkColumn("is_marketplace", "boolean", "Sold via the marketplace channel", nullable=False),
        SparkColumn("tender_type", "string", "Payment tender type"),
    ],
)

_AVG_BASKET = 2.97  # weighted mean of the basket-size distribution below
_DISCOUNT_RATE = 0.12
_TAX_RATE = 0.07


def build_fact_sales_line(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    dim_product: DataFrame,
    dim_date: DataFrame,
    dim_counts: dict,
    rows: int | None = None,
    seed: int | None = None,
) -> DataFrame:
    """Build fact_sales_line via basket-coherent transaction header → explode.

    FK ranges are derived from *actual* dimension counts (``dim_counts``),
    guaranteeing referential integrity by construction.
    """
    sp = config.scale_profile
    target_lines = rows if rows is not None else sp.sales_lines_target
    seed = seed if seed is not None else config.seed

    num_transactions = max(1, round(target_lines / _AVG_BASKET))
    partitions = max(1, min(256, num_transactions // 1_000_000))

    date_sks, weights = date_seasonality_weights(dim_date)

    # --- Build transaction header (one row per receipt) ---
    gen = (
        dg.DataGenerator(
            spark,
            name="txn_header",
            rows=num_transactions,
            partitions=partitions,
            randomSeed=seed,
            randomSeedMethod="fixed",
        )
        .withIdOutput()
        .withColumn("date_sk", "long", values=date_sks, weights=weights, random=True)
        .withColumn("store_sk", "long", minValue=1, maxValue=dim_counts["store"], random=True)
        .withColumn("customer_sk", "long", minValue=1, maxValue=dim_counts["customer"], random=True)
        .withColumn("employee_sk", "long", minValue=1, maxValue=dim_counts["employee"], random=True)
        # channel_sk order is fixed by dim_channel: 1=In-Store, 2=Web, 3=Mobile-App,
        # 4=Marketplace, 5=Call-Center. is_marketplace (below) keys on channel_sk == 4.
        .withColumn(
            "channel_sk", "long",
            values=[1, 2, 3, 4, 5], weights=[50, 28, 15, 5, 2], random=True,
        )
        .withColumn(
            "basket_size", "int",
            values=[1, 2, 3, 4, 5, 6, 7, 8], weights=[25, 25, 18, 12, 8, 6, 4, 2], random=True,
        )
    )
    header = gen.build()
    # Derive transaction_id from the dbldatagen row id; drop raw id.
    header = (
        header
        .withColumn("transaction_id", (F.col("id") + 1).cast("long"))
        .drop("id")
    )

    # --- Explode header into line items ---
    lines = (
        header
        .withColumn("line_number", F.explode(F.sequence(F.lit(1), F.col("basket_size"))))
        .drop("basket_size")
    )

    # --- Deterministic per-line attributes via Spark hash (partition-independent) ---
    num_products = dim_counts["product"]
    num_promotions = dim_counts["promotion"]

    def _u(salt: str) -> "Column":  # noqa: F821
        """Uniform pseudo-random double in [0, 1) keyed on (txn, line, salt)."""
        return uniform_hash(F.col("transaction_id"), F.col("line_number"), salt=salt)

    lines = (
        lines
        # Long-tail product distribution — pow(u,3) biases toward lower skus.
        .withColumn(
            "product_sk",
            (F.floor(F.pow(_u("p"), 3.0) * F.lit(num_products)) + 1).cast("long"),
        )
        .withColumn(
            "quantity",
            (F.pmod(F.hash(F.col("transaction_id"), F.col("line_number"), F.lit("q")), 5) + 1).cast("int"),
        )
        .withColumn(
            "promotion_sk",
            F.when(
                _u("pr") < 0.22,
                (
                    F.pmod(
                        F.hash(F.col("transaction_id"), F.col("line_number"), F.lit("ps")),
                        F.lit(num_promotions),
                    )
                    + 1
                ).cast("long"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "tender_type",
            F.element_at(
                F.array(
                    F.lit("Card"), F.lit("Card"), F.lit("Card"),
                    F.lit("Cash"), F.lit("Gift Card"), F.lit("Mobile Pay"),
                ),
                (F.pmod(F.hash(F.col("transaction_id"), F.col("line_number"), F.lit("t")), 6) + 1),
            ),
        )
    )

    # --- Join product economics (list_price / standard_cost) ---
    econ = product_economics(dim_product).select(
        F.col("product_sk").alias("_econ_sk"),
        F.col("list_price"),
        F.col("standard_cost"),
    )
    joined = lines.join(econ, lines["product_sk"] == econ["_econ_sk"], "left").drop("_econ_sk")

    # --- Derive measure chain ---
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
        .drop("list_price", "standard_cost")
    )

    return FACT_SALES_LINE_SPEC.select_ordered(df)
