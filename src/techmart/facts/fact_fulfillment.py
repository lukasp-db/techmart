"""fact_fulfillment: online fulfillment lines derived from real sales."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import bounded_int, shifted_date_sk, uniform_hash

FACT_FULFILLMENT_SPEC = SparkTableSpec(
    schema="core",
    name="fact_fulfillment",
    grain="one row per online fulfillment line",
    columns=[
        SparkColumn("order_id", "string", "Degenerate fulfillment order id", nullable=False),
        SparkColumn("date_sk", "long", "Order date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Fulfilling-node store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("channel_sk", "long", "Channel FK (dim_channel)", is_key=True, nullable=False),
        SparkColumn("fulfillment_type", "string", "BOPIS/Ship-from-Store/DC-Delivery/Curbside", nullable=False),
        SparkColumn("quantity", "int", "Units fulfilled", nullable=False),
        SparkColumn("promised_date_sk", "long", "Promised delivery date FK (dim_date)"),
        SparkColumn("actual_ship_date_sk", "long", "Actual ship date FK (dim_date)"),
        SparkColumn("delivery_date_sk", "long", "Delivery date FK (dim_date)"),
        SparkColumn("sla_met_flag", "boolean", "Delivered on or before promised date", nullable=False),
        SparkColumn("shipping_cost", "double", "Shipping cost charged"),
    ],
)

_TYPES = ["BOPIS", "Ship-from-Store", "DC-Delivery", "Curbside"]


def build_fact_fulfillment(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    max_date = dim_date.agg(F.max("date")).first()[0]
    keyed = (F.col("transaction_id"), F.col("line_number"))
    types_arr = F.array(*[F.lit(t) for t in _TYPES])

    online = (
        fact_sales_line.filter(F.col("channel_sk").isin(2, 3, 4))
        .join(dim_date.select("date_sk", "date"), "date_sk")
    )

    df = (
        online
        .withColumn(
            "order_id",
            F.concat(F.lit("ORD-"), F.col("transaction_id").cast("string"), F.lit("-"), F.col("line_number").cast("string")),
        )
        .withColumn("fulfillment_type", F.element_at(types_arr, bounded_int(*keyed, salt="ft", lo=1, hi=len(_TYPES))))
        .withColumn("_promise_lag", bounded_int(*keyed, salt="pl", lo=2, hi=7))
        .withColumn("promised_date_sk", shifted_date_sk(F.col("date"), F.col("_promise_lag"), max_date))
        .withColumn("_ship_lag", bounded_int(*keyed, salt="sl", lo=0, hi=3))
        .withColumn("actual_ship_date_sk", shifted_date_sk(F.col("date"), F.col("_ship_lag"), max_date))
        .withColumn("_deliver_lag", F.col("_ship_lag") + bounded_int(*keyed, salt="dl", lo=1, hi=6))
        .withColumn("delivery_date_sk", shifted_date_sk(F.col("date"), F.col("_deliver_lag"), max_date))
        .withColumn("sla_met_flag", F.col("delivery_date_sk") <= F.col("promised_date_sk"))
        .withColumn(
            "shipping_cost",
            F.when(
                F.col("fulfillment_type").isin("BOPIS", "Curbside"), F.lit(0.0),
            ).otherwise(F.round(F.lit(4.99) + uniform_hash(*keyed, salt="sc") * F.lit(10.0), 2)),
        )
        .drop("_promise_lag", "_ship_lag", "_deliver_lag")
    )
    return FACT_FULFILLMENT_SPEC.select_ordered(df)
