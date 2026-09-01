"""fact_returns: return lines derived from real fact_sales_line receipts."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import bounded_int, shifted_date_sk, uniform_hash

FACT_RETURNS_SPEC = SparkTableSpec(
    schema="core",
    name="fact_returns",
    grain="one row per returned sales line",
    columns=[
        SparkColumn("rma_id", "string", "Degenerate return-merchandise-authorization id", nullable=False),
        SparkColumn("original_transaction_id", "long", "Originating fact_sales_line transaction id", nullable=False),
        SparkColumn("original_line_number", "int", "Originating sales line number", nullable=False),
        SparkColumn("date_sk", "long", "Return date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("employee_sk", "long", "Processing associate FK (dim_employee)", is_key=True, nullable=False),
        SparkColumn("channel_sk", "long", "Channel FK (dim_channel)", is_key=True, nullable=False),
        SparkColumn("quantity", "int", "Units returned", nullable=False),
        SparkColumn("return_reason", "string", "Reason for the return"),
        SparkColumn("disposition", "string", "Restock/Liquidate/RTV/Scrap"),
        SparkColumn("refund_amount", "double", "Amount refunded"),
        SparkColumn("restocking_fee", "double", "Restocking fee charged"),
        SparkColumn("is_fraud_suspected", "boolean", "Flagged for possible return fraud", nullable=False),
    ],
)

_REASONS = ["Defective", "Changed-Mind", "Wrong-Item", "Damaged-in-Shipping", "No-Longer-Needed"]
_DISPOSITIONS = ["Restock", "Liquidate", "RTV", "Scrap"]


def build_fact_returns(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_date: DataFrame,
    return_rate: float = 0.08,
) -> DataFrame:
    max_date = dim_date.agg(F.max("date")).first()[0]

    sales = fact_sales_line.join(dim_date.select("date_sk", "date"), "date_sk")
    keyed = (F.col("transaction_id"), F.col("line_number"))

    returned = sales.filter(uniform_hash(*keyed, salt="ret") < F.lit(return_rate))

    reasons_arr = F.array(*[F.lit(r) for r in _REASONS])
    disp_arr = F.array(*[F.lit(d) for d in _DISPOSITIONS])

    # FKs product/store/customer/employee/channel are inherited from the sale row
    # for coherence; date_sk is overwritten below to the (later) return date.
    # refund = per-unit net of the original line (net_sales_amount / original qty)
    # times the returned quantity; _orig_qty is captured before quantity is overwritten.
    df = (
        returned
        .withColumn("original_transaction_id", F.col("transaction_id"))
        .withColumn("original_line_number", F.col("line_number"))
        .withColumn(
            "rma_id",
            F.concat(F.lit("RMA-"), F.col("transaction_id").cast("string"), F.lit("-"), F.col("line_number").cast("string")),
        )
        .withColumn("_orig_qty", F.col("quantity"))
        .withColumn("_unit_net", F.col("net_sales_amount") / F.col("_orig_qty"))
        .withColumn("_lag", bounded_int(*keyed, salt="lag", lo=1, hi=30))
        .withColumn("date_sk", shifted_date_sk(F.col("date"), F.col("_lag"), max_date))
        .withColumn("quantity", (F.pmod(F.hash(*keyed, F.lit("rq")), F.col("_orig_qty")) + F.lit(1)).cast("int"))
        .withColumn("return_reason", F.element_at(reasons_arr, bounded_int(*keyed, salt="rr", lo=1, hi=len(_REASONS))))
        .withColumn("disposition", F.element_at(disp_arr, bounded_int(*keyed, salt="dp", lo=1, hi=len(_DISPOSITIONS))))
        .withColumn("refund_amount", F.round(F.col("_unit_net") * F.col("quantity"), 2))
        .withColumn(
            "restocking_fee",
            F.when(F.col("disposition") == F.lit("Restock"), F.round(F.col("refund_amount") * F.lit(0.10), 2)).otherwise(F.lit(0.0)),
        )
        .withColumn("is_fraud_suspected", uniform_hash(*keyed, salt="fraud") < F.lit(0.02))
        .drop("_orig_qty", "_unit_net", "_lag")
    )

    return FACT_RETURNS_SPEC.select_ordered(df)
