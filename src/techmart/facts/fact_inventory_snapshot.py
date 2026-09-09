"""fact_inventory_snapshot: store x SKU x fiscal-period-end stock position.

Periodic snapshot (one row per store, carried SKU, and fiscal period-end),
calibrated to sales velocity. Replaces the former daily full-grid snapshot.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..finance.periods import date_periods, period_end_lookup
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import uniform_hash
from .lookups import product_economics

FACT_INVENTORY_SNAPSHOT_SPEC = SparkTableSpec(
    schema="core",
    name="fact_inventory_snapshot",
    grain="one row per store, SKU, and fiscal period-end (stock position)",
    columns=[
        SparkColumn("date_sk", "long", "Snapshot date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("on_hand_qty", "int", "Units physically on hand", nullable=False),
        SparkColumn("on_order_qty", "int", "Units on open purchase orders"),
        SparkColumn("in_transit_qty", "int", "Units in transit to the store"),
        SparkColumn("reserved_qty", "int", "Units reserved for orders"),
        SparkColumn("available_qty", "int", "On hand minus reserved", nullable=False),
        SparkColumn("safety_stock_qty", "int", "Safety-stock threshold"),
        SparkColumn("reorder_point", "int", "Reorder-point threshold"),
        SparkColumn("unit_cost", "double", "Standard cost per unit"),
        SparkColumn("on_hand_retail_value", "double", "on_hand_qty * list_price"),
        SparkColumn("on_hand_cost_value", "double", "on_hand_qty * unit_cost"),
        SparkColumn("days_of_supply", "double", "On hand divided by average daily demand"),
        SparkColumn("is_out_of_stock", "boolean", "True when on_hand_qty is zero", nullable=False),
    ],
)

_MIN_DAILY = 0.05          # slow-mover demand floor (units/day)
_OOS_RATE = 0.04           # fraction of snapshot cells forced out of stock
_SQFT_LO, _SQFT_HI = 15000.0, 45000.0   # dim_store.square_footage range


def build_fact_inventory_snapshot(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    dim_store: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
    fact_sales_line: DataFrame,
) -> DataFrame:
    """Periodic (fiscal-period-end) snapshot over the full history.

    Each store carries a deterministic SKU assortment (weighted by product
    popularity and store size); on-hand is calibrated to per-(store, product)
    sales velocity times a per-period seasonal index, with an explicit small
    out-of-stock draw. Grain: (date_sk, store_sk, product_sk).
    """
    sp = config.scale_profile
    assortment_rate = float(sp.assortment_rate)
    target_wos_weeks = float(sp.target_wos_weeks)
    n_days = float(dim_date.count())  # contiguous daily history
    num_products = float(dim_product.count())

    # --- period-end snapshot dates (~history_years * 12) with pidx ---
    pe = period_end_lookup(dim_date).select(
        F.col("period_end_date_sk").alias("date_sk"), "pidx",
    )

    # --- per-period seasonal demand index (normalized to mean 1.0) ---
    periods = date_periods(dim_date).select("date_sk", "pidx")
    period_units = (
        fact_sales_line.select("date_sk", "quantity")
        .join(periods, "date_sk")
        .groupBy("pidx").agg(F.sum("quantity").alias("period_units"))
    )
    avg_period_units = period_units.agg(F.avg("period_units")).first()[0] or 1.0
    seasonal = period_units.withColumn(
        "seasonal_factor",
        F.greatest(F.col("period_units") / F.lit(float(avg_period_units)), F.lit(0.1)),
    ).select("pidx", "seasonal_factor")

    # --- per-(store, product) sales velocity ---
    velocity = (
        fact_sales_line.groupBy("store_sk", "product_sk")
        .agg(F.sum("quantity").alias("_total_units"))
    )

    # --- stores (breadth from square footage) & product economics + popularity ---
    stores = dim_store.select(
        "store_sk",
        (F.lit(0.5) + (F.col("square_footage").cast("double") - F.lit(_SQFT_LO))
         / F.lit(_SQFT_HI - _SQFT_LO)).alias("_store_factor"),
    )
    prods = product_economics(dim_product).select(
        "product_sk",
        F.round(F.col("list_price"), 2).alias("list_price"),
        F.round(F.col("standard_cost"), 2).alias("unit_cost"),
        # assortment breadth: low product_sk carried more widely (breadth heuristic, not sales-velocity — velocity is joined separately below)
        (F.lit(1.0) - (F.col("product_sk").cast("double") - F.lit(1.0)) / F.lit(num_products)).alias("_pop"),
    )

    # --- assortment: deterministic carried (store, product) pairs ---
    assort = (
        stores.crossJoin(prods)
        .withColumn(
            "_carry_prob",
            F.least(
                F.lit(1.0),
                F.lit(2.0 * assortment_rate) * F.col("_pop") * F.col("_store_factor"),
            ),
        )
        .filter(uniform_hash(F.col("store_sk"), F.col("product_sk"), salt="assort") < F.col("_carry_prob"))
    )

    # --- attach per-pair velocity BEFORE the period-end cross join (avg_daily is
    #     date-independent, so this keeps the velocity shuffle on the ~assortment
    #     rows instead of the full period-end grid) ---
    assort = (
        assort.join(velocity, ["store_sk", "product_sk"], "left")
        .withColumn("_total_units", F.coalesce(F.col("_total_units"), F.lit(0)))
        .withColumn("_avg_daily", F.greatest(F.col("_total_units") / F.lit(n_days), F.lit(_MIN_DAILY)))
    )

    # --- cross with period-ends; attach the per-period seasonal factor ---
    grid = (
        assort.crossJoin(pe)
        .join(seasonal, "pidx", "left")
        .withColumn("seasonal_factor", F.coalesce(F.col("seasonal_factor"), F.lit(1.0)))
    )

    def cell(salt: str):
        return uniform_hash(F.col("store_sk"), F.col("product_sk"), F.col("date_sk"), salt=salt)

    df = (
        grid
        .withColumn("_noise", F.lit(0.85) + cell("oh") * F.lit(0.30))
        .withColumn("_oos", cell("oos") < F.lit(_OOS_RATE))
        .withColumn(
            "on_hand_qty",
            F.when(F.col("_oos"), F.lit(0)).otherwise(
                F.greatest(
                    F.round(
                        F.lit(target_wos_weeks) * F.lit(7.0) * F.col("_avg_daily")
                        * F.col("seasonal_factor") * F.col("_noise")
                    ).cast("int"),
                    F.lit(0),
                )
            ),
        )
        .withColumn("reserved_qty", F.floor(cell("rs") * F.lit(5)).cast("int"))
        .withColumn("available_qty", F.greatest(F.col("on_hand_qty") - F.col("reserved_qty"), F.lit(0)))
        .withColumn("safety_stock_qty", F.ceil(F.col("_avg_daily") * F.lit(3.0)).cast("int"))
        .withColumn("reorder_point", F.ceil(F.col("_avg_daily") * F.lit(7.0)).cast("int"))
        .withColumn(
            "on_order_qty",
            F.when(F.col("on_hand_qty") < F.col("reorder_point"),
                   F.ceil(F.col("_avg_daily") * F.lit(14.0)).cast("int")).otherwise(F.lit(0)),
        )
        .withColumn(
            "in_transit_qty",
            F.when(F.col("on_order_qty") > F.lit(0),
                   F.floor(cell("it") * F.col("on_order_qty")).cast("int")).otherwise(F.lit(0)),
        )
        .withColumn("on_hand_retail_value", F.round(F.col("on_hand_qty") * F.col("list_price"), 2))
        .withColumn("on_hand_cost_value", F.round(F.col("on_hand_qty") * F.col("unit_cost"), 2))
        .withColumn("days_of_supply", F.round(F.col("on_hand_qty") / F.col("_avg_daily"), 1))
        .withColumn("is_out_of_stock", F.col("on_hand_qty") == F.lit(0))
    )
    return FACT_INVENTORY_SNAPSHOT_SPEC.select_ordered(df)
