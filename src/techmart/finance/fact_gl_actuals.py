"""fact_gl_actuals: GL actuals derived from real core facts + injected deltas."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..facts.gen import uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec
from .periods import date_periods, period_end_lookup

_OPEX_SALT = 730_001

FACT_GL_ACTUALS_SPEC = SparkTableSpec(
    schema="finance",
    name="fact_gl_actuals",
    grain="one row per GL account × store × department × fiscal period",
    columns=[
        SparkColumn("date_sk", "long", "Period-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("gl_account_sk", "long", "GL account FK (dim_gl_account)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store/cost-center FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("department_sk", "long", "Department FK (dim_department)", is_key=True, nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("actual_amount", "double", "Actual amount (contra revenue negative)"),
        SparkColumn("currency", "string", "ISO currency code"),
    ],
)


def build_fact_gl_actuals(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    fact_returns: DataFrame,
    fact_inventory_movement: DataFrame,
    dim_date: DataFrame,
    dim_gl_account: DataFrame,
    dim_department: DataFrame,
) -> DataFrame:
    sp = config.scale_profile
    periods = date_periods(dim_date)
    pe = period_end_lookup(dim_date)  # fiscal_year, fiscal_period, pidx, period_end_date_sk, period_max_week

    # --- Group A: gross + cogs by (store, is_online, pidx), with timing shift on gross ---
    s = (
        fact_sales_line.select(
            "store_sk", "channel_sk", "date_sk", "gross_sales_amount", "cogs_amount"
        )
        .join(periods, "date_sk")
        .withColumn("is_online", F.col("channel_sk").isin(2, 3, 4))
    )
    a = s.groupBy("store_sk", "is_online", "pidx").agg(
        F.sum("gross_sales_amount").alias("gross"),
        F.sum("cogs_amount").alias("cogs"),
    )
    # last-week gross per (store, is_online, pidx)
    lw = (
        s.join(pe.select("pidx", "period_max_week"), "pidx")
        .filter(F.col("fiscal_week") == F.col("period_max_week"))
        .groupBy("store_sk", "is_online", "pidx")
        .agg(F.sum("gross_sales_amount").alias("last_week_gross"))
    )
    a = a.join(lw, ["store_sk", "is_online", "pidx"], "left").fillna(0.0, ["last_week_gross"])
    # Use LOCAL max_pidx per (store, is_online) group so that every shift_out has a
    # corresponding successor row and the telescoping sum conserves total gross exactly.
    local_max = a.groupBy("store_sk", "is_online").agg(F.max("pidx").alias("local_max_pidx"))
    a = a.join(local_max, ["store_sk", "is_online"])
    a = a.withColumn(
        "shift_out",
        F.when(F.col("pidx") < F.col("local_max_pidx"), F.lit(sp.timing_shift_pct) * F.col("last_week_gross")).otherwise(
            # Gross conservation (Σ recognized_gross == Σ gross) holds iff pidx is gap-free within each (store, is_online) group — verified true at showcase density.
            F.lit(0.0)
        ),
    )
    shift_in = a.select(
        "store_sk", "is_online", (F.col("pidx") + F.lit(1)).alias("pidx"),
        F.col("shift_out").alias("shift_in"),
    )
    a = a.join(shift_in, ["store_sk", "is_online", "pidx"], "left").fillna(0.0, ["shift_in"])
    a = a.withColumn("recognized_gross", F.col("gross") - F.col("shift_out") + F.col("shift_in"))
    a = a.withColumn(
        "dept_name", F.when(F.col("is_online"), F.lit("E-commerce")).otherwise(F.lit("Merchandising"))
    )

    rev_lines = a.select(
        "store_sk", "pidx", F.col("dept_name"),
        F.explode(
            F.array(
                F.struct(F.lit("4000").alias("acct"), F.col("recognized_gross").alias("amt")),
                F.struct(F.lit("5000").alias("acct"), F.col("cogs").alias("amt")),
            )
        ).alias("line"),
    ).select("store_sk", "pidx", "dept_name", F.col("line.acct").alias("account_number"), F.col("line.amt").alias("actual_amount"))

    # --- Group B: store-level lines, grain (store, pidx) ---
    store_g = a.groupBy("store_sk", "pidx").agg(F.sum("recognized_gross").alias("g"))

    ret = (
        fact_returns.select("store_sk", "date_sk", "refund_amount")
        .join(periods.select("date_sk", "pidx"), "date_sk")
        .groupBy("store_sk", "pidx").agg(F.sum("refund_amount").alias("returns_amt"))
    )
    shrink = (
        fact_inventory_movement.filter(F.col("movement_type") == "Shrink")
        .select("store_sk", "date_sk", "quantity", "unit_cost")
        .join(periods.select("date_sk", "pidx"), "date_sk")
        .groupBy("store_sk", "pidx")
        .agg(F.sum(F.abs(F.col("quantity")) * F.col("unit_cost")).alias("shrink_cost"))
    )
    # Union all source (store, pidx) pairs so that returns/shrink rows for periods
    # with no corresponding sales are NOT silently dropped by a left-join from store_g.
    all_store_pidx = (
        store_g.select("store_sk", "pidx")
        .unionByName(ret.select("store_sk", "pidx"))
        .unionByName(shrink.select("store_sk", "pidx"))
        .distinct()
    )
    b = (
        all_store_pidx
        .join(store_g, ["store_sk", "pidx"], "left")
        .join(ret, ["store_sk", "pidx"], "left")
        .join(shrink, ["store_sk", "pidx"], "left")
        .fillna(0.0, ["g", "returns_amt", "shrink_cost"])
    )

    def j(acct: str) -> "F.Column":
        return F.lit(0.95) + F.lit(0.10) * uniform_hash("store_sk", F.lit(acct), salt=_OPEX_SALT)

    store_lines = b.select(
        "store_sk", "pidx",
        F.explode(
            F.array(
                F.struct(F.lit("4100").alias("acct"), (-F.col("returns_amt")).alias("amt"), F.lit("Merchandising").alias("dep")),
                F.struct(F.lit("4200").alias("acct"), (-F.lit(sp.allowance_rate) * F.col("g")).alias("amt"), F.lit("Merchandising").alias("dep")),
                F.struct(F.lit("5200").alias("acct"), (F.lit(sp.markdown_rate) * F.col("g")).alias("amt"), F.lit("Merchandising").alias("dep")),
                F.struct(F.lit("5300").alias("acct"), F.col("shrink_cost").alias("amt"), F.lit("Supply Chain").alias("dep")),
                F.struct(F.lit("6000").alias("acct"), ((F.lit(8000.0) + F.lit(0.11) * F.col("g")) * j("6000")).alias("amt"), F.lit("Store Operations").alias("dep")),
                F.struct(F.lit("6100").alias("acct"), (F.lit(6000.0) * j("6100")).alias("amt"), F.lit("Store Operations").alias("dep")),
                F.struct(F.lit("6200").alias("acct"), (F.lit(0.04) * F.col("g") * j("6200")).alias("amt"), F.lit("Marketing").alias("dep")),
                F.struct(F.lit("6300").alias("acct"), (F.lit(0.03) * F.col("g") * j("6300")).alias("amt"), F.lit("Supply Chain").alias("dep")),
                F.struct(F.lit("6400").alias("acct"), ((F.lit(4000.0) + F.lit(0.02) * F.col("g")) * j("6400")).alias("amt"), F.lit("G&A").alias("dep")),
                F.struct(F.lit("6500").alias("acct"), (F.lit(3500.0) * j("6500")).alias("amt"), F.lit("G&A").alias("dep")),
            )
        ).alias("line"),
    ).select(
        "store_sk", "pidx",
        F.col("line.dep").alias("dept_name"),
        F.col("line.acct").alias("account_number"),
        F.col("line.amt").alias("actual_amount"),
    )

    lines = rev_lines.select("store_sk", "pidx", "dept_name", "account_number", "actual_amount").unionByName(
        store_lines.select("store_sk", "pidx", "dept_name", "account_number", "actual_amount")
    )

    out = (
        lines.join(dim_gl_account.select("account_number", "gl_account_sk"), "account_number")
        .join(dim_department.select(F.col("department_name").alias("dept_name"), "department_sk"), "dept_name")
        .join(pe.select("pidx", "period_end_date_sk", "fiscal_year", "fiscal_period"), "pidx")
        .withColumn("date_sk", F.col("period_end_date_sk"))
        .withColumn("actual_amount", F.round("actual_amount", 2))
        .withColumn("currency", F.lit("USD"))
    )
    return FACT_GL_ACTUALS_SPEC.select_ordered(out)
