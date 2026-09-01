from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_returns import build_fact_returns
from techmart.facts.fact_inventory_movement import build_fact_inventory_movement
from techmart.finance.dim_department import build_dim_department
from techmart.finance.dim_gl_account import build_dim_gl_account
from techmart.finance.fact_gl_actuals import build_fact_gl_actuals
from techmart.finance.fact_budget_plan import FACT_BUDGET_PLAN_SPEC, build_fact_budget_plan

_P = ScaleProfile("t", 8, 40, 1, 4000, 300, 20)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 8, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 40}


def _actuals(spark):
    dd = build_dim_date(spark, _CFG); dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=4000)
    returns = build_fact_returns(spark, _CFG, fact_sales_line=sales, dim_date=dd)
    mov = build_fact_inventory_movement(spark, _CFG, dim_date=dd, dim_product=dp, dim_counts=_COUNTS)
    return build_fact_gl_actuals(spark, _CFG, fact_sales_line=sales, fact_returns=returns,
                                 fact_inventory_movement=mov, dim_date=dd,
                                 dim_gl_account=build_dim_gl_account(spark, _CFG),
                                 dim_department=build_dim_department(spark, _CFG))


def _build(spark):
    return build_fact_budget_plan(spark, _CFG, fact_gl_actuals=_actuals(spark),
                                  dim_gl_account=build_dim_gl_account(spark, _CFG))


def test_schema_and_three_versions(spark):
    df = _build(spark)
    assert df.columns == FACT_BUDGET_PLAN_SPEC.column_names
    assert {r["plan_version"] for r in df.select("plan_version").distinct().collect()} == \
        {"Budget", "Forecast", "Latest-Estimate"}


def test_pl_only(spark):
    df = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    bs_sks = [a["gl_account_sk"] for a in gl.collect() if a["statement"] == "Balance-Sheet"]
    assert df.filter(F.col("gl_account_sk").isin(bs_sks)).count() == 0


def test_attainment_within_variance(spark):
    df = _build(spark); actuals = _actuals(spark)
    keys = ["date_sk", "gl_account_sk", "store_sk", "department_sk"]
    budget = df.filter(F.col("plan_version") == "Budget").select(*keys, F.col("plan_amount"))
    joined = actuals.select(*keys, "actual_amount").join(budget, keys).filter(F.abs("actual_amount") > 1.0)
    bad = joined.filter(
        F.abs(F.col("plan_amount") - F.col("actual_amount")) > (_P.budget_variance + 0.001) * F.abs("actual_amount")
    ).count()
    assert bad == 0


def test_grain_unique(spark):
    df = _build(spark)
    g = ["date_sk", "gl_account_sk", "store_sk", "department_sk", "plan_version"]
    assert df.groupBy(*g).count().filter(F.col("count") > 1).count() == 0


def test_deterministic(spark):
    a = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("plan_amount"), 2).alias("s")).first()
    b = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("plan_amount"), 2).alias("s")).first()
    assert a == b
