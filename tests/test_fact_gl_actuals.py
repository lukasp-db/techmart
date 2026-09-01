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
from techmart.finance.fact_gl_actuals import FACT_GL_ACTUALS_SPEC, build_fact_gl_actuals

_P = ScaleProfile("t", 8, 40, 1, 4000, 300, 20)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 8, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 40}


def _inputs(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=4000)
    returns = build_fact_returns(spark, _CFG, fact_sales_line=sales, dim_date=dd)
    mov = build_fact_inventory_movement(spark, _CFG, dim_date=dd, dim_product=dp, dim_counts=_COUNTS)
    return dd, dp, sales, returns, mov


def _build(spark):
    dd, dp, sales, returns, mov = _inputs(spark)
    return build_fact_gl_actuals(
        spark, _CFG, fact_sales_line=sales, fact_returns=returns, fact_inventory_movement=mov,
        dim_date=dd, dim_gl_account=build_dim_gl_account(spark, _CFG),
        dim_department=build_dim_department(spark, _CFG),
    ), sales, returns, dd


def test_schema_and_grain(spark):
    df, *_ = _build(spark)
    assert df.columns == FACT_GL_ACTUALS_SPEC.column_names
    grain_cols = ["gl_account_sk", "store_sk", "department_sk", "date_sk"]
    assert df.groupBy(*grain_cols).count().filter(F.col("count") > 1).count() == 0


def test_referential_integrity(spark):
    df, _, _, dd = _build(spark)
    gl = build_dim_gl_account(spark, _CFG).select("gl_account_sk")
    dep = build_dim_department(spark, _CFG).select("department_sk")
    assert df.select("date_sk").distinct().join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
    assert df.select("gl_account_sk").distinct().join(gl, "gl_account_sk", "left_anti").count() == 0
    assert df.select("department_sk").distinct().join(dep, "department_sk", "left_anti").count() == 0


def test_gross_conserved_by_timing_shift(spark):
    df, sales, _, _ = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    gross_num = [a for a in gl.collect() if a["account_number"] == "4000"][0]["gl_account_sk"]
    recognized = df.filter(F.col("gl_account_sk") == gross_num).agg(F.round(F.sum("actual_amount"), 2)).first()[0]
    merch_gross = sales.agg(F.round(F.sum("gross_sales_amount"), 2)).first()[0]
    assert abs(recognized - merch_gross) < 0.05  # penny-rounding tolerance across many rows


def test_net_sales_reconciliation(spark):
    df, sales, returns, _ = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    rev_sks = [a["gl_account_sk"] for a in gl.collect() if a["account_number"] in {"4000", "4100", "4200"}]
    net = df.filter(F.col("gl_account_sk").isin(rev_sks)).agg(F.sum("actual_amount")).first()[0]
    gross = sales.agg(F.sum("gross_sales_amount")).first()[0]
    ret = returns.agg(F.sum("refund_amount")).first()[0]
    expected = gross - ret - _P.allowance_rate * gross
    assert abs(net - expected) < 0.5  # rounding across rows


def test_opex_present_and_positive(spark):
    df, *_ = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    opex_sks = [a["gl_account_sk"] for a in gl.collect() if a["account_number"] in
                {"6000", "6100", "6200", "6300", "6400", "6500"}]
    opex = df.filter(F.col("gl_account_sk").isin(opex_sks))
    assert opex.count() > 0
    assert opex.filter(F.col("actual_amount") < 0).count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("actual_amount"), 2).alias("s")).first()
    b = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("actual_amount"), 2).alias("s")).first()
    assert a == b
