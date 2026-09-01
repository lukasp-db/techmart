from __future__ import annotations

from pyspark.sql import SparkSession

from ..config import TechmartConfig
from ..finance.dim_department import DIM_DEPARTMENT_SPEC, build_dim_department
from ..finance.dim_gl_account import DIM_GL_ACCOUNT_SPEC, build_dim_gl_account
from ..finance.fact_gl_actuals import FACT_GL_ACTUALS_SPEC, build_fact_gl_actuals
from ..finance.fact_budget_plan import FACT_BUDGET_PLAN_SPEC, build_fact_budget_plan
from ..finance.fact_inventory_valuation import FACT_INVENTORY_VALUATION_SPEC, build_fact_inventory_valuation
from ..spark.uc_write import write_table_uc


def main(spark: SparkSession, config: TechmartConfig, catalog: str, schema_prefix: str) -> None:
    """Serverless DAB entrypoint: read core tables from UC, write all finance tables to UC."""
    core = f"{catalog}.{schema_prefix}core"
    fin = f"{catalog}.{schema_prefix}finance"

    # Read core tables from UC (written by the generate_facts task)
    dim_date = spark.read.table(f"{core}.dim_date")
    dim_product = spark.read.table(f"{core}.dim_product")
    sales = spark.read.table(f"{core}.fact_sales_line")
    returns = spark.read.table(f"{core}.fact_returns")
    movement = spark.read.table(f"{core}.fact_inventory_movement")
    snapshot = spark.read.table(f"{core}.fact_inventory_snapshot")

    # --- finance dims (independent) ---
    dim_department = build_dim_department(spark, config)
    target = write_table_uc(spark, dim_department, DIM_DEPARTMENT_SPEC, catalog, schema_prefix)
    print(f"wrote {target}")

    dim_gl_account = build_dim_gl_account(spark, config)
    target = write_table_uc(spark, dim_gl_account, DIM_GL_ACCOUNT_SPEC, catalog, schema_prefix)
    print(f"wrote {target}")

    # --- gl actuals (derived from core facts + finance dims) ---
    actuals = build_fact_gl_actuals(
        spark, config,
        fact_sales_line=sales,
        fact_returns=returns,
        fact_inventory_movement=movement,
        dim_date=dim_date,
        dim_gl_account=dim_gl_account,
        dim_department=dim_department,
    )
    target = write_table_uc(spark, actuals, FACT_GL_ACTUALS_SPEC, catalog, schema_prefix)
    print(f"wrote {target}")
    # Re-read persisted fact_gl_actuals so budget builds off the deterministic written rows
    actuals = spark.read.table(f"{fin}.fact_gl_actuals")

    # --- budget plan (off persisted actuals + dim_gl_account) ---
    budget = build_fact_budget_plan(spark, config, fact_gl_actuals=actuals, dim_gl_account=dim_gl_account)
    target = write_table_uc(spark, budget, FACT_BUDGET_PLAN_SPEC, catalog, schema_prefix)
    print(f"wrote {target}")

    # --- inventory valuation (snapshot + sales + dims) ---
    val = build_fact_inventory_valuation(
        spark, config,
        fact_inventory_snapshot=snapshot,
        fact_sales_line=sales,
        dim_product=dim_product,
        dim_date=dim_date,
    )
    target = write_table_uc(spark, val, FACT_INVENTORY_VALUATION_SPEC, catalog, schema_prefix)
    print(f"wrote {target}")
