# Databricks notebook source
# MAGIC %pip install dbldatagen jmespath pyparsing
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import sys
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")
dbutils.widgets.text("seed", "42")
# COMMAND ----------
from pathlib import Path
from techmart.config import load_config
from techmart.spark.uc_write import write_table_uc
from techmart.finance.dim_department import DIM_DEPARTMENT_SPEC, build_dim_department
from techmart.finance.dim_gl_account import DIM_GL_ACCOUNT_SPEC, build_dim_gl_account
from techmart.finance.fact_gl_actuals import FACT_GL_ACTUALS_SPEC, build_fact_gl_actuals
from techmart.finance.fact_budget_plan import FACT_BUDGET_PLAN_SPEC, build_fact_budget_plan
from techmart.finance.fact_inventory_valuation import FACT_INVENTORY_VALUATION_SPEC, build_fact_inventory_valuation

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
fin = f"{catalog}.{schema_prefix}finance"

dim_date = spark.read.table(f"{core}.dim_date")
dim_product = spark.read.table(f"{core}.dim_product")
sales = spark.read.table(f"{core}.fact_sales_line")
returns = spark.read.table(f"{core}.fact_returns")
movement = spark.read.table(f"{core}.fact_inventory_movement")
snapshot = spark.read.table(f"{core}.fact_inventory_snapshot")

# --- finance dims (independent) ---
dim_department = build_dim_department(spark, config)
print("wrote", write_table_uc(spark, dim_department, DIM_DEPARTMENT_SPEC, catalog, schema_prefix))
dim_gl_account = build_dim_gl_account(spark, config)
print("wrote", write_table_uc(spark, dim_gl_account, DIM_GL_ACCOUNT_SPEC, catalog, schema_prefix))

# --- gl actuals (derived) ---
actuals = build_fact_gl_actuals(spark, config, fact_sales_line=sales, fact_returns=returns,
                                fact_inventory_movement=movement, dim_date=dim_date,
                                dim_gl_account=dim_gl_account, dim_department=dim_department)
print("wrote", write_table_uc(spark, actuals, FACT_GL_ACTUALS_SPEC, catalog, schema_prefix))
actuals = spark.read.table(f"{fin}.fact_gl_actuals")

# --- budget (off persisted actuals) ---
budget = build_fact_budget_plan(spark, config, fact_gl_actuals=actuals, dim_gl_account=dim_gl_account)
print("wrote", write_table_uc(spark, budget, FACT_BUDGET_PLAN_SPEC, catalog, schema_prefix))

# --- inventory valuation (derived) ---
val = build_fact_inventory_valuation(spark, config, fact_inventory_snapshot=snapshot,
                                     fact_sales_line=sales, dim_product=dim_product, dim_date=dim_date)
print("wrote", write_table_uc(spark, val, FACT_INVENTORY_VALUATION_SPEC, catalog, schema_prefix))

for t in ("dim_department", "dim_gl_account", "fact_gl_actuals", "fact_budget_plan", "fact_inventory_valuation"):
    print(t, spark.table(f"{fin}.{t}").count())
