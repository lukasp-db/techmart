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
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line
from techmart.facts.fact_inventory_snapshot import FACT_INVENTORY_SNAPSHOT_SPEC, build_fact_inventory_snapshot
from techmart.facts.fact_inventory_movement import FACT_INVENTORY_MOVEMENT_SPEC, build_fact_inventory_movement
from techmart.facts.fact_returns import FACT_RETURNS_SPEC, build_fact_returns
from techmart.facts.fact_fulfillment import FACT_FULFILLMENT_SPEC, build_fact_fulfillment
from techmart.facts.fact_loyalty_activity import FACT_LOYALTY_ACTIVITY_SPEC, build_fact_loyalty_activity
from techmart.facts.fact_web_events import FACT_WEB_EVENTS_SPEC, build_fact_web_events

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
dim_product = spark.read.table(f"{core}.dim_product")
dim_date = spark.read.table(f"{core}.dim_date")
dim_store = spark.read.table(f"{core}.dim_store")
dim_customer = spark.read.table(f"{core}.dim_customer")
dim_counts = {
    "store": spark.table(f"{core}.dim_store").count(),
    "customer": spark.table(f"{core}.dim_customer").count(),
    "employee": spark.table(f"{core}.dim_employee").count(),
    "promotion": spark.table(f"{core}.dim_promotion").count(),
    "vendor": spark.table(f"{core}.dim_vendor").count(),
    "product": dim_product.count(),
}

# --- sales (anchor) ---
sales = build_fact_sales_line(spark, config, dim_product=dim_product, dim_date=dim_date, dim_counts=dim_counts)
print("wrote", write_table_uc(spark, sales, FACT_SALES_LINE_SPEC, catalog, schema_prefix))
sales = spark.read.table(f"{core}.fact_sales_line")

# --- standalone facts ---
print("wrote", write_table_uc(spark, build_fact_inventory_snapshot(spark, config, dim_store=dim_store, dim_product=dim_product, dim_date=dim_date), FACT_INVENTORY_SNAPSHOT_SPEC, catalog, schema_prefix))
print("wrote", write_table_uc(spark, build_fact_inventory_movement(spark, config, dim_date=dim_date, dim_product=dim_product, dim_counts=dim_counts), FACT_INVENTORY_MOVEMENT_SPEC, catalog, schema_prefix))
print("wrote", write_table_uc(spark, build_fact_web_events(spark, config, dim_date=dim_date, dim_counts=dim_counts), FACT_WEB_EVENTS_SPEC, catalog, schema_prefix))

# --- sales-linked facts ---
print("wrote", write_table_uc(spark, build_fact_returns(spark, config, fact_sales_line=sales, dim_date=dim_date), FACT_RETURNS_SPEC, catalog, schema_prefix))
print("wrote", write_table_uc(spark, build_fact_fulfillment(spark, config, fact_sales_line=sales, dim_date=dim_date), FACT_FULFILLMENT_SPEC, catalog, schema_prefix))
print("wrote", write_table_uc(spark, build_fact_loyalty_activity(spark, config, fact_sales_line=sales, dim_customer=dim_customer, dim_date=dim_date), FACT_LOYALTY_ACTIVITY_SPEC, catalog, schema_prefix))

for t in ("fact_sales_line", "fact_inventory_snapshot", "fact_inventory_movement", "fact_web_events", "fact_returns", "fact_fulfillment", "fact_loyalty_activity"):
    print(t, spark.table(f"{core}.{t}").count())
