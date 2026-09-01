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

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
dim_product = spark.read.table(f"{core}.dim_product")
dim_date = spark.read.table(f"{core}.dim_date")
dim_counts = {
    "store": spark.table(f"{core}.dim_store").count(),
    "customer": spark.table(f"{core}.dim_customer").count(),
    "employee": spark.table(f"{core}.dim_employee").count(),
    "promotion": spark.table(f"{core}.dim_promotion").count(),
    "product": dim_product.count(),
}
df = build_fact_sales_line(spark, config, dim_product=dim_product, dim_date=dim_date, dim_counts=dim_counts)
target = write_table_uc(spark, df, FACT_SALES_LINE_SPEC, catalog, schema_prefix)
print("wrote", target, spark.table(target).count())
