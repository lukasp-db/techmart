# Databricks notebook source
# MAGIC %pip install dbldatagen jmespath pyparsing
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import sys
# DAB syncs the bundle root; this notebook lives in notebooks/, package in src/.
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")
dbutils.widgets.text("seed", "42")
# COMMAND ----------
from pathlib import Path
from techmart.config import load_config
from techmart.spark.uc_write import write_table_uc
from techmart.spark.dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from techmart.spark.dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
from techmart.spark.dimensions.dim_store import DIM_STORE_SPEC, build_dim_store
from techmart.spark.dimensions.dim_vendor import DIM_VENDOR_SPEC, build_dim_vendor
from techmart.spark.dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion
from techmart.spark.dimensions.dim_employee import DIM_EMPLOYEE_SPEC, build_dim_employee
from techmart.spark.dimensions.dim_customer import DIM_CUSTOMER_SPEC, build_dim_customer
from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
builders = [
    (DIM_DATE_SPEC, build_dim_date), (DIM_CHANNEL_SPEC, build_dim_channel),
    (DIM_STORE_SPEC, build_dim_store), (DIM_VENDOR_SPEC, build_dim_vendor),
    (DIM_PROMOTION_SPEC, build_dim_promotion), (DIM_EMPLOYEE_SPEC, build_dim_employee),
    (DIM_CUSTOMER_SPEC, build_dim_customer), (DIM_PRODUCT_SPEC, build_dim_product),
]
for spec, build in builders:
    target = write_table_uc(spark, build(spark, config), spec, catalog, schema_prefix)
    print("wrote", target, spark.table(target).count())
