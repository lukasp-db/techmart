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
from techmart.ai.anomalies import AI_ANOMALY_CATALOG_SPEC, build_ai_anomaly_catalog
from techmart.ai.fact_sales_forecast import FACT_SALES_FORECAST_SPEC, build_fact_sales_forecast
from techmart.ai.product_review import PRODUCT_REVIEW_STAGING_SPEC, build_product_review_staging
from techmart.ai.service_case import SERVICE_CASE_STAGING_SPEC, build_service_case_staging

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
ai = f"{catalog}.{schema_prefix}ai"

dim_date = spark.read.table(f"{core}.dim_date")
dim_product = spark.read.table(f"{core}.dim_product")
sales = spark.read.table(f"{core}.fact_sales_line")

# --- anomaly catalog ---
print("wrote", write_table_uc(spark, build_ai_anomaly_catalog(spark, config, dim_date=dim_date),
                              AI_ANOMALY_CATALOG_SPEC, catalog, schema_prefix))

# --- forecast (derived from sales actuals) ---
fc = build_fact_sales_forecast(spark, config, fact_sales_line=sales, dim_date=dim_date)
print("wrote", write_table_uc(spark, fc, FACT_SALES_FORECAST_SPEC, catalog, schema_prefix))

# --- review/case staging (text filled by the generate_ai_text SQL task) ---
rev = build_product_review_staging(spark, config, fact_sales_line=sales,
                                   dim_product=dim_product, dim_date=dim_date)
print("wrote", write_table_uc(spark, rev, PRODUCT_REVIEW_STAGING_SPEC, catalog, schema_prefix))
cases = build_service_case_staging(spark, config, fact_sales_line=sales, dim_date=dim_date)
print("wrote", write_table_uc(spark, cases, SERVICE_CASE_STAGING_SPEC, catalog, schema_prefix))

for t in ("ai_anomaly_catalog", "fact_sales_forecast",
          "_product_review_staging", "_service_case_staging"):
    print(t, spark.table(f"{ai}.{t}").count())
