# Databricks notebook source
import sys
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")  # accepted for parity; unused by definitions
# COMMAND ----------
from techmart.semantic.registry import METRIC_VIEW_SPECS, TABLE_CONSTRAINTS
from techmart.semantic.metric_view import metric_view_ddl
from techmart.semantic.constraints import drop_pk_ddl, fk_ddl, pk_ddl, set_not_null_ddls

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
kw = dict(catalog=catalog, schema_prefix=schema_prefix)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_prefix}semantic")

# COMMAND ----------
# --- informational PK/FK constraints (NOT ENFORCED RELY) on the gold tables ---
# Pass 1: set PK columns NOT NULL (Delta columns default nullable), then drop any
# existing PK and add the PK. Doing all PKs first ensures dim PKs exist before fact
# FKs reference them.
for tc in TABLE_CONSTRAINTS:
    for stmt in set_not_null_ddls(tc, **kw):
        spark.sql(stmt.rstrip(";"))
    spark.sql(drop_pk_ddl(tc, **kw).rstrip(";"))
    spark.sql(pk_ddl(tc, **kw).rstrip(";"))
    print("pk:", tc.schema, tc.table)

# Pass 2: add FK constraints (all referenced PKs now guaranteed to exist)
for tc in TABLE_CONSTRAINTS:
    for fk in tc.foreign_keys:
        spark.sql(fk_ddl(tc, fk, **kw).rstrip(";"))
    if tc.foreign_keys:
        print("fk:", tc.schema, tc.table)

# COMMAND ----------
# --- metric views into techmart_semantic ---
for spec in METRIC_VIEW_SPECS:
    spark.sql(metric_view_ddl(spec, **kw).rstrip(";"))
    print("metric view:", spec.name)
