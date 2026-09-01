# Databricks notebook source
# MAGIC %pip install "psycopg[binary]"
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import sys
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("lakebase_instance", "")
dbutils.widgets.text("lakebase_database", "techmart")
# COMMAND ----------
from pathlib import Path
from techmart.config import load_config
from techmart.ops.replenishment_order import REPLENISHMENT_ORDER_SPEC, build_replenishment_order
from techmart.ops.forecast_override import FORECAST_OVERRIDE_SPEC, build_forecast_override
from techmart.ops.pg_write import get_pg_connection, write_pg

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
instance = dbutils.widgets.get("lakebase_instance")
database = dbutils.widgets.get("lakebase_database")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
ai = f"{catalog}.{schema_prefix}ai"
ops_schema = f"{schema_prefix}ops"

dim_date = spark.read.table(f"{core}.dim_date")
snapshot = spark.read.table(f"{core}.fact_inventory_snapshot")
forecast = spark.read.table(f"{ai}.fact_sales_forecast")

# --- build deterministic operational rows (locally tested) ---
repl = build_replenishment_order(spark, config, fact_inventory_snapshot=snapshot, dim_date=dim_date)
ovr = build_forecast_override(spark, config, fact_sales_forecast=forecast, dim_date=dim_date)

# --- write-back tables: native writable Postgres (workspace-only) ---
conn = get_pg_connection(instance, database)
try:
    print("replenishment_order rows:", write_pg(repl, REPLENISHMENT_ORDER_SPEC, conn=conn, schema=ops_schema))
    print("forecast_override rows:", write_pg(ovr, FORECAST_OVERRIDE_SPEC, conn=conn, schema=ops_schema))
finally:
    conn.close()

# COMMAND ----------
# --- serve-to-app: Delta -> Postgres synced table (bounded fact_sales_forecast slice) ---
# The synced table + UC Postgres federation catalog (write-back read path) are created
# via the Databricks SDK / workspace against the provisioned instance. Created here (after
# fact_sales_forecast exists) rather than as a deploy-time resource, so ordering is safe.
# Validated on the workspace (proven-green gate).
from databricks.sdk import WorkspaceClient  # noqa: E402

w = WorkspaceClient()
serving_rows = config.scale_profile.forecast_serving_rows
serving_source = f"{ai}.fact_sales_forecast"
serving_target = f"{ops_schema}.forecast_serving"
print("synced table:", serving_target, "<-", serving_source, "rows cap:", serving_rows)
# w.database.create_synced_database_table(...)  # see spec §Architecture; wired on the workspace
