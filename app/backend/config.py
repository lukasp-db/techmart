"""Environment + dual-mode auth configuration.

Local dev: WorkspaceClient(profile=DATABRICKS_PROFILE) using the CLI profile.
In the app: WorkspaceClient() auto-detects the injected service-principal creds.
Either way the access token is extracted via w.config.authenticate() so it works
for OAuth/U2M/SP auth (w.config.token is None for those).
"""
from __future__ import annotations

import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient

# --- Table / catalog names (env-overridable, sensible defaults) ---
CATALOG = os.environ.get("TECHMART_CATALOG", "stable_classic_ppke9o")
AI_SCHEMA = os.environ.get("TECHMART_AI_SCHEMA", "techmart_ai")
CORE_SCHEMA = os.environ.get("TECHMART_CORE_SCHEMA", "techmart_core")
LAKEBASE_CATALOG = os.environ.get("TECHMART_LAKEBASE_CATALOG", "techmart_lakebase")
OPS_SCHEMA = os.environ.get("TECHMART_OPS_SCHEMA", "techmart_ops")

FORECAST_TABLE = f"{CATALOG}.{AI_SCHEMA}.fact_sales_forecast"
PRODUCT_TABLE = f"{CATALOG}.{CORE_SCHEMA}.dim_product"
DATE_TABLE = f"{CATALOG}.{CORE_SCHEMA}.dim_date"
OVERRIDE_TABLE = f"{LAKEBASE_CATALOG}.{OPS_SCHEMA}.forecast_override"

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "ec3c986a891e0b79")
HTTP_PATH = os.environ.get(
    "DATABRICKS_HTTP_PATH", f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
)

# Detect the Databricks Apps runtime (SP creds auto-injected).
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Controlled vocabularies (form validation).
REASONS = [
    "Local promotion",
    "Competitor closeout",
    "Weather event",
    "Known stockout recovery",
]
PLANNERS = ["planner_amir", "planner_bianca", "planner_chen", "planner_dana"]


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "field-eng-east")
    return WorkspaceClient(profile=profile)


def get_host() -> str:
    """Workspace hostname WITHOUT scheme (databricks-sql wants bare host)."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
    else:
        host = get_workspace_client().config.host
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def get_access_token() -> str:
    """Fresh bearer token via the SDK (works for OAuth/U2M/SP)."""
    headers = get_workspace_client().config.authenticate()
    return headers["Authorization"].replace("Bearer ", "")
