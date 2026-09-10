"""Environment + dual-mode auth configuration.

Auth uses the Databricks SDK (a runtime dependency) in both modes:

- In the app: the runtime injects DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET /
  DATABRICKS_HOST; the default SDK auth chain picks these up (SP OAuth M2M) and
  `w.config.authenticate()` mints a short-lived workspace bearer token.
- Local dev: no injected SP creds, so the SDK reads the CLI profile
  (DATABRICKS_PROFILE, default `field-eng-east`).

The resulting bearer token is passed to databricks-sql-connector as `access_token`.
"""
from __future__ import annotations

import os
import threading
from urllib.parse import urlparse

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

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Controlled vocabularies (form validation).
REASONS = [
    "Local promotion",
    "Competitor closeout",
    "Weather event",
    "Known stockout recovery",
]
PLANNERS = ["planner_amir", "planner_bianca", "planner_chen", "planner_dana"]

_lock = threading.Lock()
_client: WorkspaceClient | None = None


def _normalize_host(host: str) -> str:
    host = (host or "").strip()
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _workspace_client() -> WorkspaceClient:
    """Cached WorkspaceClient — SP OAuth in the app, CLI profile locally."""
    global _client
    with _lock:
        if _client is None:
            if IS_DATABRICKS_APP:
                _client = WorkspaceClient()  # injected SP creds
            else:
                profile = os.environ.get("DATABRICKS_PROFILE", "field-eng-east")
                _client = WorkspaceClient(profile=profile)
        return _client


def get_host() -> str:
    """Workspace base URL, with scheme (e.g. https://adb-....azuredatabricks.net)."""
    env_host = os.environ.get("DATABRICKS_HOST")
    if env_host:
        return _normalize_host(env_host)
    return _normalize_host(_workspace_client().config.host)


def get_server_hostname() -> str:
    """Bare hostname (no scheme) for the SQL connector."""
    return urlparse(get_host()).netloc


def get_http_path() -> str:
    """SQL warehouse HTTP path for the connector."""
    return f"/sql/1.0/warehouses/{WAREHOUSE_ID}"


def get_access_token() -> str:
    """Bearer token for the SQL connector; the SDK caches/refreshes it internally."""
    headers = _workspace_client().config.authenticate()
    return headers["Authorization"].replace("Bearer ", "")
