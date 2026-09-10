"""Environment + dual-mode auth configuration.

Auth is done with the standard library + `requests` so the deployed app needs no
databricks-sdk (its google-auth/cryptography wheels are unreliable to install in the
Apps build sandbox):

- In the app: the runtime injects DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET /
  DATABRICKS_HOST. We run the OAuth client-credentials flow against the workspace
  OIDC token endpoint to get a short-lived all-apis token, cached until near expiry.
- Local dev: no client secret is present, so we lazily import the Databricks SDK and
  read the token from the CLI profile (SDK is a dev-only dependency).
"""
from __future__ import annotations

import base64
import os
import threading
import time

import requests

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
_token_cache: dict[str, float | str] = {"token": "", "expires_at": 0.0}


def _normalize_host(host: str) -> str:
    host = (host or "").strip()
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def get_host() -> str:
    """Workspace base URL, with scheme (e.g. https://adb-....azuredatabricks.net)."""
    env_host = os.environ.get("DATABRICKS_HOST")
    if env_host:
        return _normalize_host(env_host)
    # Local dev: read from the SDK/CLI profile.
    from databricks.sdk import WorkspaceClient  # lazy: dev-only dependency

    profile = os.environ.get("DATABRICKS_PROFILE", "field-eng-east")
    return _normalize_host(WorkspaceClient(profile=profile).config.host)


def _client_credentials_token() -> tuple[str, float]:
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        f"{get_host()}/oidc/v1/token",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    return payload["access_token"], time.time() + float(payload.get("expires_in", 3600))


def _sdk_token() -> tuple[str, float]:
    from databricks.sdk import WorkspaceClient  # lazy: dev-only dependency

    profile = os.environ.get("DATABRICKS_PROFILE", "field-eng-east")
    w = WorkspaceClient(profile=profile)
    headers = w.config.authenticate()
    token = headers["Authorization"].replace("Bearer ", "")
    return token, time.time() + 600  # refresh conservatively


def get_access_token() -> str:
    """Bearer token for the REST API; cached and refreshed before expiry."""
    with _lock:
        if _token_cache["token"] and time.time() < float(_token_cache["expires_at"]) - 60:
            return str(_token_cache["token"])
        if os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET"):
            token, expires_at = _client_credentials_token()
        else:
            token, expires_at = _sdk_token()
        _token_cache["token"] = token
        _token_cache["expires_at"] = expires_at
        return token
