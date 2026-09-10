"""SQL access over the warehouse via the SQL Statement Execution REST API (requests).

We call the REST API directly with `requests` rather than databricks-sql-connector or
the SDK so the deployed app carries no pandas/numpy/cryptography footprint — those
large/native wheels are unreliable to install in the Apps build sandbox. Same result:
read-only parameterized SQL executed on the SQL warehouse.

All user input is bound as named (:name) statement parameters — never interpolated.
"""
from __future__ import annotations

import time
from typing import Any

import requests

from . import config

_INT_TYPES = {"INT", "INTEGER", "SHORT", "BYTE", "LONG", "BIGINT", "SMALLINT", "TINYINT"}
_FLOAT_TYPES = {"FLOAT", "DOUBLE", "DECIMAL"}


def _coerce(value: str | None, type_name: str | None) -> Any:
    if value is None:
        return None
    if type_name in _INT_TYPES:
        try:
            return int(value)
        except ValueError:
            return int(float(value))
    if type_name in _FLOAT_TYPES:
        return float(value)
    if type_name == "BOOLEAN":
        return value.lower() == "true"
    # STRING, DATE, TIMESTAMP, etc. are returned as their string representation.
    return value


def _to_params(params: dict[str, Any] | None) -> list[dict]:
    items: list[dict] = []
    for name, v in (params or {}).items():
        if isinstance(v, bool):
            items.append({"name": name, "value": str(v).lower(), "type": "BOOLEAN"})
        elif isinstance(v, int):
            items.append({"name": name, "value": str(v), "type": "INT"})
        elif isinstance(v, float):
            items.append({"name": name, "value": str(v), "type": "DOUBLE"})
        elif v is None:
            items.append({"name": name, "value": None})
        else:
            items.append({"name": name, "value": str(v)})
    return items


def _headers() -> dict:
    return {"Authorization": f"Bearer {config.get_access_token()}"}


def _rows_from(payload: dict) -> list[dict]:
    manifest = payload.get("manifest") or {}
    schema = manifest.get("schema") or {}
    cols = schema.get("columns") or []
    names = [c["name"] for c in cols]
    types = [c.get("type_name") for c in cols]
    data = ((payload.get("result") or {}).get("data_array")) or []
    return [
        {names[i]: _coerce(cell, types[i]) for i, cell in enumerate(row)}
        for row in data
    ]


def run_query(query: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute a parameterized SELECT and return a list of typed dict rows."""
    base = config.get_host()
    resp = requests.post(
        f"{base}/api/2.0/sql/statements",
        headers=_headers(),
        json={
            "warehouse_id": config.WAREHOUSE_ID,
            "statement": query,
            "parameters": _to_params(params),
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
        timeout=90,
    )
    resp.raise_for_status()
    payload = resp.json()

    # On-demand execution usually returns SUCCEEDED inline; poll if still running.
    statement_id = payload.get("statement_id")
    state = (payload.get("status") or {}).get("state")
    deadline = time.time() + 60
    while state in ("PENDING", "RUNNING") and statement_id and time.time() < deadline:
        time.sleep(1.0)
        r = requests.get(
            f"{base}/api/2.0/sql/statements/{statement_id}", headers=_headers(), timeout=30
        )
        r.raise_for_status()
        payload = r.json()
        state = (payload.get("status") or {}).get("state")

    if state != "SUCCEEDED":
        err = (payload.get("status") or {}).get("error") or {}
        raise RuntimeError(f"Statement {state}: {err.get('message', '')}")
    return _rows_from(payload)
