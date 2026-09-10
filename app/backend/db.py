"""Read-only SQL over the warehouse via the Databricks SDK Statement Execution API.

We deliberately do NOT use databricks-sql-connector: its transitive chain
(pandas -> numpy, pyarrow) cannot be installed in the Databricks Apps build
environment (the build pip proxy connect-fails fetching the large numpy wheel).
The SDK Statement Execution API is pure-Python (already a dependency for auth),
runs the same read-only, parameterized SQL on the same serverless warehouse, and
carries no heavy binary deps.

All user input is bound via named (`:name`) statement parameters — never
string-interpolated. Cells come back as strings in a JSON array and are coerced to
JSON-friendly typed values using the result manifest's column types.
"""
from __future__ import annotations

import time
from typing import Any

from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from . import config

_TERMINAL = {StatementState.SUCCEEDED, StatementState.FAILED,
             StatementState.CANCELED, StatementState.CLOSED}


def _param_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    return "STRING"


def _coerce(value: Any, type_name: str | None) -> Any:
    if value is None:
        return None
    t = (type_name or "").upper()
    try:
        if t in ("INT", "INTEGER", "BIGINT", "LONG", "SHORT", "BYTE"):
            return int(value)
        if t in ("DOUBLE", "FLOAT", "DECIMAL"):
            return float(value)
    except (TypeError, ValueError):
        return value
    return value  # DATE/TIMESTAMP/STRING left as ISO/text strings


def run_query(query: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute a parameterized SELECT and return a list of typed dict rows."""
    w = config._workspace_client()
    sdk_params = [
        StatementParameterListItem(name=k, value=str(v), type=_param_type(v))
        for k, v in (params or {}).items()
    ]
    resp = w.statement_execution.execute_statement(
        warehouse_id=config.WAREHOUSE_ID,
        statement=query,
        parameters=sdk_params or None,
        wait_timeout="50s",
    )

    # Poll to terminal state if the 50s inline wait was not enough.
    statement_id = resp.statement_id
    deadline = time.time() + 120
    while resp.status and resp.status.state not in _TERMINAL and time.time() < deadline:
        time.sleep(1.5)
        resp = w.statement_execution.get_statement(statement_id)

    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        msg = ""
        if resp.status and resp.status.error:
            msg = resp.status.error.message or ""
        raise RuntimeError(f"statement {statement_id} state={state}: {msg}")

    manifest = resp.manifest
    columns = manifest.schema.columns if (manifest and manifest.schema) else []
    names = [c.name for c in columns]
    types = [c.type_name.value if c.type_name else None for c in columns]

    result = resp.result
    data = (result.data_array if result and result.data_array else []) or []
    return [
        {names[i]: _coerce(cell, types[i]) for i, cell in enumerate(row)}
        for row in data
    ]
