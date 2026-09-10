"""SQL access over the warehouse via the Databricks SDK Statement Execution API.

We use the SDK (already required for auth) rather than databricks-sql-connector so
the app has no pandas/numpy footprint — the connector pulls those in and their large
wheels are unreliable to install in the Apps build environment. Same result: read-only
parameterized SQL executed on the SQL warehouse.

All user input is bound as named (:name) statement parameters — never interpolated.
"""
from __future__ import annotations

from typing import Any

from databricks.sdk.service.sql import StatementParameterListItem, StatementState

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


def _to_params(params: dict[str, Any] | None) -> list[StatementParameterListItem]:
    items: list[StatementParameterListItem] = []
    for name, v in (params or {}).items():
        if isinstance(v, bool):
            items.append(StatementParameterListItem(name=name, value=str(v).lower(), type="BOOLEAN"))
        elif isinstance(v, int):
            items.append(StatementParameterListItem(name=name, value=str(v), type="INT"))
        elif isinstance(v, float):
            items.append(StatementParameterListItem(name=name, value=str(v), type="DOUBLE"))
        elif v is None:
            items.append(StatementParameterListItem(name=name, value=None))
        else:
            items.append(StatementParameterListItem(name=name, value=str(v)))
    return items


def run_query(query: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute a parameterized SELECT and return a list of typed dict rows."""
    w = config.get_workspace_client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=config.WAREHOUSE_ID,
        statement=query,
        parameters=_to_params(params),
        wait_timeout="50s",
    )
    if resp.status and resp.status.state != StatementState.SUCCEEDED:
        detail = resp.status.error.message if resp.status.error else resp.status.state
        raise RuntimeError(f"Statement failed: {detail}")

    cols = resp.manifest.schema.columns if (resp.manifest and resp.manifest.schema) else []
    names = [c.name for c in cols]
    types = [c.type_name.value if c.type_name else None for c in cols]
    data = (resp.result.data_array if resp.result else None) or []
    return [
        {names[i]: _coerce(cell, types[i]) for i, cell in enumerate(row)}
        for row in data
    ]
