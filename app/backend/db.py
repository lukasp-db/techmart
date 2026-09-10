"""SQL access over the warehouse via databricks-sql-connector.

We open a short-lived connection per query with the current dual-mode bearer token
(local = SDK profile; app = injected SP OAuth) and run read-only parameterized SQL on
the serverless SQL warehouse. The connector's native parameter markers (`:name`) bind
values server-side — user input is never string-interpolated.

The connector returns already-typed cells; we only normalize temporal/Decimal values
to JSON-friendly forms (dates/timestamps -> ISO strings, Decimal -> float) so the API
payloads match what the frontend expects.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from databricks import sql

from . import config


def _coerce(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):  # datetime is a subclass of date
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def run_query(query: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute a parameterized SELECT and return a list of typed dict rows."""
    with sql.connect(
        server_hostname=config.get_server_hostname(),
        http_path=config.get_http_path(),
        access_token=config.get_access_token(),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query, parameters=params or None)
            columns = [c[0] for c in cur.description]
            rows = cur.fetchall()
    return [
        {columns[i]: _coerce(cell) for i, cell in enumerate(row)}
        for row in rows
    ]
