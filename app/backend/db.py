"""databricks-sql-connector connection factory + query helper.

Read-only, low volume: we open a short-lived connection per request. The token
is re-fetched from the SDK config each time (the SDK caches + refreshes it), so
long-lived processes never hand out an expired token.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from databricks import sql

from . import config


@contextmanager
def get_connection():
    conn = sql.connect(
        server_hostname=config.get_host(),
        http_path=config.HTTP_PATH,
        access_token=config.get_access_token(),
    )
    try:
        yield conn
    finally:
        conn.close()


def run_query(query: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute a parameterized SELECT and return a list of dict rows.

    Parameters use the connector's native named style (:name). NEVER interpolate
    user input into `query`.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or {})
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
