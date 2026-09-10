"""Parameterized SQL builders + fetch functions.

All user input is bound via named (:name) parameters — never string-interpolated.
Only the `level` discriminator (category|product) selects between two static WHERE
clauses; its value is validated by the caller against a fixed set.
"""
from __future__ import annotations

from typing import Any

from . import config
from .db import run_query

FORECAST = config.FORECAST_TABLE
PRODUCT = config.PRODUCT_TABLE
DATE = config.DATE_TABLE
OVERRIDE = config.OVERRIDE_TABLE


# ---------------------------------------------------------------- filters ----
def fetch_categories() -> list[str]:
    rows = run_query(
        f"""
        SELECT DISTINCT p.category_name AS category_name
        FROM {FORECAST} f
        JOIN {PRODUCT} p ON f.product_sk = p.product_sk
        WHERE p.category_name IS NOT NULL
        ORDER BY category_name
        """
    )
    return [r["category_name"] for r in rows]


def fetch_products(category: str | None = None) -> list[dict]:
    where = "WHERE p.product_name IS NOT NULL"
    params: dict[str, Any] = {}
    if category:
        where += " AND p.category_name = :cat"
        params["cat"] = category
    rows = run_query(
        f"""
        SELECT DISTINCT p.product_sk AS product_sk,
               p.category_name AS category_name,
               p.product_name  AS product_name
        FROM {FORECAST} f
        JOIN {PRODUCT} p ON f.product_sk = p.product_sk
        {where}
        ORDER BY product_name
        """,
        params,
    )
    return [
        {
            "product_sk": r["product_sk"],
            "label": f"{r['product_name']} ({r['category_name']})",
        }
        for r in rows
    ]


def fetch_week_bounds() -> dict:
    rows = run_query(
        f"""
        SELECT MIN(fiscal_year * 100 + fiscal_week) AS lo,
               MAX(fiscal_year * 100 + fiscal_week) AS hi
        FROM {FORECAST}
        """
    )
    lo, hi = rows[0]["lo"], rows[0]["hi"]
    return {
        "week_min": {"fiscal_year": lo // 100, "fiscal_week": lo % 100},
        "week_max": {"fiscal_year": hi // 100, "fiscal_week": hi % 100},
    }


# --------------------------------------------------------------- forecast ----
def fetch_forecast(
    level: str,
    id_: str,
    fy_start: int,
    fw_start: int,
    fy_end: int,
    fw_end: int,
    version: str,
) -> list[dict]:
    """Aggregate SUM across stores to (week, version). `version` = improved|baseline|both."""
    if level == "category":
        grain_join = f"JOIN {PRODUCT} p ON f.product_sk = p.product_sk"
        grain_where = "p.category_name = :id"
        id_value: Any = id_
    else:  # product
        grain_join = ""
        grain_where = "f.product_sk = :id"
        # A product id must be an int; if a stale category name slips through
        # (e.g. a UI race while switching level), return empty rather than 500.
        try:
            id_value = int(id_)
        except (TypeError, ValueError):
            return []

    params: dict[str, Any] = {
        "id": id_value,
        "lo": fy_start * 100 + fw_start,
        "hi": fy_end * 100 + fw_end,
    }
    version_clause = ""
    if version in ("improved", "baseline"):
        version_clause = "AND f.forecast_version = :ver"
        params["ver"] = version

    rows = run_query(
        f"""
        SELECT f.fiscal_year AS fiscal_year,
               f.fiscal_week AS fiscal_week,
               MAX(d.date)   AS week_end_date,
               f.forecast_version AS version,
               SUM(f.forecast_qty)    AS forecast_qty,
               SUM(f.forecast_amount) AS forecast_amount,
               SUM(f.lower_bound)     AS lower_bound,
               SUM(f.upper_bound)     AS upper_bound
        FROM {FORECAST} f
        {grain_join}
        JOIN {DATE} d ON f.date_sk = d.date_sk
        WHERE {grain_where}
          AND (f.fiscal_year * 100 + f.fiscal_week) BETWEEN :lo AND :hi
          {version_clause}
        GROUP BY f.fiscal_year, f.fiscal_week, f.forecast_version
        ORDER BY f.fiscal_year, f.fiscal_week, f.forecast_version
        """,
        params,
    )
    for r in rows:
        # week_end_date arrives as an ISO date string (e.g. "2025-02-08").
        for k in ("forecast_qty", "forecast_amount", "lower_bound", "upper_bound"):
            r[k] = float(r[k]) if r[k] is not None else None
    return rows


# ------------------------------------------------------------ adjustments ----
def fetch_overrides(limit: int = 100) -> list[dict]:
    """Read existing overrides newest-first, joined to dim_product for labels.

    Returns [] on any error so the app still works if the federated table is down.
    """
    try:
        rows = run_query(
            f"""
            SELECT o.override_id     AS override_id,
                   o.product_sk      AS product_sk,
                   p.product_name    AS product_name,
                   p.category_name   AS category_name,
                   o.store_sk        AS store_sk,
                   o.fiscal_year     AS fiscal_year,
                   o.fiscal_week     AS fiscal_week,
                   o.ai_forecast_qty AS ai_forecast_qty,
                   o.override_qty    AS override_qty,
                   o.override_reason AS override_reason,
                   o.planner_id      AS planner_id,
                   o.created_at      AS created_at
            FROM {OVERRIDE} o
            LEFT JOIN {PRODUCT} p ON o.product_sk = p.product_sk
            ORDER BY o.created_at DESC
            LIMIT :lim
            """,
            {"lim": int(limit)},
        )
    except Exception as exc:  # pragma: no cover - defensive fallback
        print(f"[warn] fetch_overrides failed, returning empty list: {exc}")
        return []

    out = []
    for r in rows:
        out.append(
            {
                "override_id": str(r["override_id"]),
                "product_sk": r["product_sk"],
                "product_name": r.get("product_name"),
                "category_name": r.get("category_name"),
                "store_sk": r["store_sk"],
                "fiscal_year": r["fiscal_year"],
                "fiscal_week": r["fiscal_week"],
                "ai_forecast_qty": float(r["ai_forecast_qty"])
                if r["ai_forecast_qty"] is not None
                else None,
                "override_qty": float(r["override_qty"])
                if r["override_qty"] is not None
                else None,
                "override_reason": r["override_reason"],
                "planner_id": r["planner_id"],
                "created_at": r.get("created_at"),
                "source": "lakebase",
            }
        )
    return out


def fetch_product_label(product_sk: int) -> tuple[str | None, str | None]:
    rows = run_query(
        f"SELECT product_name, category_name FROM {PRODUCT} WHERE product_sk = :sk LIMIT 1",
        {"sk": int(product_sk)},
    )
    if rows:
        return rows[0].get("product_name"), rows[0].get("category_name")
    return None, None
