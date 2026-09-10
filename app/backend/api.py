"""Framework-agnostic API logic (used by the stdlib http.server in server.py).

Each function takes plain params/dicts and returns (status_code, payload). This keeps
the app free of any web-framework dependency so the deployed app needs no pip installs
at all (the Apps build pypi proxy is unreliable) — only the Python standard library.
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone
from typing import Any

from . import config, queries

# In-process mock store of session-submitted overrides (newest-first).
_SESSION_OVERRIDES: list[dict] = []


def filters(category: str | None) -> tuple[int, dict]:
    return 200, {
        "categories": queries.fetch_categories(),
        "products": queries.fetch_products(category or None),
        **queries.fetch_week_bounds(),
        "reasons": config.REASONS,
        "planners": config.PLANNERS,
    }


def forecast(params: dict[str, str]) -> tuple[int, dict]:
    level = params.get("level")
    if level not in ("category", "product"):
        return 400, {"error": "level must be category|product"}
    version = params.get("version", "improved")
    if version not in ("improved", "baseline", "both"):
        return 400, {"error": "bad version"}
    id_ = params.get("id")
    if not id_:
        return 400, {"error": "id required"}
    try:
        fy_start = int(params["fy_start"])
        fw_start = int(params["fw_start"])
        fy_end = int(params["fy_end"])
        fw_end = int(params["fw_end"])
    except (KeyError, TypeError, ValueError):
        return 400, {"error": "bad timeframe"}

    series = queries.fetch_forecast(
        level=level, id_=id_, fy_start=fy_start, fw_start=fw_start,
        fy_end=fy_end, fw_end=fw_end, version=version,
    )
    summary_version = "improved" if version in ("improved", "both") else "baseline"
    summ_rows = [r for r in series if r["version"] == summary_version] or series
    total_qty = sum(r["forecast_qty"] or 0 for r in summ_rows)
    total_amount = sum(r["forecast_amount"] or 0 for r in summ_rows)
    n_weeks = len({(r["fiscal_year"], r["fiscal_week"]) for r in summ_rows}) or 1
    return 200, {
        "series": series,
        "summary": {
            "total_qty": total_qty,
            "total_amount": total_amount,
            "avg_weekly_qty": total_qty / n_weeks,
        },
    }


def get_adjustments(limit_raw: str | None) -> tuple[int, dict]:
    try:
        limit = max(1, min(1000, int(limit_raw))) if limit_raw else 100
    except (TypeError, ValueError):
        limit = 100
    existing = queries.fetch_overrides(limit=limit)
    combined = list(_SESSION_OVERRIDES) + existing
    return 200, {"rows": combined[:limit], "session_count": len(_SESSION_OVERRIDES)}


def _validate(body: dict) -> tuple[dict | None, str | None]:
    required = [
        "product_sk", "store_sk", "fiscal_year", "fiscal_week",
        "ai_forecast_qty", "override_qty", "override_reason", "planner_id",
    ]
    for f in required:
        if f not in body or body[f] is None:
            return None, f"missing field: {f}"
    try:
        clean: dict[str, Any] = {
            "product_sk": int(body["product_sk"]),
            "store_sk": int(body["store_sk"]),
            "fiscal_year": int(body["fiscal_year"]),
            "fiscal_week": int(body["fiscal_week"]),
            "ai_forecast_qty": float(body["ai_forecast_qty"]),
            "override_qty": float(body["override_qty"]),
            "override_reason": str(body["override_reason"]),
            "planner_id": str(body["planner_id"]),
        }
    except (TypeError, ValueError):
        return None, "non-numeric value in a numeric field"
    if clean["override_reason"] not in config.REASONS:
        return None, "invalid override_reason"
    if clean["planner_id"] not in config.PLANNERS:
        return None, "invalid planner_id"
    if clean["override_qty"] < 0:
        return None, "override_qty must be >= 0"
    if not (1 <= clean["fiscal_week"] <= 53):
        return None, "fiscal_week must be 1-53"
    return clean, None


def post_adjustment(body: dict) -> tuple[int, dict]:
    clean, err = _validate(body if isinstance(body, dict) else {})
    if err:
        return 422, {"error": err}
    product_name, category_name = queries.fetch_product_label(clean["product_sk"])
    row = {
        "override_id": f"session-{int(time.time()*1000)}-{random.randint(1000,9999)}",
        "product_name": product_name,
        "category_name": category_name,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": "session",
        **clean,
    }
    _SESSION_OVERRIDES.insert(0, row)
    return 200, {"status": "success", "row": row}
