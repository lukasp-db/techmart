"""TechMart — Demand Forecast Adjustments. Starlette backend + static SPA host.

Read-only over Unity Catalog via the Databricks SDK Statement Execution API. The
POST /api/adjustments endpoint is a MOCK: it validates + appends to an in-process
list and does NOT write to Lakebase.

Uses Starlette (not FastAPI) so the app has no pydantic/pydantic-core dependency —
its native wheel is unreliable to install in the Apps build sandbox. Validation is
done explicitly in the POST handler.
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from . import config, queries

# In-process mock store of session-submitted overrides (newest-first).
_SESSION_OVERRIDES: list[dict] = []


def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "app": bool(config.IS_DATABRICKS_APP)})


def filters(request: Request) -> JSONResponse:
    category = request.query_params.get("category") or None
    return JSONResponse(
        {
            "categories": queries.fetch_categories(),
            "products": queries.fetch_products(category),
            **queries.fetch_week_bounds(),
            "reasons": config.REASONS,
            "planners": config.PLANNERS,
        }
    )


def forecast(request: Request) -> JSONResponse:
    q = request.query_params
    level = q.get("level")
    if level not in ("category", "product"):
        return JSONResponse({"error": "level must be category|product"}, status_code=400)
    version = q.get("version", "improved")
    if version not in ("improved", "baseline", "both"):
        return JSONResponse({"error": "bad version"}, status_code=400)
    id_ = q.get("id")
    if not id_:
        return JSONResponse({"error": "id required"}, status_code=400)
    try:
        fy_start = int(q.get("fy_start"))
        fw_start = int(q.get("fw_start"))
        fy_end = int(q.get("fy_end"))
        fw_end = int(q.get("fw_end"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "bad timeframe"}, status_code=400)

    series = queries.fetch_forecast(
        level=level, id_=id_, fy_start=fy_start, fw_start=fw_start,
        fy_end=fy_end, fw_end=fw_end, version=version,
    )
    summary_version = "improved" if version in ("improved", "both") else "baseline"
    summ_rows = [r for r in series if r["version"] == summary_version] or series
    total_qty = sum(r["forecast_qty"] or 0 for r in summ_rows)
    total_amount = sum(r["forecast_amount"] or 0 for r in summ_rows)
    n_weeks = len({(r["fiscal_year"], r["fiscal_week"]) for r in summ_rows}) or 1
    return JSONResponse(
        {
            "series": series,
            "summary": {
                "total_qty": total_qty,
                "total_amount": total_amount,
                "avg_weekly_qty": total_qty / n_weeks,
            },
        }
    )


def get_adjustments(request: Request) -> JSONResponse:
    try:
        limit = max(1, min(1000, int(request.query_params.get("limit", "100"))))
    except (TypeError, ValueError):
        limit = 100
    existing = queries.fetch_overrides(limit=limit)
    combined = list(_SESSION_OVERRIDES) + existing
    return JSONResponse({"rows": combined[:limit], "session_count": len(_SESSION_OVERRIDES)})


def _validate_adjustment(body: dict) -> tuple[dict | None, str | None]:
    required = [
        "product_sk", "store_sk", "fiscal_year", "fiscal_week",
        "ai_forecast_qty", "override_qty", "override_reason", "planner_id",
    ]
    for f in required:
        if f not in body or body[f] is None:
            return None, f"missing field: {f}"
    try:
        clean = {
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


async def post_adjustment(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)
    clean, err = _validate_adjustment(body if isinstance(body, dict) else {})
    if err:
        return JSONResponse({"error": err}, status_code=422)

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
    return JSONResponse({"status": "success", "row": row})


routes = [
    Route("/api/health", health),
    Route("/api/filters", filters),
    Route("/api/forecast", forecast),
    Route("/api/adjustments", get_adjustments, methods=["GET"]),
    Route("/api/adjustments", post_adjustment, methods=["POST"]),
]

# --- Static SPA (built frontend) -------------------------------------------
# Order matters: the /assets mount must precede the SPA catch-all.
_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
if _DIST.exists():
    async def serve_spa(request: Request):
        full = request.path_params.get("full_path", "")
        candidate = _DIST / full
        if full and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(_DIST / "index.html")

    routes.append(Mount("/assets", app=StaticFiles(directory=str(_DIST / "assets")), name="assets"))
    routes.append(Route("/{full_path:path}", serve_spa))

app = Starlette(routes=routes)
