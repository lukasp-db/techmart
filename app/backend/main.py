"""TechMart — Demand Forecast Adjustments. FastAPI backend + static SPA host.

Read-only over Unity Catalog via databricks-sql-connector. The POST /api/adjustments
endpoint is a MOCK: it validates + appends to an in-process list and does NOT write
to Lakebase.
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from . import config, queries
from .models import AdjustmentIn

app = FastAPI(title="TechMart — Demand Forecast Adjustments")

# In-process mock store of session-submitted overrides (newest-first append order).
_SESSION_OVERRIDES: list[dict] = []


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "app": bool(config.IS_DATABRICKS_APP)}


@app.get("/api/filters")
def filters(category: str | None = Query(default=None)) -> dict:
    return {
        "categories": queries.fetch_categories(),
        "products": queries.fetch_products(category),
        **queries.fetch_week_bounds(),
        "reasons": config.REASONS,
        "planners": config.PLANNERS,
    }


@app.get("/api/forecast")
def forecast(
    level: str = Query(..., pattern="^(category|product)$"),
    id: str = Query(...),
    fy_start: int = Query(...),
    fw_start: int = Query(...),
    fy_end: int = Query(...),
    fw_end: int = Query(...),
    version: str = Query("improved", pattern="^(improved|baseline|both)$"),
) -> dict:
    series = queries.fetch_forecast(
        level=level,
        id_=id,
        fy_start=fy_start,
        fw_start=fw_start,
        fy_end=fy_end,
        fw_end=fw_end,
        version=version,
    )
    # Summary: for "both", summarize the improved series (or baseline if improved absent).
    summary_version = "improved" if version in ("improved", "both") else "baseline"
    summ_rows = [r for r in series if r["version"] == summary_version]
    if not summ_rows:
        summ_rows = series
    total_qty = sum(r["forecast_qty"] or 0 for r in summ_rows)
    total_amount = sum(r["forecast_amount"] or 0 for r in summ_rows)
    n_weeks = len({(r["fiscal_year"], r["fiscal_week"]) for r in summ_rows}) or 1
    return {
        "series": series,
        "summary": {
            "total_qty": total_qty,
            "total_amount": total_amount,
            "avg_weekly_qty": total_qty / n_weeks,
        },
    }


@app.get("/api/adjustments")
def get_adjustments(limit: int = Query(default=100, ge=1, le=1000)) -> dict:
    existing = queries.fetch_overrides(limit=limit)
    # Session rows are newest-first already (we prepend on insert); union newest-first.
    combined = list(_SESSION_OVERRIDES) + existing
    return {"rows": combined[:limit], "session_count": len(_SESSION_OVERRIDES)}


@app.post("/api/adjustments")
def post_adjustment(body: AdjustmentIn) -> dict:
    """MOCK write: validate, synthesize id + timestamps, prepend to session store."""
    product_name, category_name = queries.fetch_product_label(body.product_sk)
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "override_id": f"session-{int(time.time()*1000)}-{random.randint(1000,9999)}",
        "product_sk": body.product_sk,
        "product_name": product_name,
        "category_name": category_name,
        "store_sk": body.store_sk,
        "fiscal_year": body.fiscal_year,
        "fiscal_week": body.fiscal_week,
        "ai_forecast_qty": body.ai_forecast_qty,
        "override_qty": body.override_qty,
        "override_reason": body.override_reason,
        "planner_id": body.planner_id,
        "created_at": now,
        "source": "session",
    }
    _SESSION_OVERRIDES.insert(0, row)
    return {"status": "success", "row": row}


# --- Static SPA (built frontend) -------------------------------------------
_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
if _DIST.exists():
    app.mount(
        "/assets", StaticFiles(directory=_DIST / "assets"), name="assets"
    )

    @app.get("/{full_path:path}")
    def serve_spa(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        candidate = _DIST / full_path
        if full_path and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(_DIST / "index.html")
