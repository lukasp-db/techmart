"""FastAPI transport for the TechMart Forecast Adjustments app.

Wraps the framework-agnostic handlers in `backend.api` (validation + in-memory
session store unchanged) and serves the built React SPA. Data access runs through
the SQL Statement Execution REST API over stdlib urllib (backend/db.py) — no
connector or SDK at runtime. Route handlers are sync `def`: the urllib calls are
blocking, and FastAPI runs sync endpoints in a threadpool.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import api, config

_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
_INDEX = _DIST / "index.html"

app = FastAPI(title="TechMart — Demand Forecast Adjustments")


# ------------------------------------------------------------ models ----
class WeekRef(BaseModel):
    fiscal_year: int
    fiscal_week: int


class ProductOpt(BaseModel):
    product_sk: int
    label: str


class FiltersResp(BaseModel):
    categories: list[str]
    products: list[ProductOpt]
    week_min: WeekRef
    week_max: WeekRef
    reasons: list[str]
    planners: list[str]


class SeriesPoint(BaseModel):
    fiscal_year: int
    fiscal_week: int
    week_end_date: Optional[str] = None
    version: str
    forecast_qty: Optional[float] = None
    forecast_amount: Optional[float] = None
    lower_bound: Optional[float] = None
    upper_bound: Optional[float] = None


class ForecastSummary(BaseModel):
    total_qty: float
    total_amount: float
    avg_weekly_qty: float


class ForecastResp(BaseModel):
    series: list[SeriesPoint]
    summary: ForecastSummary


class AdjustmentRow(BaseModel):
    override_id: str
    product_sk: int
    product_name: Optional[str] = None
    category_name: Optional[str] = None
    store_sk: int
    fiscal_year: int
    fiscal_week: int
    ai_forecast_qty: Optional[float] = None
    override_qty: Optional[float] = None
    override_reason: str
    planner_id: str
    created_at: Optional[str] = None
    source: str


class AdjustmentsResp(BaseModel):
    rows: list[AdjustmentRow]
    session_count: int


class AdjustmentIn(BaseModel):
    product_sk: int
    store_sk: int
    fiscal_year: int
    fiscal_week: int
    ai_forecast_qty: float
    override_qty: float
    override_reason: str
    planner_id: str


class PostResp(BaseModel):
    status: str
    row: AdjustmentRow


class Health(BaseModel):
    status: str
    app: bool


# ------------------------------------------------------------ routes ----
def _unwrap(result: tuple[int, dict], ok: int = 200) -> dict:
    status, body = result
    if status != ok:
        raise HTTPException(status_code=status, detail=body.get("error", "error"))
    return body


@app.get("/api/health", response_model=Health)
def health() -> dict:
    return {"status": "ok", "app": bool(config.IS_DATABRICKS_APP)}


@app.get("/api/filters", response_model=FiltersResp)
def filters(category: Optional[str] = None) -> dict:
    return _unwrap(api.filters(category or None))


@app.get("/api/forecast", response_model=ForecastResp)
def forecast(
    level: str,
    id: str,
    fy_start: str,
    fw_start: str,
    fy_end: str,
    fw_end: str,
    version: str = "improved",
) -> dict:
    params = {
        "level": level, "id": id, "version": version,
        "fy_start": fy_start, "fw_start": fw_start,
        "fy_end": fy_end, "fw_end": fw_end,
    }
    return _unwrap(api.forecast(params))


@app.get("/api/adjustments", response_model=AdjustmentsResp)
def get_adjustments(limit: Optional[str] = None) -> dict:
    return _unwrap(api.get_adjustments(limit))


@app.post("/api/adjustments", response_model=PostResp)
def post_adjustment(body: AdjustmentIn) -> dict:
    return _unwrap(api.post_adjustment(body.model_dump()))


# --------------------------------------------------------- static SPA ----
if (_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=_DIST / "assets"), name="assets")


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    # Never let the SPA fallback shadow unmatched API routes.
    if full_path.startswith("api/"):
        return JSONResponse(status_code=404, content={"error": "not found"})
    candidate = (_DIST / full_path).resolve() if full_path else _INDEX
    try:
        candidate.relative_to(_DIST.resolve())
        if candidate.is_file():
            return FileResponse(candidate)
    except (ValueError, OSError):
        pass
    if _INDEX.is_file():
        return FileResponse(_INDEX)
    return JSONResponse(status_code=404, content={"error": "not found"})


def main() -> None:
    import uvicorn

    port = int(os.environ.get("PORT") or os.environ.get("DATABRICKS_APP_PORT") or "8000")
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
