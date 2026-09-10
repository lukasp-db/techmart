"""Starlette transport for the TechMart Forecast Adjustments app.

FastAPI is not deployable on this workspace's Databricks Apps build proxy: it
requires the compiled `pydantic-core` wheel, which the build pip proxy
connect-fails to fetch (same failure as numpy and cryptography — the proxy cannot
serve compiled manylinux x86_64 wheels). Starlette is a pure-Python ASGI framework
(no pydantic-core), so it installs cleanly while still giving us a real web
framework (routing, request/response, ASGI) rather than the stdlib http.server.

The endpoint logic lives in the framework-agnostic `backend.api` (validation +
in-memory session store); data access is stdlib-urllib REST (`backend.db`). GET
handlers are sync `def` — Starlette runs them in a threadpool, which suits the
blocking urllib calls; the POST handler is async only to read the JSON body.
"""
from __future__ import annotations

import os
from pathlib import Path

from starlette.applications import Starlette
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from . import api, config

_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
_INDEX = _DIST / "index.html"


def _json(result: tuple[int, dict]) -> JSONResponse:
    status, body = result
    return JSONResponse(body, status_code=status)


# ------------------------------------------------------------ routes ----
def health(request) -> JSONResponse:
    return JSONResponse({"status": "ok", "app": bool(config.IS_DATABRICKS_APP)})


def filters(request) -> JSONResponse:
    return _json(api.filters(request.query_params.get("category") or None))


def forecast(request) -> JSONResponse:
    q = request.query_params
    params = {
        k: q.get(k)
        for k in ("level", "id", "fy_start", "fw_start", "fy_end", "fw_end", "version")
    }
    if not params.get("version"):
        params["version"] = "improved"
    return _json(api.forecast(params))


def get_adjustments(request) -> JSONResponse:
    return _json(api.get_adjustments(request.query_params.get("limit")))


async def post_adjustment(request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        body = {}
    return _json(api.post_adjustment(body if isinstance(body, dict) else {}))


def serve_spa(request) -> FileResponse | JSONResponse:
    full_path = request.path_params.get("path", "")
    if full_path.startswith("api/"):
        return JSONResponse({"error": "not found"}, status_code=404)
    if full_path:
        candidate = (_DIST / full_path).resolve()
        try:
            candidate.relative_to(_DIST.resolve())
            if candidate.is_file():
                return FileResponse(candidate)
        except (ValueError, OSError):
            pass
    if _INDEX.is_file():
        return FileResponse(_INDEX)
    return JSONResponse({"error": "not found"}, status_code=404)


routes = [
    Route("/api/health", health),
    Route("/api/filters", filters),
    Route("/api/forecast", forecast),
    Route("/api/adjustments", get_adjustments, methods=["GET"]),
    Route("/api/adjustments", post_adjustment, methods=["POST"]),
]
# Serve built SPA assets (JS/CSS) before the catch-all (order matters in Starlette).
if (_DIST / "assets").is_dir():
    routes.append(Mount("/assets", app=StaticFiles(directory=str(_DIST / "assets")), name="assets"))
# Catch-all: serve index.html / SPA fallback. Must be last.
routes.append(Route("/{path:path}", serve_spa))

app = Starlette(routes=routes)


def main() -> None:
    import uvicorn

    port = int(os.environ.get("PORT") or os.environ.get("DATABRICKS_APP_PORT") or "8000")
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
