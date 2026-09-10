"""Standard-library HTTP server for the Databricks App (no pip dependencies).

Uses only the Python standard library (http.server) so the deployed app installs
zero packages — the Apps build pypi proxy is unreliable, so anything to download is
a liability. Serves the JSON API under /api/* and the built React SPA for everything
else. Data access (backend/db.py) uses urllib against the SQL warehouse REST API.
"""
from __future__ import annotations

import json
import mimetypes
import os
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import api, config

_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
_INDEX = _DIST / "index.html"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # --- helpers ---------------------------------------------------------
    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, path: str) -> None:
        # Map the request path to a file under dist; fall back to index.html (SPA).
        rel = path.lstrip("/")
        candidate = (_DIST / rel) if rel else _INDEX
        try:
            candidate = candidate.resolve()
            candidate.relative_to(_DIST.resolve())  # prevent path traversal
            if not candidate.is_file():
                raise FileNotFoundError
        except (FileNotFoundError, ValueError):
            candidate = _INDEX
        if not candidate.is_file():
            return self._send_json(404, {"error": "not found"})
        ctype = mimetypes.guess_type(str(candidate))[0] or "application/octet-stream"
        self._send_bytes(200, candidate.read_bytes(), ctype)

    # --- routing ---------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        try:
            if path == "/api/health":
                return self._send_json(200, {"status": "ok", "app": bool(config.IS_DATABRICKS_APP)})
            if path == "/api/filters":
                return self._send_json(*api.filters(qs.get("category")))
            if path == "/api/forecast":
                return self._send_json(*api.forecast(qs))
            if path == "/api/adjustments":
                return self._send_json(*api.get_adjustments(qs.get("limit")))
            if path.startswith("/api/"):
                return self._send_json(404, {"error": "not found"})
            return self._serve_static(path)
        except Exception as exc:  # keep the server alive; report the error
            traceback.print_exc()
            return self._send_json(500, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/adjustments":
            return self._send_json(404, {"error": "not found"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            body = json.loads(raw.decode("utf-8") or "{}")
        except (ValueError, json.JSONDecodeError):
            return self._send_json(400, {"error": "invalid JSON"})
        try:
            return self._send_json(*api.post_adjustment(body))
        except Exception as exc:
            traceback.print_exc()
            return self._send_json(500, {"error": str(exc)})

    def log_message(self, fmt: str, *args) -> None:  # concise access log
        print("[http] " + (fmt % args))


def main() -> None:
    port = int(os.environ.get("PORT") or os.environ.get("DATABRICKS_APP_PORT") or "8000")
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[server] listening on 0.0.0.0:{port} (dist present: {_DIST.exists()})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
