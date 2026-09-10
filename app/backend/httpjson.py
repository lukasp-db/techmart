"""Minimal JSON-over-HTTPS helpers using only the standard library (urllib).

Avoids `requests` so the deployed app pulls no certifi/charset-normalizer/urllib3
wheels (certifi's wheel is unreliable on the Apps build pypi proxy). Uses the system
CA store by default; falls back to certifi's bundle locally if it happens to be
installed (dev only).
"""
from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

try:  # certifi is a local/dev-only dep; not present in the deployed app.
    import certifi

    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL_CTX = ssl.create_default_context()


def _read(req: urllib.request.Request, timeout: float) -> Any:
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post_form(url: str, form: dict, headers: dict, timeout: float = 30) -> Any:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    return _read(req, timeout)


def post_json(url: str, body: dict, headers: dict, timeout: float = 90) -> Any:
    data = json.dumps(body).encode("utf-8")
    h = {"Content-Type": "application/json", **headers}
    req = urllib.request.Request(url, data=data, headers=h, method="POST")
    return _read(req, timeout)


def get_json(url: str, headers: dict, timeout: float = 30) -> Any:
    req = urllib.request.Request(url, headers=headers, method="GET")
    return _read(req, timeout)
