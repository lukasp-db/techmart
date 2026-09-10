"""Backend unit tests — no live warehouse (queries.run_query is monkeypatched).

Tests call the framework-agnostic API functions in backend.api directly.
"""
from __future__ import annotations

import pytest

from backend import api, queries


@pytest.fixture(autouse=True)
def _reset_session():
    api._SESSION_OVERRIDES.clear()
    yield
    api._SESSION_OVERRIDES.clear()


@pytest.fixture(autouse=True)
def _stub_sql(monkeypatch):
    def fake_run_query(query, params=None):
        q = query.lower()
        if "left join" in q and "override" in q:  # fetch_overrides
            return []
        if "min(fiscal_year" in q:  # week bounds
            return [{"lo": 202501, "hi": 202552}]
        if "distinct p.category_name" in q:  # categories
            return [{"category_name": "Smartphones"}, {"category_name": "Tablets"}]
        if "distinct p.product_sk" in q:  # products
            return [
                {"product_sk": 1, "category_name": "Smartphones", "product_name": "Phone A"}
            ]
        if "where product_sk" in q:  # product label
            return [{"product_name": "Phone A", "category_name": "Smartphones"}]
        return []

    monkeypatch.setattr(queries, "run_query", fake_run_query)


def test_filters():
    status, body = api.filters(None)
    assert status == 200
    assert body["categories"] == ["Smartphones", "Tablets"]
    assert body["week_min"] == {"fiscal_year": 2025, "fiscal_week": 1}
    assert body["reasons"] and body["planners"]


def test_post_valid_prepends_and_unions():
    payload = {
        "product_sk": 1, "store_sk": 10, "fiscal_year": 2025, "fiscal_week": 5,
        "ai_forecast_qty": 12.0, "override_qty": 15.0,
        "override_reason": "Local promotion", "planner_id": "planner_amir",
    }
    status, resp = api.post_adjustment(payload)
    assert status == 200
    row = resp["row"]
    assert row["source"] == "session" and row["override_id"].startswith("session-")
    assert row["product_name"] == "Phone A"

    _, g = api.get_adjustments(None)
    assert g["session_count"] == 1
    assert g["rows"][0]["override_id"] == row["override_id"]  # newest-first


@pytest.mark.parametrize("field,value", [
    ("override_reason", "Bogus reason"),
    ("planner_id", "planner_zzz"),
    ("override_qty", -3.0),
])
def test_post_rejects_invalid(field, value):
    payload = {
        "product_sk": 1, "store_sk": 10, "fiscal_year": 2025, "fiscal_week": 5,
        "ai_forecast_qty": 12.0, "override_qty": 15.0,
        "override_reason": "Local promotion", "planner_id": "planner_amir",
    }
    payload[field] = value
    status, _ = api.post_adjustment(payload)
    assert status == 422


def test_forecast_query_is_parameterized(monkeypatch):
    """level/version branching binds named params; user input never interpolated."""
    captured = {}

    def spy(query, params=None):
        captured["query"], captured["params"] = query, params
        return [
            {"fiscal_year": 2025, "fiscal_week": 1, "week_end_date": "2025-02-08",
             "version": "improved", "forecast_qty": 10.0, "forecast_amount": 100.0,
             "lower_bound": 8.0, "upper_bound": 12.0},
        ]

    monkeypatch.setattr(queries, "run_query", spy)
    rows = queries.fetch_forecast("category", "Smartphones", 2025, 1, 2025, 8, "improved")
    assert captured["params"]["id"] == "Smartphones"
    assert captured["params"]["lo"] == 202501 and captured["params"]["hi"] == 202508
    assert captured["params"]["ver"] == "improved"
    assert "Smartphones" not in captured["query"]  # value not interpolated
    assert rows[0]["forecast_qty"] == 10.0

    queries.fetch_forecast("product", "42", 2025, 1, 2025, 4, "both")
    assert captured["params"]["id"] == 42
    assert "ver" not in captured["params"]
