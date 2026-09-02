"""Build the Techmart merch exec Lakeview dashboard (.lvdash.json).

Pure Python / stdlib only — no Spark, no network calls.

Widget shapes follow the real exported Lakeview schema.  Helper builders
(_counter, _bar, _line, _pivot, _table, _filter) return one layout-item dict
each; build_dashboard() assembles the full lvdash payload.
"""
from __future__ import annotations

import json
from pathlib import Path

from techmart.dashboards.datasets import DATASETS, WEEKS_PER_YEAR
from techmart.dashboards.theme import ui_theme

# ── number formats ─────────────────────────────────────────────────────────────

def _fmt_currency() -> dict:
    return {
        "type": "number-currency",
        "currencyCode": "USD",
        "decimalPlaces": {"type": "exact", "places": 1},
        "abbreviation": "compact",
    }


def _fmt_percent() -> dict:
    return {
        "type": "number-percent",
        "decimalPlaces": {"type": "exact", "places": 1},
    }


def _fmt_plain() -> dict:
    return {
        "type": "number-plain",
        "decimalPlaces": {"type": "exact", "places": 1},
        "abbreviation": "compact",
    }


# ── widget builders ────────────────────────────────────────────────────────────

def _counter(
    name: str,
    title: str,
    dataset: str,
    expression: str,
    alias: str,
    fmt: dict,
    x: int,
    y: int,
    width: int = 2,
    height: int = 3,
) -> dict:
    """KPI counter tile."""
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset,
                        "fields": [{"name": alias, "expression": expression}],
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": "counter",
                "encodings": {
                    "value": {
                        "fieldName": alias,
                        "format": fmt,
                        "displayName": title,
                    }
                },
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _bar(
    name: str,
    title: str,
    dataset: str,
    dim_col: str,
    dim_label: str,
    measure_expr: str,
    measure_alias: str,
    measure_label: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> dict:
    """Vertical bar chart — categorical dimension x quantitative measure."""
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset,
                        "fields": [
                            {"name": dim_col, "expression": f"`{dim_col}`"},
                            {"name": measure_alias, "expression": measure_expr},
                        ],
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": "bar",
                "encodings": {
                    "x": {
                        "fieldName": dim_col,
                        "scale": {"type": "categorical"},
                        "displayName": dim_label,
                    },
                    "y": {
                        "fieldName": measure_alias,
                        "scale": {"type": "quantitative"},
                        "displayName": measure_label,
                    },
                },
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _line(
    name: str,
    title: str,
    dataset: str,
    dim_col: str,
    dim_label: str,
    dim_scale: str,
    measure_expr: str,
    measure_alias: str,
    measure_label: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> dict:
    """Line chart — dimension (categorical or temporal) x quantitative measure."""
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset,
                        "fields": [
                            {"name": dim_col, "expression": f"`{dim_col}`"},
                            {"name": measure_alias, "expression": measure_expr},
                        ],
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": "line",
                "encodings": {
                    "x": {
                        "fieldName": dim_col,
                        "scale": {"type": dim_scale},
                        "displayName": dim_label,
                    },
                    "y": {
                        "fieldName": measure_alias,
                        "scale": {"type": "quantitative"},
                        "displayName": measure_label,
                    },
                },
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _pivot(
    name: str,
    title: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> dict:
    """Category performance pivot — bridge dataset, rows division→department→category.

    Ratio value cells use SUM(num)/NULLIF(SUM(den),0) so they are never computed
    by aggregating a precomputed ratio column (Ruling A).
    """
    row_fields = [
        {"name": "division", "expression": "`division`"},
        {"name": "department", "expression": "`department`"},
        {"name": "category", "expression": "`category`"},
    ]
    value_fields = [
        {"name": "net_sales",
         "expression": "SUM(`net_sales`)"},
        {"name": "gross_margin_pct",
         "expression": "SUM(`gross_margin`)/NULLIF(SUM(`net_sales`),0)"},
        {"name": "units",
         "expression": "SUM(`units`)"},
        {"name": "sell_through_pct",
         "expression": "SUM(`units`)/NULLIF(SUM(`units`)+SUM(`on_hand_qty`),0)"},
        {"name": "weeks_of_supply",
         "expression": f"SUM(`on_hand_qty`)*{WEEKS_PER_YEAR}/NULLIF(SUM(`units`),0)"},
        {"name": "gmroi",
         "expression": "SUM(`gross_margin`)/NULLIF(SUM(`on_hand_cost_value`),0)"},
        {"name": "out_of_stock_rate",
         "expression": "AVG(`out_of_stock_rate`)"},
    ]

    enc_rows = [
        {"fieldName": "division", "displayName": "Division"},
        {"fieldName": "department", "displayName": "Department"},
        {"fieldName": "category", "displayName": "Category"},
    ]
    enc_values = [
        {"fieldName": "net_sales", "displayName": "Net Sales",
         "format": _fmt_currency()},
        {"fieldName": "gross_margin_pct", "displayName": "GM %",
         "format": _fmt_percent()},
        {"fieldName": "units", "displayName": "Units",
         "format": _fmt_plain()},
        {"fieldName": "sell_through_pct", "displayName": "Sell-Through %",
         "format": _fmt_percent()},
        {"fieldName": "weeks_of_supply", "displayName": "Weeks of Supply",
         "format": _fmt_plain()},
        {"fieldName": "gmroi", "displayName": "GMROI",
         "format": _fmt_plain()},
        {"fieldName": "out_of_stock_rate", "displayName": "OOS Rate",
         "format": _fmt_percent()},
    ]

    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": "bridge",
                        "fields": row_fields + value_fields,
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": "pivot",
                "encodings": {
                    "rows": enc_rows,
                    "values": enc_values,
                },
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _table(
    name: str,
    title: str,
    dataset: str,
    columns: list[tuple[str, str]],
    x: int,
    y: int,
    width: int,
    height: int,
    rows_per_page: int = 25,
) -> dict:
    """Simple data table widget.

    *columns* is a list of (col_name, display_name) pairs; each column binds the
    bare backtick-quoted column expression.
    """
    fields = [{"name": col, "expression": f"`{col}`"} for col, _ in columns]
    enc_columns = [{"fieldName": col, "displayName": label} for col, label in columns]

    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset,
                        "fields": fields,
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": "table",
                "rowsPerPage": rows_per_page,
                "encodings": {
                    "columns": enc_columns,
                },
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _filter(
    name: str,
    title: str,
    dataset: str,
    col: str,
    widget_type: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> dict:
    """Filter widget — single-select or multi-select.

    *widget_type* must be "filter-single-select" or "filter-multi-select".
    """
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset,
                        "fields": [{"name": col, "expression": f"`{col}`"}],
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": widget_type,
                "encodings": {
                    "fields": [
                        {
                            "fieldName": col,
                            "displayName": title,
                            "queryName": "main_query",
                        }
                    ],
                },
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _ai_takeaways(name: str, x: int, y: int, width: int, height: int) -> dict:
    """AI Key Takeaways widget — table bound to ai_takeaways dataset."""
    return _table(
        name=name,
        title="AI Key Takeaways",
        dataset="ai_takeaways",
        columns=[("takeaways", "Key Takeaways")],
        x=x,
        y=y,
        width=width,
        height=height,
    )


# ── dashboard assembly ─────────────────────────────────────────────────────────

def build_dashboard() -> dict:
    """Return the complete merch exec Lakeview dashboard as a plain dict.

    Layout (12-column canvas):
      y=0  h=2   filter bar   (fiscal_year, division, category, region, channel_type)
      y=2  h=3   KPI row      (6 counters, width=2 each)
      y=5  h=4   AI takeaways (width=12)
      y=9  h=6   sales charts (trend line + mix bar, each width=6)
      y=15 h=8   category pivot (width=12)
      y=23 h=6   inventory charts (value bar + OOS bar, each width=6)
      y=29 h=7   lost-sales table (width=12)
    """
    # ── datasets ──────────────────────────────────────────────────────────────
    datasets = [
        {
            "name": ds.name,
            "displayName": ds.display_name,
            "queryLines": ds.query_lines,
        }
        for ds in DATASETS
    ]

    layout: list[dict] = []

    # ── filter bar (y=0, h=2) ─────────────────────────────────────────────────
    # fiscal_year: single-select; division/category/region → bridge;
    # channel_type → sales (not present in bridge)
    layout.append(_filter(
        "filter_fiscal_year", "Fiscal Year", "sales", "fiscal_year",
        "filter-single-select", x=0, y=0, width=2, height=2,
    ))
    layout.append(_filter(
        "filter_division", "Division", "bridge", "division",
        "filter-multi-select", x=2, y=0, width=2, height=2,
    ))
    layout.append(_filter(
        "filter_category", "Category", "bridge", "category",
        "filter-multi-select", x=4, y=0, width=3, height=2,
    ))
    layout.append(_filter(
        "filter_region", "Region", "bridge", "region",
        "filter-multi-select", x=7, y=0, width=3, height=2,
    ))
    layout.append(_filter(
        "filter_channel_type", "Channel Type", "sales", "channel_type",
        "filter-multi-select", x=10, y=0, width=2, height=2,
    ))

    # ── KPI counters (y=2, h=3, width=2 each) ─────────────────────────────────
    # All bound to bridge; ratios computed from base columns (Ruling A).
    layout.append(_counter(
        "kpi_net_sales", "Net Sales", "bridge",
        "SUM(`net_sales`)", "net_sales", _fmt_currency(),
        x=0, y=2,
    ))
    layout.append(_counter(
        "kpi_gross_margin_pct", "Gross Margin %", "bridge",
        "SUM(`gross_margin`)/NULLIF(SUM(`net_sales`),0)", "gross_margin_pct",
        _fmt_percent(), x=2, y=2,
    ))
    layout.append(_counter(
        "kpi_units", "Units", "bridge",
        "SUM(`units`)", "units", _fmt_plain(),
        x=4, y=2,
    ))
    layout.append(_counter(
        "kpi_sell_through_pct", "Sell-Through %", "bridge",
        "SUM(`units`)/NULLIF(SUM(`units`)+SUM(`on_hand_qty`),0)", "sell_through_pct",
        _fmt_percent(), x=6, y=2,
    ))
    layout.append(_counter(
        "kpi_weeks_of_supply", "Weeks of Supply", "bridge",
        f"SUM(`on_hand_qty`)*{WEEKS_PER_YEAR}/NULLIF(SUM(`units`),0)", "weeks_of_supply",
        _fmt_plain(), x=8, y=2,
    ))
    layout.append(_counter(
        "kpi_gmroi", "GMROI", "bridge",
        "SUM(`gross_margin`)/NULLIF(SUM(`on_hand_cost_value`),0)", "gmroi",
        _fmt_plain(), x=10, y=2,
    ))

    # ── AI takeaways (y=5, h=4, width=12) ────────────────────────────────────
    layout.append(_ai_takeaways("ai_takeaways_widget", x=0, y=5, width=12, height=4))

    # ── sales charts (y=9, h=6) ──────────────────────────────────────────────
    _sales_trend = _line(
        "chart_sales_trend", "Net Sales Trend", "sales",
        "fiscal_period", "Fiscal Period", "categorical",
        "SUM(`net_sales`)", "net_sales", "Net Sales",
        x=0, y=9, width=6, height=6,
    )
    # Fix 4: add fiscal_year as categorical color series for year-over-year comparison.
    _sales_trend["widget"]["queries"][0]["query"]["fields"].append(
        {"name": "fiscal_year", "expression": "`fiscal_year`"}
    )
    _sales_trend["widget"]["spec"]["encodings"]["color"] = {
        "fieldName": "fiscal_year",
        "scale": {"type": "categorical"},
        "displayName": "Fiscal Year",
    }
    layout.append(_sales_trend)
    layout.append(_bar(
        "chart_sales_mix", "Net Sales by Department", "sales",
        "department", "Department",
        "SUM(`net_sales`)", "net_sales", "Net Sales",
        x=6, y=9, width=6, height=6,
    ))

    # ── category performance pivot (y=15, h=8, width=12) ─────────────────────
    layout.append(_pivot(
        "pivot_category_matrix", "Category Performance Matrix",
        x=0, y=15, width=12, height=8,
    ))

    # ── inventory charts (y=23, h=6) ─────────────────────────────────────────
    layout.append(_bar(
        "chart_inventory_value", "Inventory Value by Category", "inventory",
        "category", "Category",
        "SUM(`on_hand_cost_value`)", "on_hand_cost_value", "On-Hand Cost Value",
        x=0, y=23, width=6, height=6,
    ))
    layout.append(_bar(
        "chart_oos_by_region", "Out-of-Stock Rate by Region", "inventory",
        "region", "Region",
        "AVG(`out_of_stock_rate`)", "out_of_stock_rate", "OOS Rate",
        x=6, y=23, width=6, height=6,
    ))

    # ── lost-sales table (y=29, h=7, width=12) ───────────────────────────────
    # Dataset is category-grain (mv_inventory has no sub-category).
    layout.append(_table(
        "table_lost_sales", "Lost-Sales Risk (by category)", "lost_sales",
        columns=[
            ("division", "Division"),
            ("department", "Department"),
            ("category", "Category"),
            ("units", "Units"),
            ("net_sales", "Net Sales"),
            ("out_of_stock_rate", "OOS Rate"),
        ],
        x=0, y=29, width=12, height=7,
    ))

    # ── assemble ──────────────────────────────────────────────────────────────
    return {
        "datasets": datasets,
        "pages": [
            {
                "name": "merch_exec",
                "displayName": "Merch Executive",
                "layout": layout,
            }
        ],
        "uiSettings": {"theme": ui_theme()},
    }


def write_dashboard(path: str) -> None:
    """Write build_dashboard() as indented JSON to *path*."""
    data = build_dashboard()
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
