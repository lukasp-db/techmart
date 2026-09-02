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
    columns: list[tuple],
    x: int,
    y: int,
    width: int,
    height: int,
    rows_per_page: int = 25,
) -> dict:
    """Simple data table widget.

    *columns* is a list of (col_name, display_name) or
    (col_name, display_name, fmt_dict | None) tuples.  Each column binds the
    bare backtick-quoted column expression.  An optional third element adds a
    Lakeview format object to the encoding.
    """
    fields = [{"name": col_spec[0], "expression": f"`{col_spec[0]}`"} for col_spec in columns]
    enc_columns = []
    for col_spec in columns:
        col, label = col_spec[0], col_spec[1]
        entry: dict = {"fieldName": col, "displayName": label}
        if len(col_spec) > 2 and col_spec[2] is not None:
            entry["format"] = col_spec[2]
        enc_columns.append(entry)

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
    datasets_cols: list[tuple[str, str]],
    widget_type: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> dict:
    """Filter widget — single-select or multi-select, optionally spanning multiple datasets.

    *datasets_cols* is a list of (dataset_name, col_name) pairs.  One query is
    emitted per pair so Lakeview cross-dataset filtering works; encodings.fields
    also has one entry per pair pointing at the matching query.

    *widget_type* must be "filter-single-select" or "filter-multi-select".
    """
    queries = [
        {
            "name": f"{ds}_q",
            "query": {
                "datasetName": ds,
                "fields": [{"name": col, "expression": f"`{col}`"}],
                "disaggregated": False,
            },
        }
        for ds, col in datasets_cols
    ]
    enc_fields = [
        {
            "fieldName": col,
            "displayName": title,
            "queryName": f"{ds}_q",
        }
        for ds, col in datasets_cols
    ]
    return {
        "widget": {
            "name": name,
            "queries": queries,
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": widget_type,
                "encodings": {
                    "fields": enc_fields,
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

def _pack_query(lines: list[str]) -> list[str]:
    """Package dataset SQL as Lakeview ``queryLines``.

    Lakeview concatenates ``queryLines`` WITHOUT inserting separators, so an
    array of bare per-line strings merges tokens across breaks (``mv_sales`` +
    ``GROUP``) and — worse — a leading ``--`` line comment comments out the rest
    of the query on the single joined line, leaving the dataset blank and every
    visualization empty. We therefore emit a SINGLE element holding the SQL
    joined with real newline characters, with comment-only lines dropped.
    """
    sql = "\n".join(line for line in lines if not line.lstrip().startswith("--"))
    return [sql]


def build_dashboard() -> dict:
    """Return the complete merch exec Lakeview dashboard as a plain dict.

    Layout (12-column canvas):
      y=0  h=2   filter bar   (fiscal_year, division, category, region, channel_type)
      y=2  h=3   KPI row      (6 counters, width=2 each)
      y=5  h=4   AI takeaways (width=12)
      y=9  h=6   sales charts (trend line + mix bar, each width=6)
      y=15 h=5   category ranking table — indexed vs. chain (width=12)
      y=20 h=3   top categories by sell-through index bar (width=12)
      y=23 h=6   inventory charts (value bar + OOS bar, each width=6)
      y=29 h=7   lost-sales table (width=12)
    """
    # ── datasets ──────────────────────────────────────────────────────────────
    datasets = [
        {
            "name": ds.name,
            "displayName": ds.display_name,
            "queryLines": _pack_query(ds.query_lines),
        }
        for ds in DATASETS
    ]

    layout: list[dict] = []

    # ── filter bar (y=0, h=2) ─────────────────────────────────────────────────
    # fiscal_year: single-select → sales only (bridge has no fiscal_year column).
    # division/category: cross-dataset → sales, inventory, bridge, lost_sales.
    # region: cross-dataset → sales, inventory (bridge is now category grain, no region).
    # channel_type: sales only.
    layout.append(_filter(
        "filter_fiscal_year", "Fiscal Year",
        [("sales", "fiscal_year")],
        "filter-single-select", x=0, y=0, width=2, height=2,
    ))
    layout.append(_filter(
        "filter_division", "Division",
        [("sales", "division"), ("inventory", "division"),
         ("bridge", "division"), ("lost_sales", "division")],
        "filter-multi-select", x=2, y=0, width=2, height=2,
    ))
    layout.append(_filter(
        "filter_category", "Category",
        [("sales", "category"), ("inventory", "category"),
         ("bridge", "category"), ("lost_sales", "category")],
        "filter-multi-select", x=4, y=0, width=3, height=2,
    ))
    layout.append(_filter(
        "filter_region", "Region",
        [("sales", "region"), ("inventory", "region")],
        "filter-multi-select", x=7, y=0, width=3, height=2,
    ))
    layout.append(_filter(
        "filter_channel_type", "Channel Type",
        [("sales", "channel_type")],
        "filter-multi-select", x=10, y=0, width=2, height=2,
    ))

    # ── KPI counters (y=2, h=3, width=2 each) ─────────────────────────────────
    # Realistic absolutes: first three from sales, last three from inventory.
    # Cross-fact ratios (WOS/GMROI/sell-through) are not credible as absolutes
    # on this dataset — they are shown as indices in the category ranking table.
    layout.append(_counter(
        "kpi_net_sales", "Net Sales", "sales",
        "SUM(`net_sales`)", "net_sales", _fmt_currency(),
        x=0, y=2,
    ))
    layout.append(_counter(
        "kpi_gross_margin_pct", "Gross Margin %", "sales",
        "SUM(`gross_margin`)/NULLIF(SUM(`net_sales`),0)", "gross_margin_pct",
        _fmt_percent(), x=2, y=2,
    ))
    layout.append(_counter(
        "kpi_units", "Units", "sales",
        "SUM(`units`)", "units", _fmt_plain(),
        x=4, y=2,
    ))
    layout.append(_counter(
        "kpi_avg_days_of_supply", "Avg Days of Supply", "inventory",
        "AVG(`avg_days_of_supply`)", "avg_days_of_supply", _fmt_plain(),
        x=6, y=2,
    ))
    layout.append(_counter(
        "kpi_on_hand_value", "On-Hand Value", "inventory",
        "SUM(`on_hand_cost_value`)", "on_hand_cost_value", _fmt_currency(),
        x=8, y=2,
    ))
    layout.append(_counter(
        "kpi_oos_rate", "Out-of-Stock Rate", "inventory",
        "AVG(`out_of_stock_rate`)", "out_of_stock_rate", _fmt_percent(),
        x=10, y=2,
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

    # ── category ranking table (y=15, h=5, width=12) ─────────────────────────
    # Bare-column expressions — bridge is one row per category so no aggregation.
    layout.append(_table(
        "table_category_ranking",
        "Category Performance — indexed vs. chain (100 = avg)",
        "bridge",
        columns=[
            ("category", "Category"),
            ("net_sales", "Net Sales", _fmt_currency()),
            ("gross_margin_pct", "GM %", _fmt_percent()),
            ("units", "Units", _fmt_plain()),
            ("sell_through_index", "ST Index", _fmt_plain()),
            ("inventory_efficiency_index", "Inv Eff Index", _fmt_plain()),
            ("gmroi_index", "GMROI Index", _fmt_plain()),
        ],
        x=0, y=15, width=12, height=5,
        rows_per_page=25,
    ))

    # ── top categories by sell-through index bar (y=20, h=3, width=12) ──────
    layout.append(_bar(
        "chart_sell_through_index", "Top Categories by Sell-Through Index", "bridge",
        "category", "Category",
        "SUM(`sell_through_index`)", "sell_through_index", "Sell-Through Index",
        x=0, y=20, width=12, height=3,
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
