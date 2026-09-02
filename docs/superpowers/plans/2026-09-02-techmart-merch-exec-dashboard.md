# Techmart Merchandising Executive Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate, from typed Python, a governed AI/BI dashboard (`merch_exec.lvdash.json`) for a merchandising executive — sales performance ⋈ inventory position with the full open-to-buy hero metric set — deployed by the DAB and validated on the live showcase data.

**Architecture:** Pure-Python builders (`src/techmart/dashboards/`) assemble the Lakeview `.lvdash.json` dict: `theme.py` emits the `uiSettings.theme` block (California Sunset palette), `datasets.py` emits each dataset's `queryLines` (MEASURE() queries over `mv_sales`/`mv_inventory` + the cross-fact `bridge` join), and `build.py` composes datasets + widgets + theme into one page and writes the committed `dashboards/merch_exec.lvdash.json`. A DAB `dashboards` resource deploys it, using `dataset_catalog`/`dataset_schema` to inject catalog/schema so dataset SQL stays unqualified. Everything except the final deploy is unit-tested locally with no workspace.

**Tech Stack:** Python 3.11 (stdlib only — dict/JSON assembly, no Spark), pytest, Databricks Asset Bundles, Databricks AI/BI (Lakeview) dashboards, metric views (`MEASURE()`), `ai_query`.

**Spec:** `docs/superpowers/specs/2026-09-02-techmart-merch-exec-dashboard-design.md`

## Global Constraints

- **No workspace calls in `src/` or `tests/`.** The builders are pure functions returning dicts/strings; only Task 5 touches the workspace.
- **Dataset SQL uses UNQUALIFIED table names** (`mv_sales`, `mv_inventory`). Catalog/schema are injected by the DAB resource's `dataset_catalog: ${var.catalog}` and `dataset_schema: ${var.schema_prefix}semantic`. Never hard-code `stable_classic_ppke9o` or a schema in dataset SQL.
- **Metric views are queried with `MEASURE()`**: `SELECT <dims>, MEASURE(<measure>) AS <alias> FROM <mv> GROUP BY ALL`. Measure and dimension names MUST match `src/techmart/semantic/metric_views.py` exactly (verified list in Task 2).
- **Every ratio is `NULLIF(...,0)`-guarded** against divide-by-zero.
- **Stock vs. flow:** inventory stock metrics (`on_hand_qty`, `on_hand_cost_value`) are taken from the **latest snapshot date** (current position), never summed across days; sales metrics are flows over the filtered period. This is the documented modeling choice for the bridge (spec §4.3).
- **Palette (California Sunset), exact hex:** `blue-dark #47527B`, `blue-med #707DA3`, `blue-light #8F9CC1`, `terra #ECA991`, `pink #CC928C`, `violet-dark #78566A`; derived canvas off-white `#FAF7F3`.
- **No new bundle variables.** Reuse `catalog`, `schema_prefix`, `llm_endpoint`; reference the existing `techmart_warehouse` SQL warehouse resource.
- **Branch:** `merch-exec-dashboard`. Commit messages end with `Co-authored-by: Isaac <no-reply@databricks.com>`. Never use `--no-verify` (Databricks git hooks are active).
- **`display_name`** of the dashboard: `Techmart — Merchandising Executive`.

---

## File Structure

- `src/techmart/dashboards/__init__.py` — package marker; re-exports `build_dashboard`.
- `src/techmart/dashboards/theme.py` — palette tokens, semantic role map, `ui_theme()` → `uiSettings.theme` dict.
- `src/techmart/dashboards/datasets.py` — one function per dataset returning `list[str]` (queryLines); a `DATASETS` registry mapping stable dataset name → (displayName, queryLines).
- `src/techmart/dashboards/build.py` — widget helper builders (`_counter`, `_kpi_row`, `_bar`, `_line`, `_pivot`, `_table`, `_filter`, `_markdown`), `build_dashboard() -> dict`, and `write_dashboard(path)`.
- `dashboards/merch_exec.lvdash.json` — generated, committed artifact.
- `resources/dashboards.yml` — DAB `dashboards` resource.
- `tests/test_dashboard_theme.py`, `tests/test_dashboard_datasets.py`, `tests/test_dashboard_build.py` — local unit tests.

---

## Task 1: Dashboards package + California Sunset theme

**Files:**
- Create: `src/techmart/dashboards/__init__.py`
- Create: `src/techmart/dashboards/theme.py`
- Test: `tests/test_dashboard_theme.py`

**Interfaces:**
- Produces: `PALETTE: dict[str,str]` (token→hex), `ROLES: dict[str,str]` (semantic role→token), `ui_theme() -> dict` (the `uiSettings.theme` block: `canvasBackgroundColor`, `widgetBackgroundColor`, `fontColor`, `selectionColor`, `visualizationColors`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dashboard_theme.py
import re
from techmart.dashboards.theme import PALETTE, ROLES, ui_theme

HEX = re.compile(r"^#[0-9A-Fa-f]{6}$")

def test_palette_has_six_dmc_tokens_all_valid_hex():
    assert set(PALETTE) == {"blue-dark", "blue-med", "blue-light", "terra", "pink", "violet-dark"}
    assert all(HEX.match(v) for v in PALETTE.values())
    assert PALETTE["blue-dark"] == "#47527B"

def test_roles_reference_real_tokens():
    for role, token in ROLES.items():
        assert token in PALETTE, f"role {role} -> unknown token {token}"
    # cornflower carries the primary/positive role; antique violet is text
    assert ROLES["primary"] == "blue-dark"
    assert ROLES["text"] == "violet-dark"

def test_ui_theme_shape_and_ordering():
    t = ui_theme()
    assert HEX.match(t["canvasBackgroundColor"]["light"])
    assert t["canvasBackgroundColor"]["light"] == "#FAF7F3"
    vc = t["visualizationColors"]
    assert vc[0] == PALETTE["blue-dark"]           # primary series first
    assert PALETTE["terra"] in vc and PALETTE["pink"] in vc
    assert all(HEX.match(c) for c in vc)
    assert HEX.match(t["fontColor"]["light"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dashboard_theme.py -v`
Expected: FAIL (module `techmart.dashboards.theme` not found).

- [ ] **Step 3: Write minimal implementation**

```python
# src/techmart/dashboards/__init__.py
"""Techmart AI/BI dashboard builders (Lakeview .lvdash.json generation)."""
from __future__ import annotations
```

```python
# src/techmart/dashboards/theme.py
"""California Sunset design system — shared across all Techmart demo artifacts.

Palette tokens are the canonical DMC floss->RGB values for the referenced
Golden-Gate-sunset swatch. `ui_theme()` emits the Lakeview `uiSettings.theme`
block; the same tokens are reused by the Excel report and apps later.
"""
from __future__ import annotations

# token -> hex (DMC 792/793/794/758/223/3740)
PALETTE: dict[str, str] = {
    "blue-dark": "#47527B",   # DMC 792 Dark Cornflower
    "blue-med": "#707DA3",    # DMC 793 Medium Cornflower
    "blue-light": "#8F9CC1",  # DMC 794 Light Cornflower
    "terra": "#ECA991",       # DMC 758 Very Light Terra Cotta
    "pink": "#CC928C",        # DMC 223 Light Shell Pink
    "violet-dark": "#78566A", # DMC 3740 Dark Antique Violet
}

# semantic role -> token
ROLES: dict[str, str] = {
    "primary": "blue-dark",
    "secondary": "blue-med",
    "tertiary": "blue-light",
    "accent": "terra",
    "warn": "pink",
    "negative": "violet-dark",
    "text": "violet-dark",
    "selection": "blue-dark",
}

_CANVAS_LIGHT = "#FAF7F3"   # warm off-white
_WIDGET_LIGHT = "#FFFFFF"

# Categorical series order: three cornflowers, then the warm accents.
_SERIES_ORDER = ["blue-dark", "blue-med", "blue-light", "terra", "pink", "violet-dark"]


def ui_theme() -> dict:
    """The Lakeview `uiSettings.theme` block for the California Sunset look."""
    return {
        "canvasBackgroundColor": {"light": _CANVAS_LIGHT, "dark": "#241E28"},
        "widgetBackgroundColor": {"light": _WIDGET_LIGHT, "dark": "#2E2733"},
        "fontColor": {"light": PALETTE["violet-dark"], "dark": "#E8E2E6"},
        "selectionColor": {"light": PALETTE[ROLES["selection"]], "dark": PALETTE["blue-light"]},
        "visualizationColors": [PALETTE[t] for t in _SERIES_ORDER],
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_dashboard_theme.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/dashboards/__init__.py src/techmart/dashboards/theme.py tests/test_dashboard_theme.py
git commit -m "Add California Sunset theme for Techmart dashboards

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 2: Dataset SQL builders (metric-view MEASURE queries + the bridge)

**Files:**
- Create: `src/techmart/dashboards/datasets.py`
- Test: `tests/test_dashboard_datasets.py`

**Interfaces:**
- Consumes: nothing (pure SQL string builders).
- Produces: `sales_querylines() -> list[str]`, `inventory_querylines() -> list[str]`, `bridge_querylines() -> list[str]`, `lost_sales_querylines() -> list[str]`, `ai_takeaways_querylines() -> list[str]`, and `DATASETS: tuple[Dataset, ...]` where `Dataset = namedtuple("Dataset", "name display_name query_lines")` with STABLE `name` values: `sales`, `inventory`, `bridge`, `lost_sales`, `ai_takeaways`. Task 3 binds widgets to these `name`s.

**Reference — exact metric-view names (from `src/techmart/semantic/metric_views.py`):**
- `mv_sales` measures: `gross_sales, net_sales, discount, discount_rate, cogs, gross_margin, gross_margin_pct, units, line_count, transaction_count, avg_order_value, avg_basket_units, avg_unit_price`.
- `mv_sales` dims (subset used): `fiscal_year, fiscal_quarter, fiscal_period, selling_season, division, department, category, subcategory, brand, region, district, store_format, channel, channel_type`.
- `mv_inventory` measures: `on_hand_qty, available_qty, on_order_qty, reserved_qty, on_hand_cost_value, on_hand_retail_value, avg_days_of_supply, out_of_stock_rate, sku_count, stocked_store_count`.
- `mv_inventory` dims (subset used): `date, fiscal_year, fiscal_quarter, fiscal_period, division, department, category, region, district, store_format`.

**Grain:** category-level (division→department→category) × region × fiscal period for the aggregate datasets (keeps result cardinality browser-safe at 750M-row scale; subcategory/brand/product are drills for later iterations). `lost_sales` is a top-N product table.

**Modeling notes (encode as comments in the SQL):**
- Inventory stock is read from the **latest snapshot** (`WHERE date = (SELECT MAX(date) FROM mv_inventory)`), never summed across days.
- Weeks-of-supply uses `_WEEKS_PER_PERIOD = 4.333` (retail 4-4-5 period ≈ 4.33 weeks).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dashboard_datasets.py
from techmart.dashboards import datasets as D

def _sql(lines): return "\n".join(lines)

def test_all_datasets_registered_with_stable_names():
    names = {d.name for d in D.DATASETS}
    assert names == {"sales", "inventory", "bridge", "lost_sales", "ai_takeaways"}
    for d in D.DATASETS:
        assert d.query_lines and all(isinstance(x, str) for x in d.query_lines)

def test_dataset_sql_is_unqualified():
    # never hard-code catalog/schema; tables referenced bare
    for d in D.DATASETS:
        s = _sql(d.query_lines)
        assert "stable_classic_ppke9o" not in s
        assert ".semantic." not in s and "techmart_" not in s

def test_sales_uses_measure_and_real_measures():
    s = _sql(D.sales_querylines())
    assert "MEASURE(net_sales)" in s and "MEASURE(gross_margin)" in s and "MEASURE(units)" in s
    assert "FROM mv_sales" in s

def test_inventory_uses_latest_snapshot():
    s = _sql(D.inventory_querylines())
    assert "MAX(date)" in s and "FROM mv_inventory" in s
    assert "MEASURE(on_hand_qty)" in s and "MEASURE(on_hand_cost_value)" in s

def test_bridge_ratios_are_nullif_guarded():
    s = _sql(D.bridge_querylines())
    for ratio in ("sell_through_pct", "weeks_of_supply", "gmroi", "inventory_turns"):
        assert ratio in s
    assert s.count("NULLIF(") >= 4  # one guard per ratio, at least

def test_ai_takeaways_calls_ai_query_with_endpoint_placeholder():
    s = _sql(D.ai_takeaways_querylines())
    assert "ai_query(" in s and ":llm_endpoint" in s  # dashboard param, injected at deploy
    assert "takeaways" in s
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_dashboard_datasets.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

```python
# src/techmart/dashboards/datasets.py
"""Dataset queryLines for the merch exec dashboard.

All queries use UNQUALIFIED metric-view names (mv_sales, mv_inventory); the DAB
`dashboards` resource sets dataset_catalog/dataset_schema so they resolve to
${catalog}.${schema_prefix}semantic at deploy. Metric views are queried with
MEASURE(); every ratio is NULLIF-guarded. Inventory stock is the latest
snapshot (current position), not summed across days.
"""
from __future__ import annotations

from collections import namedtuple

Dataset = namedtuple("Dataset", "name display_name query_lines")

_WEEKS_PER_PERIOD = 4.333  # retail 4-4-5 fiscal period ≈ 4.33 weeks


def sales_querylines() -> list[str]:
    return [
        "-- Sales flow by category x region x fiscal period",
        "SELECT fiscal_year, fiscal_quarter, fiscal_period,",
        "       division, department, category, region, channel_type,",
        "       MEASURE(net_sales)        AS net_sales,",
        "       MEASURE(gross_sales)      AS gross_sales,",
        "       MEASURE(gross_margin)     AS gross_margin,",
        "       MEASURE(gross_margin_pct) AS gross_margin_pct,",
        "       MEASURE(cogs)             AS cogs,",
        "       MEASURE(units)            AS units,",
        "       MEASURE(transaction_count) AS transaction_count,",
        "       MEASURE(discount_rate)    AS discount_rate",
        "FROM mv_sales",
        "GROUP BY ALL",
    ]


def inventory_querylines() -> list[str]:
    return [
        "-- Current inventory position: latest snapshot only (stock, not summed over days)",
        "SELECT division, department, category, region,",
        "       MEASURE(on_hand_qty)          AS on_hand_qty,",
        "       MEASURE(on_hand_cost_value)   AS on_hand_cost_value,",
        "       MEASURE(on_hand_retail_value) AS on_hand_retail_value,",
        "       MEASURE(out_of_stock_rate)    AS out_of_stock_rate,",
        "       MEASURE(avg_days_of_supply)   AS avg_days_of_supply,",
        "       MEASURE(sku_count)            AS sku_count",
        "FROM mv_inventory",
        "WHERE date = (SELECT MAX(date) FROM mv_inventory)",
        "GROUP BY ALL",
    ]


def bridge_querylines() -> list[str]:
    return [
        "-- Cross-fact bridge: period sales flow ⋈ current inventory position.",
        "-- Sell-through/WOS/GMROI/turns require terms from both facts (NULLIF-guarded).",
        "WITH s AS (",
        "  SELECT fiscal_year, fiscal_quarter, fiscal_period, division, department, category, region,",
        "         MEASURE(net_sales) AS net_sales, MEASURE(gross_margin) AS gross_margin,",
        "         MEASURE(gross_margin_pct) AS gross_margin_pct, MEASURE(units) AS units,",
        "         MEASURE(cogs) AS cogs",
        "  FROM mv_sales GROUP BY ALL",
        "),",
        "i AS (",
        "  SELECT division, department, category, region,",
        "         MEASURE(on_hand_qty) AS on_hand_qty, MEASURE(on_hand_cost_value) AS on_hand_cost_value,",
        "         MEASURE(out_of_stock_rate) AS out_of_stock_rate",
        "  FROM mv_inventory",
        "  WHERE date = (SELECT MAX(date) FROM mv_inventory) GROUP BY ALL",
        ")",
        "SELECT s.fiscal_year, s.fiscal_quarter, s.fiscal_period,",
        "       s.division, s.department, s.category, s.region,",
        "       s.net_sales, s.gross_margin, s.gross_margin_pct, s.units, s.cogs,",
        "       i.on_hand_qty, i.on_hand_cost_value, i.out_of_stock_rate,",
        "       s.units / NULLIF(s.units + i.on_hand_qty, 0)                    AS sell_through_pct,",
        f"       (i.on_hand_qty * {_WEEKS_PER_PERIOD}) / NULLIF(s.units, 0)      AS weeks_of_supply,",
        "       s.gross_margin / NULLIF(i.on_hand_cost_value, 0)                AS gmroi,",
        "       s.cogs / NULLIF(i.on_hand_cost_value, 0)                        AS inventory_turns",
        "FROM s LEFT JOIN i USING (division, department, category, region)",
    ]


def lost_sales_querylines() -> list[str]:
    return [
        "-- Lost-sales risk: high out-of-stock AND high velocity, latest period.",
        "WITH s AS (",
        "  SELECT division, department, category, MEASURE(units) AS units, MEASURE(net_sales) AS net_sales",
        "  FROM mv_sales WHERE fiscal_period = (SELECT MAX(fiscal_period) FROM mv_sales) GROUP BY ALL",
        "),",
        "i AS (",
        "  SELECT division, department, category, MEASURE(out_of_stock_rate) AS out_of_stock_rate",
        "  FROM mv_inventory WHERE date = (SELECT MAX(date) FROM mv_inventory) GROUP BY ALL",
        ")",
        "SELECT s.division, s.department, s.category, s.units, s.net_sales, i.out_of_stock_rate",
        "FROM s JOIN i USING (division, department, category)",
        "WHERE i.out_of_stock_rate > 0.05",
        "ORDER BY (i.out_of_stock_rate * s.units) DESC",
        "LIMIT 25",
    ]


def ai_takeaways_querylines() -> list[str]:
    # ai_query over the current-fiscal-year headline aggregates. :llm_endpoint is a
    # dashboard parameter bound to ${var.llm_endpoint} at deploy.
    return [
        "-- AI Key Takeaways: LLM summary of the headline metrics (current fiscal year).",
        "WITH agg AS (",
        "  SELECT MEASURE(net_sales) AS net_sales, MEASURE(gross_margin_pct) AS gm_pct, MEASURE(units) AS units",
        "  FROM mv_sales WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM mv_sales)",
        ")",
        "SELECT ai_query(",
        "  :llm_endpoint,",
        "  CONCAT(",
        "    'You are a retail merchandising analyst. In 3 short bullet points, summarize ',",
        "    'these current-fiscal-year metrics for a VP of Merchandising and flag one risk. ',",
        "    'Net sales: ', CAST(ROUND(net_sales) AS STRING),",
        "    '. Gross margin %: ', CAST(ROUND(gm_pct*100,1) AS STRING),",
        "    '. Units: ', CAST(ROUND(units) AS STRING), '.'",
        "  )",
        ") AS takeaways",
        "FROM agg",
    ]


DATASETS: tuple[Dataset, ...] = (
    Dataset("sales", "Sales performance", sales_querylines()),
    Dataset("inventory", "Inventory position", inventory_querylines()),
    Dataset("bridge", "Sales ⋈ inventory bridge", bridge_querylines()),
    Dataset("lost_sales", "Lost-sales risk", lost_sales_querylines()),
    Dataset("ai_takeaways", "AI key takeaways", ai_takeaways_querylines()),
)
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_dashboard_datasets.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/dashboards/datasets.py tests/test_dashboard_datasets.py
git commit -m "Add merch dashboard dataset SQL builders (metric-view MEASURE + bridge)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 3: Widget builders, `build_dashboard()`, and the generated `.lvdash.json`

**Files:**
- Create: `src/techmart/dashboards/build.py`
- Create: `dashboards/merch_exec.lvdash.json` (generated by `write_dashboard`)
- Modify: `src/techmart/dashboards/__init__.py` (re-export `build_dashboard`, `write_dashboard`)
- Test: `tests/test_dashboard_build.py`

**Interfaces:**
- Consumes: `theme.ui_theme()`, `datasets.DATASETS`.
- Produces: `build_dashboard() -> dict` (a full lvdash dict with keys `datasets`, `pages`, `uiSettings`), `write_dashboard(path: str) -> None`.

**Widget JSON shapes (from a real exported lvdash.json — follow exactly):**

*Counter* (KPI tile):
```json
{"widget": {"name": "<unique>", "queries": [{"name": "main_query", "query": {
  "datasetName": "<dataset name>", "fields": [{"name": "<alias>", "expression": "SUM(`<col>`)"}],
  "disaggregated": false}}],
  "spec": {"version": 2, "frame": {"title": "<title>", "showTitle": true}, "widgetType": "counter",
  "encodings": {"value": {"fieldName": "<alias>", "format": {<format>}, "displayName": "<title>"}}}},
 "position": {"x": <int>, "y": <int>, "width": <int>, "height": <int>}}
```
*Formats:* currency `{"type":"number-currency","currencyCode":"USD","decimalPlaces":{"type":"exact","places":1},"abbreviation":"compact"}`; percent `{"type":"number-percent","decimalPlaces":{"type":"exact","places":1}}`; plain `{"type":"number-plain","decimalPlaces":{"type":"exact","places":1},"abbreviation":"compact"}`.

*Bar/Line* — `spec.widgetType` `"bar"`/`"line"`; `encodings` has `x` (dimension, `{"fieldName","scale":{"type":"categorical"|"temporal"},"displayName"}`) and `y` (measure, with `scale.type":"quantitative"`); query `fields` list both the dimension (`expression` = bare column in backticks) and the measure (`SUM(...)`).

*Pivot* (category matrix) — `spec.widgetType` `"pivot"`; `encodings` has `rows` (array of dimension fields, ordered division→department→category), `columns` (optional), and `values` (array of measure fields). Bind to `bridge`.

*Table* (lost-sales) — `spec.widgetType` `"table"`, `spec.rowsPerPage` = 25; `encodings.columns` = array of `{"fieldName","displayName","booleanValues"?,...}`. Bind to `lost_sales`.

*Filter* — `spec.widgetType` `"filter-single-select"` or `"filter-multi-select"`; `encodings.fields` = `[{"fieldName":"<dim>","displayName":"<label>","queryName":"main_query"}]`. One filter per shared dimension (fiscal_year, division, category, region, channel_type), each bound to the `bridge` dataset so the drill applies to the cross-fact view.

*Markdown/AI takeaways* — bind a widget to the `ai_takeaways` dataset showing the `takeaways` text column. Primary approach: a `table` widget with a single column `takeaways` and `frame.title` "AI Key Takeaways". (A dynamic text/markdown-from-field widget, if the current workspace supports it, is a Task-5 polish; the static `multilineTextboxSpec` is the ultra-fallback.)

**Layout grid:** the canvas is 12 columns wide. Suggested `y`/`height` bands: filter bar row `y=0 h=2`; KPI row `y=2 h=3` (six counters `width=2` each); AI takeaways `y=5 h=4 width=12`; sales trend + mix `y=9 h=6`; category pivot `y=15 h=8 width=12`; inventory charts `y=23 h=6`; lost-sales table `y=29 h=7 width=12`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dashboard_build.py
import json, pathlib
from techmart.dashboards.build import build_dashboard, write_dashboard
from techmart.dashboards.datasets import DATASETS
from techmart.dashboards.theme import ui_theme

def test_dashboard_has_all_datasets_and_one_page():
    d = build_dashboard()
    ds_names = {x["name"] for x in d["datasets"]}
    assert {ds.name for ds in DATASETS} <= ds_names
    assert len(d["pages"]) == 1
    assert d["uiSettings"]["theme"] == ui_theme()

def test_every_widget_binds_a_defined_dataset():
    d = build_dashboard()
    defined = {x["name"] for x in d["datasets"]}
    for item in d["pages"][0]["layout"]:
        for q in item["widget"].get("queries", []):
            assert q["query"]["datasetName"] in defined

def test_six_kpi_counters_present():
    d = build_dashboard()
    counters = [it for it in d["pages"][0]["layout"]
                if it["widget"].get("spec", {}).get("widgetType") == "counter"]
    assert len(counters) == 6
    titles = {c["widget"]["spec"]["frame"]["title"] for c in counters}
    assert {"Net Sales", "Gross Margin %", "Units", "Sell-Through %",
            "Weeks of Supply", "GMROI"} <= titles

def test_category_pivot_and_filters_bind_bridge():
    d = build_dashboard()
    layout = d["pages"][0]["layout"]
    pivots = [it for it in layout if it["widget"]["spec"].get("widgetType") == "pivot"]
    assert len(pivots) == 1
    assert pivots[0]["widget"]["queries"][0]["query"]["datasetName"] == "bridge"

def test_positions_do_not_overlap_and_fit_12_cols():
    d = build_dashboard()
    for it in d["pages"][0]["layout"]:
        p = it["position"]
        assert 0 <= p["x"] and p["x"] + p["width"] <= 12

def test_write_dashboard_roundtrips(tmp_path):
    out = tmp_path / "merch_exec.lvdash.json"
    write_dashboard(str(out))
    reloaded = json.loads(out.read_text())
    assert reloaded == build_dashboard()

def test_committed_json_matches_builder():
    # the committed artifact must equal build_dashboard() output (no drift)
    repo = pathlib.Path(__file__).resolve().parents[1]
    committed = json.loads((repo / "dashboards" / "merch_exec.lvdash.json").read_text())
    assert committed == build_dashboard()
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_dashboard_build.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

Implement `build.py` with small helper builders (`_counter`, `_bar`, `_line`, `_pivot`, `_table`, `_filter`, `_ai_takeaways`) each returning one layout item dict following the shapes above, and `build_dashboard()` that assembles: `datasets` from `DATASETS` (`{"name","displayName","queryLines"}`), one page `{"name","displayName":"Merch Executive","layout":[...]}` with the six KPI counters (Net Sales→`bridge.net_sales` currency, Gross Margin %→`bridge.gross_margin_pct` percent, Units→`bridge.units` plain, Sell-Through %→`bridge.sell_through_pct` percent, Weeks of Supply→`bridge.weeks_of_supply` plain, GMROI→`bridge.gmroi` plain), the AI takeaways widget, sales trend (`line`, `sales`: x=`fiscal_period`, y=`net_sales`), sales mix (`bar`, `sales`: x=`department`, y=`net_sales`), the category `pivot` (`bridge`: rows division/department/category; values net_sales, gross_margin_pct, units, sell_through_pct, weeks_of_supply, gmroi, out_of_stock_rate), inventory charts (`bar` `inventory`: x=`category` y=`on_hand_cost_value`; `bar` `inventory`: x=`region` y=`out_of_stock_rate`), the `lost_sales` table, and the filter row (`filter-single-select` on fiscal_year; `filter-multi-select` on division, category, region, channel_type — all bound to `bridge`), plus `uiSettings={"theme": ui_theme()}`. Use deterministic widget `name`s. `write_dashboard(path)` dumps `build_dashboard()` as indented JSON.

Then generate the committed artifact:

```bash
mkdir -p dashboards
python -c "from techmart.dashboards.build import write_dashboard; write_dashboard('dashboards/merch_exec.lvdash.json')"
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_dashboard_build.py -v`
Expected: PASS (including `test_committed_json_matches_builder`).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/dashboards/build.py src/techmart/dashboards/__init__.py dashboards/merch_exec.lvdash.json tests/test_dashboard_build.py
git commit -m "Generate merch exec dashboard .lvdash.json from typed builders

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 4: DAB dashboard resource

**Files:**
- Create: `resources/dashboards.yml`
- Test: `tests/test_dashboards_resource.py`

**Interfaces:**
- Consumes: the committed `dashboards/merch_exec.lvdash.json`, the existing `resources/sql_warehouse.yml` (`techmart_warehouse`), bundle vars `catalog`, `schema_prefix`, `llm_endpoint`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dashboards_resource.py
import pathlib, yaml

def test_dashboard_resource_wires_file_and_dataset_defaults():
    repo = pathlib.Path(__file__).resolve().parents[1]
    doc = yaml.safe_load((repo / "resources" / "dashboards.yml").read_text())
    dash = doc["resources"]["dashboards"]["merch_exec"]
    assert dash["file_path"].endswith("dashboards/merch_exec.lvdash.json")
    assert dash["dataset_catalog"] == "${var.catalog}"
    assert dash["dataset_schema"] == "${var.schema_prefix}semantic"
    assert dash["warehouse_id"] == "${resources.sql_warehouses.techmart_warehouse.id}"
    assert dash["display_name"] == "Techmart — Merchandising Executive"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_dashboards_resource.py -v`
Expected: FAIL (file missing).

- [ ] **Step 3: Write minimal implementation**

```yaml
# resources/dashboards.yml
# AI/BI dashboard, deployed from the generated .lvdash.json. dataset_catalog /
# dataset_schema inject the target catalog+semantic schema so the dashboard's
# dataset SQL stays unqualified (mv_sales, mv_inventory). llm_endpoint feeds the
# AI-takeaways ai_query via a dashboard parameter.
resources:
  dashboards:
    merch_exec:
      display_name: "Techmart — Merchandising Executive"
      file_path: ../dashboards/merch_exec.lvdash.json
      warehouse_id: ${resources.sql_warehouses.techmart_warehouse.id}
      dataset_catalog: ${var.catalog}
      dataset_schema: ${var.schema_prefix}semantic
      parent_path: /Workspace/Shared/techmart
```

- [ ] **Step 4: Run to verify it passes + validate the bundle**

Run: `python -m pytest tests/test_dashboards_resource.py -v` → PASS.
Run: `/opt/homebrew/bin/databricks bundle validate -t dev -p field-eng-east`
Expected: validates with no error referencing the dashboard resource. (Resolve the `:llm_endpoint` dashboard-parameter binding here if `validate` flags it — bind it in the resource or the lvdash parameter block; adjust Task 3's dataset if the parameter must be declared in the dashboard JSON.)

- [ ] **Step 5: Commit**

```bash
git add resources/dashboards.yml tests/test_dashboards_resource.py
git commit -m "Add DAB dashboard resource for the merch exec dashboard

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 5: Workspace deploy + validation (field-eng-east, showcase)

**This task is workspace-interactive** — run by the main session, not a blind subagent. No new code unless validation surfaces a fix (then add a regression test).

- [ ] **Step 1: Run the full local suite**

Run: `python -m pytest -q`
Expected: all prior tests plus the new dashboard tests pass.

- [ ] **Step 2: Deploy the bundle**

Run: `/opt/homebrew/bin/databricks bundle deploy -t dev -p field-eng-east`
(Requires explicit user authorization for `--auto-approve`-style prompts.) Confirm the `merch_exec` dashboard resource is created and points at the semantic schema.

- [ ] **Step 3: Confirm the dashboard publishes and datasets resolve**

Via CLI/API (`databricks lakeview get <id> -p field-eng-east`): confirm it exists, `display_name` matches, and re-run each dataset's SQL through the SQL Statement Execution API against `${catalog}.${schema_prefix}semantic` — every dataset returns rows.

- [ ] **Step 4: Sanity-check the numbers**

Via the SQL API: confirm KPI values match direct `MEASURE()` queries on `mv_sales`/`mv_inventory`; sell-through ∈ [0,1], weeks-of-supply positive/finite, GMROI positive; the `ai_takeaways` query returns non-empty text.

- [ ] **Step 5: Validate the AI-takeaways rendering + theme**

Confirm the AI-takeaways widget shows text and the California Sunset palette is applied (visualizationColors/canvas). If the dynamic text-from-field rendering is awkward on this workspace, apply the documented fallback (static `multilineTextboxSpec` seeded from one `ai_query` run) and note it. Record findings; no PR-blocking issues → proceed to finishing the branch.

---

## Self-Review

- **Spec coverage:** §2 scope → Tasks 2–3 (two metric views + bridge, hero metrics, single page, AI card); §7 theme → Task 1; §8 repo layout/delivery → Tasks 3–4; §9 validation → Task 5. Out-of-scope views are absent. ✓
- **Placeholder scan:** all code steps carry real code; the two genuine unknowns (native relationship math; dynamic AI-text widget) are flagged as workspace-validated in Task 5 with concrete fallbacks, not TODOs. ✓
- **Type consistency:** dataset `name`s (`sales`/`inventory`/`bridge`/`lost_sales`/`ai_takeaways`) defined in Task 2 are the exact `datasetName`s bound in Task 3; measure/dimension names match `metric_views.py`; `ui_theme()` shape produced in Task 1 is consumed unchanged in Task 3. ✓
