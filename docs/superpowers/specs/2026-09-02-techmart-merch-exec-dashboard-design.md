# Techmart Merchandising Executive Dashboard — Design Spec

**Date:** 2026-09-02
**Author:** Lukas Peterson (Databricks SA)
**Status:** Design — pending review
**Companion to:** [BI series blog notes](../../blog-series/2026-08-30-bi-series-blog-notes.md),
[semantic layer design](2026-09-01-techmart-semantic-design.md)
**Artifact phase:** First of the downstream demo artifacts (see the
`techmart-demo-artifacts-roadmap` memory). Later artifacts (Excel FP&A report,
Genie spaces, apps) reuse the shared design system defined here.

---

## 1. Goal

A governed **AI/BI dashboard** for a merchandising executive that tells one
story: **how sales performance and inventory position relate**. It is the
flagship "governed, curated analytics" front door of Blog 1, built entirely on
the live showcase data (`stable_classic_ppke9o.techmart_*` on field-eng-east,
750M sales lines / daily store×SKU inventory snapshots).

The dashboard is a **checked-in, DAB-deployed artifact** — reproducible from the
public repo, not a hand-built one-off.

## 2. Scope

**In scope**
- One AI/BI dashboard, **single scrolling page** with a global filter bar and a
  category drill that cross-filters the whole page.
- Two metric-view datasets — `mv_sales` and `mv_inventory` — **bridged on the
  conformed dimensions** (product / store / date) via dashboard relationships.
- The **full open-to-buy hero metric set** realized across the bridge:
  sell-through %, weeks-of-supply, GMROI, inventory turns, and a stockout /
  lost-sales signal.
- One **AI Key Takeaways** narrative widget (`ai_query` over the aggregated,
  filtered view).
- A shared **"California Sunset" design system** (palette tokens + role mapping)
  applied to this dashboard and reused by all later artifacts.
- The DAB dashboard resource, warehouse wiring, catalog/schema parameterization,
  and local tests for any JSON-authoring code.

**Out of scope** (deliberate — keeps v1 focused)
- The other four metric views (`mv_inventory_valuation`, `mv_forecast`,
  `mv_gl_actuals`, `mv_budget_plan`). No forecast overlay, no budget-vs-actual,
  no finance reconciliation here — those belong to the Excel/Genie/app artifacts.
- Any write-back (that is the apps artifact / Blog 3).
- Multi-page layout.

## 3. Personas & use case

Primary: **Merchandising executive / VP of Merchandising.** Secondary: category
managers who drill from the exec view. The question the dashboard answers:
*"Which parts of the assortment are selling profitably, and is our inventory
position (too little / too much / stocked out) helping or hurting?"* — the
classic drill-across only a conformed model supports.

## 4. Data foundation

### 4.1 Datasets

Both datasets query the governed metric views in
`${catalog}.${schema_prefix}semantic` using `MEASURE()` aggregation, so metric
definitions stay single-sourced in the semantic layer.

| Dataset | Source metric view | Key measures used |
|---------|--------------------|-------------------|
| `sales` | `mv_sales` | `net_sales`, `gross_sales`, `gross_margin`, `gross_margin_pct`, `cogs`, `units`, `transaction_count`, `avg_order_value`, `discount_rate`, `avg_unit_price` |
| `inventory` | `mv_inventory` | `on_hand_qty`, `available_qty`, `on_hand_cost_value`, `on_hand_retail_value`, `avg_days_of_supply`, `out_of_stock_rate`, `sku_count` |
| `bridge` | `mv_sales` ⋈ `mv_inventory` | derived ratios (see §4.3) |

Shared dimensions available to both (from the metric-view specs): the 6-level
product hierarchy (`division` → `department` → `category` → `subcategory` →
`brand` → `product`), store geography (`region`, `district`, `store_format`,
`store`, `state`), full fiscal date (`fiscal_year`, `fiscal_quarter`,
`fiscal_period`, `selling_season`, `date`). `sales` additionally exposes channel,
customer, and promotion dimensions.

### 4.2 Relationships

Define dashboard relationships joining `sales` and `inventory` on the conformed
surrogate grain they share — product (via the hierarchy), store (via region), and
date. This lets the **global filter bar and the category drill cross-filter both
datasets at once** — the drill-across story the conformed dimensions were built
for, and a headline Blog-1 talking point ("relationships, not pre-joined marts").

### 4.3 The bridge (cross-fact ratios)

The hero metrics are ratios whose numerator and denominator come from **different
facts**, so each must be computed with both terms in one query. Approach:

- Use **relationships** for all cross-filtering and for any visual the native
  relationship engine can compute directly.
- Add a **thin purpose-built `bridge` dataset**: one SQL query that joins the two
  metric views (via `MEASURE()`) at **category × fiscal-period** grain (the grain
  at which sell-through / WOS / GMROI are meaningful and readable) and computes the
  ratios. This guarantees correct ratio KPI tiles and the category matrix columns.

The exact split (what relationships compute natively vs. what the bridge dataset
supplies) will be **confirmed against the live workspace during implementation**;
the bridge dataset is the correctness fallback that guarantees the ratios.

**Hero metric formulas** (real column names):

| Metric | Formula |
|--------|---------|
| Sell-through % | `units` / (`units` + period-end `on_hand_qty`) |
| Weeks of supply | period-end `on_hand_qty` / avg weekly `units` over the period |
| GMROI | `gross_margin` / avg `on_hand_cost_value` |
| Inventory turns | `cogs` / avg `on_hand_cost_value` |
| Stockout / lost-sales | `out_of_stock_rate` (native) ranked with sales velocity (`units`) → high-OOS × high-velocity = lost-sales risk |

"Period-end" and "avg over period" use the snapshot at / averaged over the
filtered date range. Denominators guarded with `NULLIF(...,0)`.

## 5. Layout (single page, top → bottom)

1. **Global filter bar** — date range + fiscal period/quarter/year · Division →
   Department → Category → Subcategory (cascading) · Region / District / Store ·
   Channel & Channel Type · Brand. Bound across datasets via relationships.
2. **KPI scorecard row** (counters with period-over-period deltas): Net Sales ·
   Gross Margin % · Units · **Sell-through %** · **Weeks of Supply** · **GMROI**.
3. **AI Key Takeaways** — full-width narrative card (see §6).
4. **Sales performance** — net-sales trend (current vs. prior fiscal year) · sales
   mix by division/department · discount-rate / promo lift.
5. **Category performance matrix** — the drill centerpiece: expandable 6-level
   hierarchy rows × columns [Net Sales, GM%, Units, Sell-through %, WOS, GMROI,
   OOS rate]. Selecting a row cross-filters the whole page.
6. **Inventory & OTB** — on-hand cost/retail value trend · weeks-of-supply by
   category · out-of-stock rate by region · avg days-of-supply.
7. **Lost-sales callout** — table of high-OOS + high-velocity categories/SKUs.

The built-in AI/BI Genie assistant is available on the canvas automatically (no
extra work); it is a bonus front door on the same governed data, not a widget we
build.

## 6. AI Key Takeaways widget

A dataset runs `ai_query('${llm_endpoint}', <prompt>)` where the prompt embeds the
current filtered aggregates (net sales, GM%, top/bottom categories by sell-through
and GMROI, notable OOS). It returns a short plain-language summary rendered in a
text/markdown widget. The aggregate subquery respects the same filters/relationships
as the rest of the page. `llm_endpoint` reuses the existing bundle variable
(`databricks-meta-llama-3-1-8b-instruct` default). Keep the prompt deterministic in
structure and bounded in length; the call runs on dataset refresh, not per-viewer.

## 7. Design system — "California Sunset"

Defined once, reused by every downstream artifact. Palette tokens (canonical DMC
floss→RGB for the referenced swatch; fine-tuned visually at build):

| Token | DMC | Hex | Role |
|-------|-----|-----|------|
| `blue-dark` | 792 Dark Cornflower | `#47527B` | headers, primary categorical series, emphasis |
| `blue-med` | 793 Medium Cornflower | `#707DA3` | secondary series |
| `blue-light` | 794 Light Cornflower | `#8F9CC1` | tertiary series, light fills |
| `terra` | 758 Very Light Terra Cotta | `#ECA991` | warm accent / highlight |
| `pink` | 223 Light Shell Pink | `#CC928C` | soft alert / secondary negative |
| `violet-dark` | 3740 Dark Antique Violet | `#78566A` | text, deep neutral, strong negative |

Derived neutrals: warm off-white canvas (≈ `#FAF7F3`), light warm gridline gray.
**Semantic roles:** positive/favorable → cornflower blues; caution/unfavorable
(declining sales, OOS, low WOS) → terra cotta → shell pink → antique violet ramp;
text/axis → antique violet. Applied to the dashboard as its categorical + diverging
color palettes, counter accents, and canvas background. Recorded as a small shared
theme asset (see §8) so Excel and apps inherit the same tokens.

## 8. Repo layout & delivery

```
dashboards/
  merch_exec.lvdash.json         # deployable AI/BI dashboard (source of truth for layout)
src/techmart/dashboards/
  __init__.py
  theme.py                       # California Sunset tokens + role map (shared design system)
  datasets.py                    # dataset SQL builders (MEASURE() queries + the bridge join)
  render.py                      # inject ${catalog}/${schema_prefix} into dataset queries
resources/
  dashboards.yml                 # DAB `dashboards` resource (file_path, warehouse_id, display_name)
tests/
  test_dashboard_theme.py        # palette tokens present, roles map to valid hex
  test_dashboard_datasets.py     # dataset SQL shape, bridge ratios use NULLIF guards, measures exist
  test_dashboard_render.py       # catalog/schema substitution correctness
```

- **Source of truth for layout** is the committed `merch_exec.lvdash.json`
  (layout/widget positions are inherently positional; authoring them directly — or
  building once in the AI/BI UI on live data and exporting — is acceptable).
- The **correctness-critical, testable parts** — dataset SQL (measures, bridge
  ratios, filter wiring) and **catalog/schema parameterization** — are produced by
  the typed helpers in `src/techmart/dashboards/` and unit-tested locally. `render.py`
  injects `${catalog}`/`${schema_prefix}`; the committed JSON carries the repo
  defaults so `bundle deploy` works out of the box, and the renderer regenerates it
  for any other catalog.
- **DAB resource** (`resources/dashboards.yml`): a `dashboards` entry with
  `file_path: ../dashboards/merch_exec.lvdash.json`, `warehouse_id` pointing at the
  existing `techmart_warehouse`, and a `display_name`. Deployed by the same bundle;
  no new bundle variables (reuses `catalog`, `schema_prefix`, `llm_endpoint`,
  `warehouse_size`).

## 9. Testing & validation

**Local (pytest, no workspace):** theme tokens/roles; dataset SQL builders emit
valid measure references and `NULLIF`-guarded ratios; `render.py` substitutes
catalog/schema correctly; the committed `.lvdash.json` parses and every dataset
reference resolves to a defined dataset.

**Workspace (field-eng-east, showcase):** deploy via DAB; confirm the dashboard
publishes and every dataset returns rows; via the SQL Statement API, sanity-check
that KPI values match direct `MEASURE()` queries against `mv_sales`/`mv_inventory`,
and that bridge ratios land in plausible ranges (sell-through 0–100%, WOS positive
and finite, GMROI positive); confirm the AI Key Takeaways `ai_query` returns
non-empty text.

## 10. Open questions / risks

- **Native relationship math vs. bridge dataset:** the precise division of labor
  (§4.3) is validated on the live workspace during implementation; the bridge
  dataset is the guaranteed-correct fallback.
- **Metric-view materialization** (`mv_sales`/`mv_inventory` are materialized daily)
  keeps the KPI queries fast at 750M rows; the category × SKU matrix drill on raw
  grain is the main performance watch item — validate response time on the Large
  warehouse and add LIMIT / top-N where needed.
- **lvdash.json authoring effort:** the schema is verbose; if hand-authoring proves
  brittle, build once in the UI on live data and export, then codify parameterization
  via `render.py`.

---

## Addendum (2026-09-02) — Indexed bridge (post-validation redesign)

Live validation on showcase data revealed the synthetic inventory is uncalibrated to
sales (every store carries every SKU; on-hand ≈ 35 "days" of an internal forecast running
~280× hotter than actual `fact_sales_line` sales), so absolute cross-fact ratios
(WOS ≈ 1400 wks, sell-through ≈ 3.5%, GMROI ≈ 0.035) are not demo-credible. Data is kept
as-is; the metric is reframed (user decision):

- **KPI tiles** show realistic absolutes only: Net Sales, Gross Margin %, Units (from `sales`);
  Avg Days-of-Supply, On-Hand Value, Out-of-Stock Rate (native `inventory`). The broken
  cross-fact ratios are NOT shown as single-value tiles.
- **Cross-fact bridge stays the hero** but at **category grain**, presented as **indices vs.
  the chain (100 = chain average)** and **category rankings**: `sell_through_index`,
  `inventory_efficiency_index` (inverted WOS: higher = leaner), `gmroi_index`, computed with
  total-based chain aggregates via `... OVER ()` window functions. Shown in a ranked category
  table + a top-categories bar, alongside absolute Net Sales / GM% / Units.
- The absolute ratio columns remain in the dataset (documentation/tooltip) but are not surfaced
  as headline numbers.
- Cross-dataset filtering (former parked item #2) is implemented here: division/category/region
  filters carry one field per dataset that has the column, so the filter bar drives sales,
  inventory, and bridge widgets together.
- Follow-up (data foundation, separate): recalibrate `fact_inventory_snapshot` on-hand to real
  sales velocity + sparse per-store assortment, after which absolute WOS/GMROI/turns become
  credible and the index framing can revert to absolutes if desired.
