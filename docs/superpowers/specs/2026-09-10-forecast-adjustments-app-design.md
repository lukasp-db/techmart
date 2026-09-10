# Demand Forecast Adjustments App — Design

**Date:** 2026-09-10
**Status:** Approved for planning
**Author:** Lukas Peterson (with Claude)

## Problem / goal

A Databricks App for demand planners to review the weekly demand forecast and
submit forecast adjustments (overrides). It displays forecast results filtered by
category or product and a timeframe, and offers an "Adjust Forecast" form. The
form is **mocked** for now (returns success + shows the submitted row); real
write-back to Lakebase is a later iteration. The app matches the look and feel of
the hand-built "Techmart Sales & Inventory" dashboard.

This is the "operational loop" front door of the demo (the Blog-3 write-back
story), built on the live showcase data.

## Data sources (read-only, via databricks-sql-connector)

- **Forecast:** `stable_classic_ppke9o.techmart_ai.fact_sales_forecast` — weekly
  forecast per (product_sk, store_sk, fiscal_year, fiscal_week, forecast_version).
  Two versions: `baseline` (seasonal_naive_v1) and `improved` (gbt_v2). Columns
  include `forecast_qty`, `forecast_amount`, `lower_bound`, `upper_bound`,
  `date_sk` (week-end).
- **Product hierarchy:** `stable_classic_ppke9o.techmart_core.dim_product`
  (`product_sk`, `category_name`, `product_name`, `division_name`, …) for the
  category/product pickers and labels.
- **Dates:** `stable_classic_ppke9o.techmart_core.dim_date` for week-end dates /
  labels if needed (forecast rows already carry fiscal_year/week + date_sk).
- **Adjustments (existing):** `techmart_lakebase.ops.forecast_override` — the
  Lakebase-federated UC catalog, read-only. Columns: `override_id`, `product_sk`,
  `store_sk`, `fiscal_year`, `fiscal_week`, `ai_forecast_qty`, `override_qty`,
  `override_reason`, `planner_id`, `created_at`, `updated_at`. Reason vocabulary:
  Local promotion / Competitor closeout / Weather event / Known stockout recovery.
  Planner vocabulary: planner_amir / _bianca / _chen / _dana.

Catalog and schema names come from env vars (defaults above); nothing hardcoded
beyond sensible fallbacks.

## Architecture (Databricks Apps best practices)

New top-level `app/`:

```
app/
  app.yaml                 # Apps manifest: run command, env, sql-warehouse resource
  backend/
    main.py                # FastAPI: API routes + serves built frontend static files
    db.py                  # dbsql connection factory (SDK OAuth + warehouse http_path)
    queries.py             # parameterized SELECTs (forecast, filters, adjustments)
    models.py              # pydantic request/response models
    requirements.txt
  frontend/
    package.json, vite.config.ts, tsconfig.json
    index.html
    src/                   # React + TS SPA (components, theme, api client)
resources/app.yml          # DAB `apps` resource pointing at ../app
```

- **Backend:** FastAPI (uvicorn) serves `/api/*` and mounts the built React app
  (`frontend/dist`) as static files at `/`. Data access uses
  `databricks-sql-connector`; the connection is built from the Databricks SDK
  `Config` (host + the app service principal's OAuth token) and a
  `DATABRICKS_WAREHOUSE_ID` → `/sql/1.0/warehouses/<id>` http_path, both injected
  by the Apps runtime / `app.yaml`. All SQL is parameterized; no string
  interpolation of user input.
- **Frontend:** React + Vite + TypeScript, Recharts for the forecast chart,
  styled to the dashboard palette (below). Built with `npm run build`; the
  backend serves the static bundle. A dev proxy points `/api` at the local
  backend during the inner loop.
- **Auth/permissions:** read-only SELECTs. The app's service principal needs
  `USE CATALOG`/`SELECT` on `stable_classic_ppke9o` and the federated
  `techmart_lakebase`, and `CAN USE` on the SQL warehouse. No write grants.

## API

- `GET /api/filters` → `{ categories: string[], products: {product_sk, label}[],
  week_min: {fy, fw}, week_max: {fy, fw} }`. Products optionally scoped by
  `?category=`.
- `GET /api/forecast?level=category|product&id=<category_name|product_sk>&
  fy_start&fw_start&fy_end&fw_end&version=improved|baseline|both` →
  `{ series: [{fiscal_year, fiscal_week, week_end_date, version, forecast_qty,
  forecast_amount, lower_bound, upper_bound}], summary: {total_qty, total_amount,
  avg_weekly_qty} }`. Aggregated **across stores** to the chosen grain
  (SUM(qty/amount), and bounds summed) per week per version, over the timeframe.
- `GET /api/adjustments?limit=` → existing `forecast_override` rows (joined to
  `dim_product` for category/product labels) newest-first, **plus** any in-memory
  session rows, unioned and returned newest-first.
- `POST /api/adjustments` → body `{product_sk, store_sk, fiscal_year, fiscal_week,
  ai_forecast_qty, override_qty, override_reason, planner_id}`. **Mock:**
  validate against the reason/planner vocabularies and ranges, synthesize
  `override_id` + timestamps, append to an in-process list, return
  `{status:"success", row:{…}}`. Does **not** write to Lakebase. (In-memory store
  is per-process and resets on restart — acceptable for the mock.)

## Frontend UX

- **Header:** app title, warm off-white canvas (`#FAF7F3`), navy text, coral
  accent — matching the dashboard.
- **Filter bar:** Category | Product toggle; searchable picker; timeframe presets
  (e.g. Next 8 / 13 / 26 weeks within the available range) or a from/to week
  selector; version toggle (Improved / Baseline / Both).
- **KPI tiles:** total forecast units, total forecast $, avg weekly units over the
  window.
- **Forecast chart (Recharts):** x = week-end date; improved = coral line,
  baseline = navy line; shaded prediction band (`lower_bound`–`upper_bound`) for
  the selected version. Legend + tooltip.
- **Weekly table:** the series rows.
- **"Adjust Forecast" button** → modal form: product (+ store) selector, fiscal
  week, AI forecast qty (prefilled from the selected cell where possible), new
  override qty, reason (dropdown of the 4 reasons), planner (dropdown). Submit →
  success toast + the new row is prepended to the **Adjustments table**.
- **Adjustments table:** existing overrides (from Lakebase, read-only) + session
  submissions, newest-first, with product/category, week, AI qty, override qty,
  delta %, reason, planner, created_at.

### Palette (from the dashboard image — the brighter scheme the user pointed at)

Sampled approximations (finalized during build against the screenshot):
`coral #E8785D` (primary/accent, improved series), `navy #22283F` (text /
baseline series), `teal #57B0A0`, `gold #E4C15C`, `tan #E0B487`, canvas
`#FAF7F3`, widget `#FFFFFF`. Categorical order: coral, navy, teal, gold, tan.
(NB: intentionally the dashboard palette, not `theme.py`'s cornflower "California
Sunset" tokens.) Follow the `dataviz` skill for chart color/legend/axis rules.

## Non-goals

- No real write to `ops.forecast_override` (mock only).
- No store-level forecast browsing (chain-level aggregate; the override form still
  captures a store for the row).
- No auth/RBAC beyond the app SP's read grants; no per-planner identity (planner is
  a form field).
- No editing/deleting existing overrides.

## Deploy

- DAB `apps` resource in `resources/app.yml`; `databricks bundle deploy` then app
  deploy on `field-eng-east`. Verify the live URL loads, filters work, the chart
  renders live forecast data, and the mock form round-trips. Grant the app SP the
  read permissions above.

## Testing

- **Backend:** unit-test `queries.py` SQL builders (parameterization, filter/
  timeframe/level branching) and `POST /api/adjustments` validation + in-memory
  append with FastAPI `TestClient` (no live warehouse — the connection factory is
  injected/mocked). Validation rejects unknown reason/planner and negative qty.
- **Frontend:** component smoke (filter bar, chart renders given series, modal
  submit calls the API and prepends the row). Inner-loop verification via the
  web-devloop-tester (dev server + browser: pickers, chart, form round-trip,
  console clean).
- **Live:** after deploy, confirm the three GETs return real data and the POST
  returns success with the row.

## Risks

- **Apps runtime auth idiom** (SDK OAuth → dbsql http_path) is the one spot to get
  exactly right; the `databricks-apps` skill is the authority and the build
  verifies it live.
- **Forecast cardinality** — only `product_sk ≤ forecast_active_products` (5000 at
  showcase) have forecasts; the category picker lists only categories with
  forecasted products, and queries are week-windowed + aggregated to stay light.
- **In-memory mock** resets on app restart / multiple replicas — acceptable and
  documented for the mock stage.
