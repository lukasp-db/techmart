# Techmart AI (`techmart_ai`) — Design Spec

> Phase 6 (was Phase 5 sub-project 2 of 4: finance / **AI** / ops write-back / semantic). Builds on
> the completed `techmart_core` star schema and `techmart_finance` layer in the proven
> serverless-native dbldatagen/PySpark model. Parent spec:
> `docs/superpowers/specs/2026-08-30-techmart-data-foundation-design.md` (§AI).

## Purpose

Add the `techmart_ai` schema — the AI/ML and unstructured-text layer of the data foundation:
demand **forecasts** that pair with sales actuals for a forecast-accuracy story, an LLM-generated
**review** and **service-case** text corpus that fuels `ai_query` sentiment/summarization demos, and
a **documented anomaly catalog** so "explain this dip" narratives have real, findable signal.

This phase also fixes a pre-existing `techmart_core` defect (see §0) that is a **prerequisite** for a
credible forecast: sales currently collapse onto ~2 distinct days per store across the whole history,
so there is almost no weekly history to project from.

## Decisions locked in brainstorming

1. **Fix `fact_sales_line` date spread first** — root-caused to `randomSeedMethod="fixed"`
   correlating the header's `random=True` columns (§0). Fixing it is load-bearing for the forecast.
   Scope the fix to `fact_sales_line`; flag the identical pattern in the other builders as a
   documented follow-up (not swept in this phase).
2. **Real `ai_query` for text**, not templated synthesis — on-brand for a "state-of-the-art BI on
   Databricks" series. Because `ai_query` runs on a SQL warehouse, the AI schema is built by **two
   job tasks**: a serverless notebook that writes deterministic structure + a `prompt` column, then a
   **SQL task on a SQL warehouse** that fills the text columns via `ai_query`.
3. **Forecast-side anomalies** — actuals stay clean; the forecast deliberately misses around
   documented windows, so the anomaly lives in the forecast-vs-actual gap. A small
   `ai_anomaly_catalog` table documents each one.
4. **Bounded LLM volume** — reviews/cases use **absolute count levers** per scale profile (not a rate
   off sales-line volume), so `ai_query` cost stays controlled even at showcase/stress.
5. **Job fan-out** — `generate_ai` runs concurrently with `generate_finance` (both only depend on
   `generate_facts`); `generate_ai_text` depends on `generate_ai`.

## 0. Prerequisite fix — `fact_sales_line` date spread

### Root cause (confirmed empirically, local Spark)

The transaction-header `DataGenerator` in `src/techmart/facts/fact_sales_line.py` sets
`randomSeedMethod="fixed"`. Under `"fixed"`, every `random=True` column derives from the **same**
seeded stream, so `date_sk` becomes nearly a deterministic **function of** `store_sk`. The finer the
store cardinality, the tighter the collapse:

- 5 stores (smoke) → not noticeable (which is why local tests never caught it).
- 1000 stores (showcase) → each store's transactions land on ~2.1 distinct dates out of 1097; the
  derived finance facts (faithfully) then cover only ~2,092 of ~37,000 store-periods.

Reproduced with the real builder over a 3-year `dim_date`, 1000 stores, ~101 txns/store: **global**
distinct `date_sk` = 1097/1097 (all dates appear *somewhere*), but **per-store** distinct dates
avg = **2.1**. The weighted seasonality draw itself is fine — the defect is column correlation.

### Fix

Change the header generator to `randomSeedMethod="hash_fieldname"` (each column seeds off its own
field name → independent streams, still fully deterministic/reproducible given `randomSeed`).
Confirmed fix: per-store distinct dates go **2.1 → ~249** (≈ the coupon-collector expectation for
~300 txns over 1097 dates), decorrelating all header FKs (`date_sk`, `store_sk`, `customer_sk`,
`employee_sk`, `channel_sk`) at once. One-line change.

### Regression test (the property that would have caught it)

A test that builds `fact_sales_line` over a multi-year `dim_date` with **many stores** (e.g. 200+ at
a small rows override) and asserts per-store date spread scales with store count — e.g. average
distinct `date_sk` per store ≥ a healthy fraction of `min(txns_per_store, #dates)`, and NOT ~2.
Assert the *property* (spread scales, not correlated), not exact values. Existing basket-coherence
and RI tests must still pass (coherence keys on shared `transaction_id`, unaffected by seed method).

### Follow-up (documented, not in this phase)

The other fact/dim builders (`fact_inventory_movement`, `fact_returns`, `fact_web_events`, the
dbldatagen dims, etc.) use the same `randomSeedMethod="fixed"` and may carry latent
column-correlation. Flag for a dedicated audit/PR; only `fact_sales_line` is fixed here (it is what
the forecast reads, and the finance facts inherit the improvement through it).

## Tables (`techmart_ai`)

Every table + column carries a `COMMENT` (Genie), written via `write_table_uc` with the existing
`schema_prefix` mechanism (`<catalog>.techmart_ai.<table>`); table-level grain COMMENT emitted as in
Phase 4.

### `fact_sales_forecast` — grain: `product_sk × store_sk × fiscal_week × forecast_version`

Maps to `supplychain.demand_forecast`. Pairs with `fact_sales_line` actuals for forecast accuracy and
with `fact_budget_plan` for the AI-vs-plan story.

| column | type | notes |
|---|---|---|
| `date_sk` | long (key) | week-end `date_sk` for the fiscal week (real `dim_date` row → RI) |
| `product_sk` | long (key) | FK `dim_product` |
| `store_sk` | long (key) | FK `dim_store` |
| `forecast_version` | string | e.g. `baseline`, `improved` (≥2 versions → accuracy-improvement story) |
| `fiscal_year` / `fiscal_week` | int | degenerate period attrs |
| `forecast_qty` | double | projected units |
| `forecast_amount` | double | projected net sales |
| `lower_bound` / `upper_bound` | double | prediction interval around `forecast_qty` |
| `model_name` | string | e.g. `seasonal_naive_v1` |
| `forecast_generated_date` | date | as-of date the forecast was "produced" |

**Derivation (deterministic, PySpark):** aggregate `fact_sales_line` actual `quantity` /
`net_sales_amount` to (`product_sk`, `store_sk`, `fiscal_year`, `fiscal_week`). Forecast via a
**seasonal-naive** projection (same fiscal week prior year, or trailing mean where no prior) plus a
deterministic hash-keyed bias/noise and a prediction interval (`forecast_qty ± k·σ` proxy). Bound the
grain for scale: limit to **active products** and a rolling window (recent history + a short future
horizon), controlled by scale-profile knobs (§Scale). `forecast_version="baseline"` deliberately
under/over-shoots at anomaly windows (§Anomalies); `forecast_version="improved"` narrows the error →
the "our new model fixed forecast accuracy" narrative.

### `product_review` (text) — grain: one row per review

Maps to `ecommerce.product_review`. Reviews attach to **real** purchases (sampled `fact_sales_line`
rows) so `product_sk`/`customer_sk` are RI-valid and reviews correlate with things people bought.

| column | type | notes |
|---|---|---|
| `review_id` | long (key) | sequential/hash id |
| `product_sk` | long (key) | FK `dim_product` (from the sampled sales line) |
| `customer_sk` | long (key) | FK `dim_customer` |
| `date_sk` | long (key) | FK `dim_date` (review date, on/after purchase) |
| `rating` | int | 1–5, skewed toward 4–5 with a realistic long tail |
| `review_title` | string | **`ai_query`-filled** |
| `review_text` | string | **`ai_query`-filled** |
| `verified_purchase` | boolean | true when tied to a real sales line |
| `helpful_votes` | int | hash-keyed count |

### `service_case` (text) — grain: one row per case — the "Geek Squad" analog

Maps to `service.service_case`.

| column | type | notes |
|---|---|---|
| `case_id` | long (key) | sequential/hash id |
| `customer_sk` | long (key) | FK `dim_customer` |
| `product_sk` | long (key) | FK `dim_product` (some tied to `fact_returns` rows for coherence) |
| `store_sk` | long (key) | FK `dim_store` |
| `date_sk` | long (key) | FK `dim_date` |
| `case_type` | string | Repair / Warranty / Support |
| `channel` | string | Phone / In-Store / Online |
| `status` | string | Open / In-Progress / Resolved / Closed |
| `case_notes` | string | **`ai_query`-filled** |
| `resolution_notes` | string | **`ai_query`-filled** (null while Open) |
| `csat_score` | int | 1–5, correlated with `status`/`case_type` |

### `ai_anomaly_catalog` — grain: one row per documented anomaly

Deterministic reference table (built in the notebook; no LLM). Documents the injected/known
anomalies so demos can find them and the blog can cite them.

| column | type | notes |
|---|---|---|
| `anomaly_id` | long (key) | sequential |
| `anomaly_type` | string | holiday-demand-spike / vendor-supply-disruption / pricing-error / return-fraud-cluster / data-quality-blemish |
| `description` | string | human-readable narrative |
| `start_date_sk` / `end_date_sk` | long | window (real `dim_date` rows) |
| `affected_dimension` | string | e.g. product/category, store/region, vendor |
| `expected_signal` | string | what a detector should see (e.g. "forecast_qty << actual for 3 fiscal weeks") |
| `realized_in` | string | `fact_sales_forecast` for the two forecast-side ones; `catalog-only` for the rest |

The two demand-related anomalies (holiday spike, vendor supply disruption → stockouts) are
**realized** as forecast-vs-actual divergence in `fact_sales_forecast`. The other three
(pricing error, return-fraud cluster, DQ blemish) are documented as narrative for a later
core-fact-injection phase (they'd require perturbing already-built core generation).

## Text generation — real `ai_query`, notebook → SQL split

`ai_query` runs on a SQL warehouse, so text generation is split across two job tasks:

1. **`generate_ai` (serverless notebook task):** builds `fact_sales_forecast` (complete),
   `ai_anomaly_catalog` (complete), and the **structural** columns of `product_review` /
   `service_case` — every column *except* the text ones — **plus a deterministic `prompt` column**
   assembled per row from rating + product name/category + verified-purchase (reviews) or
   case_type + product + status (cases). Structural rows are written to staging tables
   (e.g. `_product_review_staging`, `_service_case_staging`), text columns absent.
2. **`generate_ai_text` (SQL task, warehouse):** `CREATE OR REPLACE TABLE … AS SELECT * EXCEPT(prompt),
   ai_query(:llm_endpoint, prompt) AS review_text …` off the staging tables into the final
   `product_review` / `service_case`. Runs on the configured SQL warehouse. `depends_on: generate_ai`.

New DAB variables (`databricks.yml`):
- **`warehouse_id`** — the serverless SQL warehouse to run the `ai_query` SQL task. No committed
  default (supplied per-deploy, like `host`); the SQL task needs it.
- **`llm_endpoint`** — Foundation Model endpoint name; default a Databricks-hosted pay-per-token
  model (e.g. `databricks-meta-llama-3-1-8b-instruct` — cheap/fast, adequate for synthetic text).

`ai_query` is non-deterministic; structural columns stay fully deterministic/seeded, and the SQL fill
uses `modelParameters` (`temperature` low) for stability. Reproducibility guarantees apply to
structure and measures, not the exact prose (acceptable for a demo corpus).

> Alternative considered: `ai_query` also runs inside a serverless notebook via `spark.sql`, avoiding
> a `warehouse_id`. Rejected in brainstorming in favor of the explicit warehouse/SQL-task split.

## Job DAG (fan-out — the parallelism goal)

```
generate_dims ─▶ generate_facts ─┬─▶ generate_finance                     (unchanged)
                                  └─▶ generate_ai ─▶ generate_ai_text
                                      (notebook)      (SQL task, warehouse)
```

`generate_finance` and `generate_ai` become concurrent siblings off `generate_facts` — Databricks
runs tasks as soon as their `depends_on` sets are satisfied, so this is purely an edge change in
`resources/generate_facts_job.yml` (add the `generate_ai`/`generate_ai_text` tasks with the right
`depends_on`; leave finance chained off `generate_facts` only, not off the AI branch). No artificial
serialization between finance and AI.

## Scale & config

Forecast is aggregated to fiscal week per active product×store within a bounded window — order
10⁶–10⁷ at showcase, not sales-line scale. Text corpora are deliberately small and **bounded by
absolute count**, not derived as a fraction of the 750M sales lines:

Add to `ScaleProfile` (with per-profile values, small at smoke, capped at showcase/stress):
- `num_reviews` — absolute review-row target (e.g. smoke ~200; showcase capped ~50k–100k).
- `num_service_cases` — absolute case-row target (e.g. smoke ~100; showcase capped ~25k–50k).
- `forecast_active_products` (cap on the number of products forecast) **and** `forecast_horizon_weeks`
  (size of the recent-history + future window) — together bound the forecast grain.
- `forecast_versions` — count/labels of forecast versions (default 2: baseline + improved).

These keep `ai_query` volume — and therefore Foundation Model cost/runtime — controlled at any scale.

## Determinism & referential integrity (unchanged discipline)

- Forecast is a deterministic aggregation + seasonal-naive projection of deterministic core facts.
- Review/case **structure** (ids, FKs, ratings, statuses, dates, prompts, helpful_votes, csat) uses
  hash-keyed factors via `facts/gen.py` (`uniform_hash`, `bounded_int`) — never `rand()`,
  `monotonically_increasing_id()`, `current_timestamp()`, or `uuid()`.
- FKs come from real dim/fact rows (reviews/cases sample real `fact_sales_line`/`fact_returns` and
  real dims; forecast keys on real `dim_product`/`dim_store` and period-end `date_sk`) → RI by
  construction.
- `fact_sales_line` header now uses `randomSeedMethod="hash_fieldname"` (still deterministic).
- The only non-determinism is the `ai_query` prose (isolated to two text columns each).

## Wiring

- New module package `src/techmart/ai/` — one file per table
  (`fact_sales_forecast.py`, `product_review.py`, `service_case.py`, `anomaly_catalog.py`) each
  exposing a `*_SPEC` + `build_*` function, plus `registry.py` (`AI_SPECS`) and the shared prompt
  assembly. Reuse `spark/framework.py`, `spark/uc_write.py`, `facts/gen.py`.
- `src/techmart/reference/` — curated anomaly catalog data (engine-agnostic, like
  `gl_accounts.py`/`pools.py`).
- `notebooks/generate_ai.py` (Databricks source notebook) reads persisted core tables
  (`spark.read.table`), writes forecast + anomaly catalog + review/case **staging** tables.
- `resources/generate_facts_job.yml` gains `generate_ai` (notebook_task, `depends_on: generate_facts`)
  and `generate_ai_text` (sql_task on `${var.warehouse_id}`, `depends_on: generate_ai`).
- `databricks.yml` gains `warehouse_id` and `llm_endpoint` variables.
- `src/techmart/facts/fact_sales_line.py` — the one-line `randomSeedMethod` fix.

## Testing (local Spark, mirrors Phase 4/5 fact tests)

- **`fact_sales_line` date-spread regression** (§0) — per-store date spread scales with store count.
- Per AI table: schema + grain uniqueness, RI (0 orphan FKs against source dims/facts), measure
  invariants, and determinism (same seed/inputs → identical **structure**).
- Forecast: aggregation correctness vs a reference roll-up; interval ordering
  (`lower_bound ≤ forecast_qty ≤ upper_bound`); anomaly-window divergence present for `baseline` and
  reduced for `improved`.
- Reviews/cases: bounded row counts honor `num_reviews`/`num_service_cases`; prompt column non-empty
  and well-formed; verified_purchase true iff tied to a real sales line; csat/status correlation.
- Anomaly catalog completeness (all five documented; windows are real `dim_date` rows).
- `ai_query` text fill is **not** locally testable — validated on the workspace at smoke scale
  (proven-green gate, as with finance): non-null text, sane length, no `ai_query` errors.

## Deployment notes

- Follows the finance DEPLOY GOTCHA: `dev` target pins `scale_profile=smoke`; showcase needs
  `bundle deploy --var=scale_profile=showcase` first (base_parameters bake at deploy).
- The SQL task requires `--var=warehouse_id=<serverless SQL warehouse id>` (field-eng-east) and a
  reachable `llm_endpoint`. Prove green end-to-end at smoke on field-eng-east before scaling.

## Out of scope (later phases)

- `techmart_ops` (Lakebase write-back) and `techmart_semantic` metric views — each its own
  spec→plan→PR cycle.
- Injecting the non-forecast anomalies (pricing error, return-fraud, DQ blemish) into core facts.
- Sweeping the other fact/dim builders for the `randomSeedMethod="fixed"` correlation (§0 follow-up).
