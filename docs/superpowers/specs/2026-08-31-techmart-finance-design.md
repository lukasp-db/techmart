# Techmart Finance (`techmart_finance`) — Design Spec

> Phase 5, sub-project 1 of 4 (finance / AI / ops write-back / semantic). Builds on the
> completed `techmart_core` star schema in the proven serverless-native dbldatagen/PySpark model.
> Parent spec: `docs/superpowers/specs/2026-08-30-techmart-data-foundation-design.md` (§Finance).

## Purpose

Add the `techmart_finance` schema — a general-ledger / budgeting / inventory-valuation layer that
**derives from the real core facts** so finance numbers tie back to merchandising numbers. The
deliberate, documented gap between operational gross sales and finance net sales is the
single-source-of-truth **reconciliation teaching story** the whole data foundation is built around.

## Decisions locked in brainstorming

1. **Derive + inject deltas** — Revenue/COGS GL actuals roll up from real `fact_sales_line` +
   `fact_returns` + `fact_inventory_movement`; `fact_inventory_valuation` rolls up from real
   `fact_inventory_snapshot`. Opex actuals and budget are standalone. Small deterministic
   timing/allowance/markdown deltas are injected so net ≠ gross is real and resolvable.
2. **New `dim_department`** — small conformed dimension in `techmart_finance`; facts carry
   `department_sk`.

## Calendar & grain

Finance runs on the fiscal 4-5-4 calendar already in `dim_date` (`fiscal_year`, `fiscal_period`
1–12, `fiscal_quarter`, `fiscal_week`). A **fiscal period** = (`fiscal_year`, `fiscal_period`).

Every finance fact is keyed on the **period-end `date_sk`** — the maximum `dim_date.date_sk` whose
(`fiscal_year`, `fiscal_period`) equals the fact's period. Because that `date_sk` is always a real
`dim_date` row, `date_sk` referential integrity holds by construction. The period-end lookup is a
single small DataFrame (`fiscal_year`, `fiscal_period` → `period_end_date_sk`) derived once from
`dim_date` and reused by all three facts.

## Dimensions (deterministic; `spark.createDataFrame`; no SCD2 — the `dim_channel` pattern)

### `dim_department` (grain: department)
~7 static rows. Built deterministically (no dbldatagen, no SCD2).

| column | type | notes |
|---|---|---|
| `department_sk` | long (key) | 1..N sequential |
| `department_name` | string | Merchandising, Store Operations, Supply Chain, Marketing, E-commerce, Finance & Admin, G&A |
| `department_group` | string | `COGS-bearing` vs `Opex` |

### `dim_gl_account` (grain: GL account)
A hand-authored chart of accounts (~50–60 accounts) authored as a structured Python reference list
in `src/techmart/reference/gl_accounts.py` (engine-agnostic, like `pools.py`/`taxonomy.py`), built
via `spark.createDataFrame`.

| column | type | notes |
|---|---|---|
| `gl_account_sk` | long (key) | 1..N sequential |
| `account_number` | string | e.g. `4000` |
| `account_name` | string | e.g. `Gross Product Sales` |
| `account_type` | string | Revenue / COGS / Opex / Asset |
| `statement` | string | P&L / Balance-Sheet |
| `statement_section` | string | rollup level 1 (e.g. `Net Sales`, `Cost of Goods Sold`, `Operating Expenses`, `Current Assets`) |
| `account_category` | string | rollup level 2 (e.g. `Store Payroll`, `Occupancy`, `Marketing`) |
| `normal_balance` | string | Debit / Credit |
| `is_contra` | boolean | true for Sales Returns, Sales Allowances |

**Required accounts** (the derivation targets must exist):
- Revenue: Gross Product Sales; Sales Returns (contra); Sales Allowances (contra) → roll up to
  `Net Sales`.
- COGS: Product COGS; Inventory Shrink; Markdowns; Freight-In.
- Opex: Store Payroll; Occupancy/Rent; Marketing; Supply-Chain Opex; General & Administrative;
  Depreciation.
- Asset: Merchandise Inventory (ties to `fact_inventory_valuation`).

## Facts

### `fact_gl_actuals` — grain: `gl_account_sk × store_sk × department_sk × fiscal_period`
FKs: `date_sk` (period-end), `gl_account_sk`, `store_sk` (cost center), `department_sk`.
Measures: `actual_amount` (double), `currency` (string, `USD`). Degenerate attrs: `fiscal_year`,
`fiscal_period`.

Line families:
- **Revenue (derived):** aggregate `fact_sales_line.gross_sales_amount` by (`store_sk`,
  fiscal period) → Gross Product Sales. Aggregate `fact_returns.refund_amount` by (`store_sk`,
  period) → Sales Returns (contra, negative to revenue). **Sales Allowances = `allowance_rate` ×
  gross** (contra), injected deterministically. Department = Merchandising, except online-channel
  sales (`channel_sk ∈ {2,3,4}`) attributed to E-commerce.
- **COGS (derived):** aggregate `fact_sales_line.cogs_amount` by (`store_sk`, period) → Product
  COGS (Merchandising). Aggregate `fact_inventory_movement` Shrink-event signed quantity × unit
  cost by (`store_sk`, period) → Inventory Shrink (Supply Chain). Markdowns = `markdown_rate` ×
  gross, injected (Merchandising).
- **Opex (standalone):** per (`store_sk`, period) generate Store Payroll, Occupancy, Marketing,
  Supply-Chain Opex, G&A, Depreciation — each = a fixed per-store base + a rate × the store's gross
  sales for that period, with a small deterministic hash-keyed jitter. Department per account
  (Payroll/Occupancy → Store Operations; Marketing → Marketing; Supply-Chain → Supply Chain;
  G&A/Depreciation → G&A).

**Reconciliation timing delta:** a `timing_shift_pct` slice of a period's **last-fiscal-week**
gross sales is recognized in the *next* fiscal period's Net Sales instead of the current one
(deterministic; shifts the boundary, conserves the annual total). This, plus returns and
allowances, is exactly why finance net sales ≠ merch gross — a documented, drill-down-resolvable
gap.

### `fact_budget_plan` — grain: `department_sk × store_sk × gl_account_sk × fiscal_period × plan_version`
FKs: `date_sk` (period-end), `store_sk`, `department_sk`, `gl_account_sk`.
Measures: `plan_amount` (double), `plan_units` (long). Attrs: `plan_version`
(Budget / Forecast / Latest-Estimate), `scenario` (string), `fiscal_year`, `fiscal_period`.

Standalone plan derived off actuals for realism: `plan_amount = actual_amount × (1 ±
budget_variance)` where the signed variance is a deterministic hash factor per
(account, store, plan_version). Three `plan_version` rows per actuals row so budget-vs-actual
attainment hovers realistically near 100% with a resolvable spread. Only P&L accounts are budgeted
(no balance-sheet asset budgeting).

### `fact_inventory_valuation` — grain: `store_sk × category_id × fiscal_period`
FKs: `date_sk` (period-end), `store_sk`, `category_id` (from `dim_product`).
Measures: `on_hand_cost_value`, `on_hand_retail_value`, `cogs_amount`, `markdown_amount`,
`shrink_amount`, `gmroi`. Attrs: `category_name`, `fiscal_year`, `fiscal_period`.

Derived from the **period-end** `fact_inventory_snapshot` (the snapshot row whose `date_sk` equals
the period-end `date_sk`): aggregate `on_hand_cost_value`/`on_hand_retail_value` by (`store_sk`,
category, period-end) after joining `dim_product` for `category_id`/`category_name`. `cogs_amount`
= sales COGS rolled to (store, category, period). `markdown_amount`/`shrink_amount` derived from the
same injected rates as `fact_gl_actuals` (consistent numbers across facts). `gmroi` = period gross
margin ÷ average inventory cost value (guard divide-by-zero with a `greatest(..., lit(1))` floor).

**`vendor_sk` dropped** from the parent spec's FK list: it conflicts with a category-level grain.
Noted deviation.

## Reconciliation (the teaching centerpiece)

`fact_sales_line` produces **operational gross sales**. Finance **Net Sales** (the
`Net Sales` rollup of `fact_gl_actuals`) = gross − returns − allowances, recognized on the finance
period calendar with the timing shift. The deltas are small, deterministic, and each traceable to
its cause (returns fact, injected allowances, boundary timing). The later `techmart_semantic` layer
will expose each metric with one authoritative definition — the single-source-of-truth payoff.

## Determinism & referential integrity (unchanged discipline)

- Derived facts are deterministic aggregations of already-deterministic core facts.
- Injected deltas (allowances, markdowns, timing, budget variance, opex jitter) use hash-keyed
  factors via `facts/gen.py` (`uniform_hash`, `bounded_int`) — never `rand()`,
  `monotonically_increasing_id()`, `current_timestamp()`, or `uuid()`.
- Period-end `date_sk` is always a real `dim_date` row → `date_sk` RI by construction. `store_sk`,
  `gl_account_sk`, `department_sk`, `category_id` all come from real dim/fact rows → RI by
  construction.
- Any standalone dbldatagen use sets `randomSeed=config.seed, randomSeedMethod="fixed"`.

## Scale & config

Finance is small relative to core (everything aggregated to fiscal period):
- `fact_gl_actuals` ≈ #stores × #accounts-per-store × #periods (order 10⁶–10⁷ at showcase).
- `fact_budget_plan` ≈ P&L subset × 3 plan versions.
- `fact_inventory_valuation` ≈ #stores × #categories × #periods.

No large new row-count knobs. Add reconciliation levers to `ScaleProfile` with defaults applied to
all profiles: `allowance_rate` (≈0.010), `markdown_rate` (≈0.015), `timing_shift_pct` (≈0.05),
`budget_variance` (≈0.08). These are shared across profiles (behavioral, not volume) unless a
profile overrides.

## Wiring

- New module package `src/techmart/finance/` — one file per fact/dim
  (`dim_department.py`, `dim_gl_account.py`, `fact_gl_actuals.py`, `fact_budget_plan.py`,
  `fact_inventory_valuation.py`) each exposing a `*_SPEC` + `build_*` function, plus a
  `registry.py` (`FINANCE_SPECS`). Reuse `spark/framework.py`, `spark/uc_write.py`,
  `facts/gen.py`, `spark/dim_builder.py`.
- `src/techmart/reference/gl_accounts.py` — the chart-of-accounts reference list.
- Schema `techmart_finance` via `write_table_uc`'s existing `schema_prefix` mechanism (writes to
  `<catalog>.techmart_finance.<table>`); table-level grain COMMENT emitted as in Phase 4.
- `notebooks/generate_finance.py` (Databricks source notebook) reads the persisted core tables
  (`spark.read.table`) and writes the finance tables.
- `src/techmart/jobs/generate_finance.py` — serverless `main()` mirroring the notebook, no dbutils.
- `resources/generate_finance_job.yml` (or extend the existing job) — a serverless notebook task
  that `depends_on` `generate_facts` (finance needs the persisted core facts).

## Testing (local Spark, mirrors Phase 4 fact tests)

Per fact/dim: schema + grain uniqueness, referential integrity (0 orphan FKs against the source
dims/facts), measure invariants, cross-fact **reconciliation coherence** (finance Net Sales =
merch gross − returns − allowances − timing, to the penny given the injected rates), and
determinism (same seed/inputs → identical output). Chart-of-accounts completeness test (all
required accounts present; contra flags correct).

## Out of scope (later Phase 5 sub-projects)

- `techmart_ai`, `techmart_ops` (Lakebase), `techmart_semantic` metric views — each its own
  spec→plan→PR cycle.
- The semantic-layer metric definitions that consume these facts.
