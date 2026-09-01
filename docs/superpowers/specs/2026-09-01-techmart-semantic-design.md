# Techmart Semantic (`techmart_semantic`) — Design Spec

> Phase 5.4 (the last of Phase 5's four sub-projects: finance / AI / ops write-back /
> **semantic**). Builds on the completed `techmart_core`, `techmart_finance`, and
> `techmart_ai` schemas in the proven serverless-native model. Parent spec:
> `docs/superpowers/specs/2026-08-30-techmart-data-foundation-design.md` (§Semantic layer).

## Purpose

Add the `techmart_semantic` schema — the **governed semantic layer** the blog's BI story
turns on. Databricks **metric views** (first-class YAML semantic objects: a source, joins,
dimensions, and measures with single authoritative definitions) sit over the gold star
schema so Genie, dashboards, and the Excel add-on all read the *same* metric definitions —
the single-source-of-truth payoff. Alongside the metric views, informational **PK/FK
constraints** are declared on the gold tables so BI tools and Genie can discover
relationships and the optimizer can eliminate redundant joins/aggregations.

## Decisions locked in brainstorming

1. **Subject-area metric views, one source fact each.** Each metric view is `source`d from a
   single fact modeled as a star (joins to the conformed dims). No persona-named objects;
   persona (Exec / Merch / Finance / Store) is a *documented* view→persona mapping, not
   duplicated definitions. Use the metric-view features fully: dimensions, measures,
   materialization.
2. **Cross-fact metrics deferred.** A metric view aggregates a single source, so metrics that
   span two facts at different grains — sell-through, weeks-of-supply, forecast accuracy/MAPE,
   budget attainment — are **out of scope this phase**. They will be shown through AI/BI
   dashboard relationships in the blog material later. No bridge views are built now.
3. **Informational PK/FK constraints with `RELY`.** Gold Delta tables gain
   `PRIMARY KEY (…) NOT ENFORCED RELY` and `FOREIGN KEY (…) REFERENCES … NOT ENFORCED RELY`.
   `RELY` lets the optimizer trust the constraint (join elimination, distinct/group-by
   elimination — a real perf win for Genie/BI). It is **only** safe because the generators
   guarantee key uniqueness and referential integrity by construction; the phase declares
   `RELY` exactly where existing tests prove those properties, and adds static tests that the
   FK graph is well-formed and every PK column is non-nullable.
4. **Materialization on 1–2 flagship views.** `mv_sales` (and likely `mv_inventory`) carry a
   `materialization:` block (scheduled, dimensionally-reduced aggregate) to demonstrate the
   capability and the Blog-2 perf payoff. Materialization is Preview / DBR 17.3+ and
   workspace-only, behind the proven-green gate.
5. **Deterministic structure, workspace-only apply.** All DDL/YAML is emitted by pure Python
   from typed specs and is fully locally testable; only the actual `CREATE VIEW WITH METRICS`,
   materialization refresh, and `RELY` constraint creation are workspace-only — exactly the
   gate used for the AI phase's `ai_query` and the ops phase's psycopg write.

## Architecture

Mirrors the `spark/uc_write.py` and `ops/pg_write.py` separation: **build the definition in
pure Python (testable) → apply it via a target-specific path (workspace-only).**

```
   techmart_core / techmart_finance / techmart_ai   (Delta gold tables, Unity Catalog)
                    │                        ▲
   (ALTER TABLE …   │                        │  (metric views SELECT from the gold tables
    ADD CONSTRAINT  │                        │   via WITH METRICS LANGUAGE YAML)
    PK/FK RELY)     ▼                        │
   gold tables gain PK/FK RELY ───────►  techmart_semantic.mv_*  (metric views)
```

### New package `src/techmart/semantic/`

- **`metric_view.py`** — typed specs and a pure emitter:
  - `MetricDimension(name, expr, comment, display_name=None, synonyms=None, format=None)`
  - `MetricMeasure(name, expr, comment, display_name=None, synonyms=None, format=None)`
  - `MetricJoin(name, source, on)` — `source` is a bare table name resolved against the same
    catalog/prefix at emit time; `on` is the join predicate (e.g. `source.date_sk = dim_date.date_sk`).
  - `Materialization(schedule, mode, materialized_views)` and
    `MaterializedView(name, dimensions, measures)`.
  - `MetricViewSpec(schema, name, source, comment, dimensions, measures, joins=[], materialization=None)`.
  - `metric_view_ddl(spec, *, catalog, schema_prefix) -> str` — emit
    `CREATE OR REPLACE VIEW <catalog>.<schema_prefix>semantic.<name> WITH METRICS LANGUAGE YAML AS $$\n<yaml>\n$$`.
    The YAML is `version: 1.1` with `source`, `joins` (quoted `"on":` key), `dimensions`,
    `measures`, optional `materialization`, and `comment`. **Pure, deterministic string
    generation** — hand-rolled YAML (no new dependency), with proper string escaping/quoting.
  - `qualified(source, catalog, schema_prefix, schema)` helper resolves a bare table name (e.g.
    `fact_sales_line`, `dim_date`) to `<catalog>.<schema_prefix><schema>.<table>`. Sources/joins
    carry the schema they live in (core/finance/ai) so resolution is unambiguous.

- **`constraints.py`** — informational-constraint specs and pure emitters:
  - `TableConstraints(schema, table, primary_key: tuple[str, ...], foreign_keys: list[ForeignKey])`
    where `ForeignKey(columns, ref_schema, ref_table, ref_columns)`.
  - `pk_ddl(tc, *, catalog, schema_prefix) -> str` →
    `ALTER TABLE <cat>.<prefix><schema>.<table> ADD CONSTRAINT <table>_pk PRIMARY KEY (<cols>) NOT ENFORCED RELY`.
  - `fk_ddl(tc, fk, *, catalog, schema_prefix) -> str` →
    `ALTER TABLE … ADD CONSTRAINT <table>_<refcols>_fk FOREIGN KEY (<cols>) REFERENCES
    <cat>.<prefix><refschema>.<reftable> (<refcols>) NOT ENFORCED RELY`.
  - Constraint names are deterministic and unique per table.

- **`registry.py`** — `METRIC_VIEW_SPECS: list[MetricViewSpec]` and
  `TABLE_CONSTRAINTS: list[TableConstraints]`.

### `notebooks/generate_semantic.py` (serverless notebook task)

Thin serverless wrapper over the emitters:

1. `CREATE SCHEMA IF NOT EXISTS <catalog>.<schema_prefix>semantic`.
2. For each `TableConstraints`: `spark.sql(pk_ddl(...))` then each `fk_ddl(...)` against the
   core/finance/ai gold tables. (Idempotent: constraints are re-declared; drop-if-exists
   guard as needed so re-runs are clean.)
3. For each `MetricViewSpec`: `spark.sql(metric_view_ddl(...))` into `…semantic`.

The notebook body only calls `spark.sql` on emitted strings, so it imports and is
string-testable locally; the actual execution (metric-view engine, `RELY`, materialization) is
workspace-only.

## Metric views (`techmart_semantic`)

Six single-source metric views, each a star over the conformed dimensions. Every dimension and
measure carries `comment` + `display_name`; high-value fields carry `synonyms` and `format`
(currency/percentage/number) for Genie. Deferred cross-fact metrics are listed per view.

### `mv_sales` — source `techmart_core.fact_sales_line` ⭐ materialized

Joins: `dim_date`, `dim_product`, `dim_store`, `dim_customer`, `dim_channel`, `dim_promotion`
(all `techmart_core`).

- **Dimensions:** date (`date`, `fiscal_year`, `fiscal_period`, `fiscal_quarter`,
  `selling_season`, `is_weekend`, `is_holiday`); product hierarchy (`division_name`,
  `department_name`, `category_name`, `subcategory_name`, `brand_name`, `product_name`);
  store (`region_name`, `district_name`, `store_format`, `store_name`); channel
  (`channel_name`, `channel_type`); customer (`segment`, `loyalty_tier`, `customer_type`);
  promotion (`promo_type`, `funding_source`).
- **Measures:** `gross_sales` `SUM(gross_sales_amount)`; `net_sales` `SUM(net_sales_amount)`;
  `discount` `SUM(discount_amount)`; `discount_rate`
  `SUM(discount_amount)/NULLIF(SUM(gross_sales_amount),0)`; `cogs` `SUM(cogs_amount)`;
  `gross_margin` `SUM(gross_margin_amount)`; `gross_margin_pct`
  `SUM(gross_margin_amount)/NULLIF(SUM(net_sales_amount),0)`; `units` `SUM(quantity)`;
  `line_count` `COUNT(1)`; `transaction_count` `COUNT(DISTINCT transaction_id)`;
  `avg_order_value` `SUM(net_sales_amount)/NULLIF(COUNT(DISTINCT transaction_id),0)`;
  `avg_basket_units` `SUM(quantity)/NULLIF(COUNT(DISTINCT transaction_id),0)`;
  `avg_unit_price` `SUM(gross_sales_amount)/NULLIF(SUM(quantity),0)`.
- **Materialization:** a daily aggregate over a reduced dimension set (e.g. date × region ×
  department) of the core sales measures.
- *Deferred:* sell-through, weeks-of-supply (need inventory), returns-adjusted net sales
  (needs `fact_returns`).

### `mv_inventory` — source `techmart_core.fact_inventory_snapshot` ⭐ materialized

Joins: `dim_date`, `dim_product`, `dim_store`.

- **Dimensions:** date (fiscal period, `date`); product hierarchy; store (region/district/format).
- **Measures:** `on_hand_qty` `SUM(on_hand_qty)`; `available_qty` `SUM(available_qty)`;
  `on_order_qty` `SUM(on_order_qty)`; `reserved_qty` `SUM(reserved_qty)`;
  `on_hand_cost_value` `SUM(on_hand_cost_value)`; `on_hand_retail_value`
  `SUM(on_hand_retail_value)`; `avg_days_of_supply` `AVG(days_of_supply)`;
  `out_of_stock_rate` `AVG(CASE WHEN is_out_of_stock THEN 1 ELSE 0 END)`;
  `sku_count` `COUNT(DISTINCT product_sk)`; `stocked_store_count` `COUNT(DISTINCT store_sk)`.
- *Deferred:* sell-through, weeks-of-supply (need sales demand).

### `mv_inventory_valuation` — source `techmart_finance.fact_inventory_valuation`

Joins: `dim_date`, `dim_store` (core), `dim_vendor` (core). Category is a degenerate attribute
of the valuation grain.

- **Dimensions:** fiscal period (date); store (region); vendor (`vendor_name`, `vendor_type`);
  category.
- **Measures:** `on_hand_cost_value` `SUM(on_hand_cost_value)`; `on_hand_retail_value`
  `SUM(on_hand_retail_value)`; `cogs` `SUM(cogs_amount)`; `markdown` `SUM(markdown_amount)`;
  `shrink` `SUM(shrink_amount)`; `avg_gmroi` `AVG(gmroi)`.
- *Deferred:* reconciliation deltas vs `fact_inventory_snapshot` (shown via dashboards).

### `mv_forecast` — source `techmart_ai.fact_sales_forecast`

Joins: `dim_date`, `dim_product`, `dim_store` (core).

- **Dimensions:** `forecast_version`, `model_name`; date (fiscal week/period); product; store.
- **Measures:** `forecast_qty` `SUM(forecast_qty)`; `forecast_amount` `SUM(forecast_amount)`;
  `lower_bound` `SUM(lower_bound)`; `upper_bound` `SUM(upper_bound)`; `interval_width`
  `SUM(upper_bound - lower_bound)`.
- *Deferred:* forecast accuracy / MAPE (needs actuals join).

### `mv_gl_actuals` — source `techmart_finance.fact_gl_actuals`

Joins: `dim_date`, `dim_gl_account` (finance), `dim_store` (core), `dim_department` (finance).

- **Dimensions:** account (`account_type`, `statement`, `account_name`); fiscal period; store
  (region); department.
- **Measures:** `actual_amount` `SUM(actual_amount)`.
- *Deferred:* budget attainment (needs `fact_budget_plan` join).

### `mv_budget_plan` — source `techmart_finance.fact_budget_plan`

Joins: `dim_date`, `dim_store` (core), `dim_gl_account` (finance), `dim_department` (finance).

- **Dimensions:** `plan_version`, `scenario`; account; fiscal period; store (region); department.
- **Measures:** `plan_amount` `SUM(plan_amount)`; `plan_units` `SUM(plan_units)`.
- *Deferred:* budget attainment / variance (needs actuals join).

### Persona → metric-view mapping (documentation, not objects)

| Persona | Metric views |
|---|---|
| Executive | `mv_sales`, `mv_gl_actuals`, `mv_budget_plan` |
| Merchandising | `mv_sales`, `mv_inventory`, `mv_inventory_valuation` |
| Finance | `mv_gl_actuals`, `mv_budget_plan`, `mv_inventory_valuation` |
| Store / Ops | `mv_sales`, `mv_inventory` |

## Informational constraints (`RELY`) on the gold tables

Declared via `constraints.py` emitters, applied by `generate_semantic.py`. `NOT ENFORCED RELY`
throughout — safe because generation guarantees uniqueness (PK) and RI (FK).

- **Dimensions (PK on the surrogate key):** `dim_product(product_sk)`, `dim_date(date_sk)`,
  `dim_store(store_sk)`, `dim_customer(customer_sk)`, `dim_employee(employee_sk)`,
  `dim_vendor(vendor_sk)`, `dim_promotion(promotion_sk)`, `dim_channel(channel_sk)`,
  `dim_gl_account(gl_account_sk)`, `dim_department(department_id)`.
  - SCD2 dims: `*_sk` is unique per version and non-null → valid PK for `RELY`.
- **Facts (PK on the grain, FK to each dim):** e.g. `fact_sales_line` PK
  `(transaction_id, line_number)`, FKs `date_sk`/`product_sk`/`store_sk`/`customer_sk`/
  `employee_sk`/`channel_sk` (and `promotion_sk`, nullable → FK still valid) to the matching
  dims. Every core/finance/ai fact declares its grain PK and its FK edges to the conformed
  dims per the bus matrix.
- Only columns declared `nullable=False` in a spec may appear in a PK (a static test enforces
  this — PK + `RELY` requires NOT NULL). FK columns may be nullable (a null means "no ref").

## Job DAG (extends the Phase-5.3 fan-out)

```
generate_dims → generate_facts ─┬→ generate_finance ──────────┐
                                ├→ generate_ai ─┬→ generate_ai_text
                                │               └→ generate_ops
                                └───────────────────────────────┴→ generate_semantic
                                   (depends_on: generate_facts, generate_finance, generate_ai)
```

`generate_semantic` is a serverless notebook task with
`depends_on: [generate_facts, generate_finance, generate_ai]` (it reads core facts, finance
facts, and the AI forecast). It does **not** depend on `generate_ai_text` (metric views do not
use review/case text) or `generate_ops` (Postgres, unrelated). A pure edge addition in
`resources/generate_facts_job.yml`.

## Scale & config

**No new `ScaleProfile` levers.** Metric-view and constraint definitions are volume-independent;
they describe the gold tables at any scale. The materialization schedule is a fixed cadence in
the YAML.

## Wiring

- **`resources/generate_facts_job.yml`** — add the `generate_semantic` notebook task with the
  right `depends_on` and the standard `catalog`/`schema_prefix`/`scale_profile` base parameters
  (scale_profile passed for consistency though unused by the definitions).
- **`src/techmart/semantic/`** — the module package above.
- **`notebooks/generate_semantic.py`** — the serverless notebook.
- No new `databricks.yml` variables (metric views need only catalog + schema_prefix, already
  present).

## Determinism & correctness discipline

- All DDL/YAML is emitted by pure functions from typed specs — deterministic strings, no
  runtime randomness.
- `RELY` correctness rests on the existing generator guarantees (uniqueness + RI by
  construction), re-asserted here by static tests over the constraint registry.
- Metric measure expressions are validated locally against sample data (below), so the metric
  math is proven independent of the workspace metric-view engine.

## Testing (local Spark, mirrors Phase 4/5/6 tests)

- **`metric_view_ddl` / YAML emission (pure strings):** each spec emits
  `CREATE OR REPLACE VIEW … WITH METRICS LANGUAGE YAML AS $$…$$`; `version: 1.1`; `source`
  fully qualified; each join present with a quoted `"on":` predicate; every dimension and
  measure present with `expr` + `comment` + `display_name`; the materialization block present
  on the flagship view(s); YAML round-trips (parse the emitted YAML and assert structure).
- **Measure-math validation (the key semantic test):** build tiny sample DataFrames matching
  each fact's schema, register them as temp views, run each measure's `expr` as a Spark SQL
  aggregation, and assert the numeric result (e.g. `net_sales = SUM(net_sales_amount)`;
  `gross_margin_pct = SUM(margin)/SUM(net)`; `out_of_stock_rate` fraction; `avg_gmroi`). This
  proves every metric definition is semantically correct on real Spark even though the
  metric-view wrapper is workspace-only.
- **Constraint DDL (`pk_ddl` / `fk_ddl`):** PK clause carries `NOT ENFORCED RELY` on the right
  columns; FK clauses reference the correct qualified table + columns and carry `RELY`;
  constraint names are unique per table.
- **Constraint-registry integrity (static):** every FK target `(schema, table, columns)` exists
  in the core/finance/ai registries; every PK column is `nullable=False` in its source spec
  (RELY safety); every fact in the bus matrix has a PK and the expected FK edges.
- **Registry:** `METRIC_VIEW_SPECS` has the six expected views with distinct qualified names;
  every measure/dimension `expr` references only columns present on the source/joined tables.
- **Notebook + DAB coverage:** `generate_semantic.py` is a Databricks source notebook
  referencing the emitters (mirror `test_notebooks.py`); the job has a `generate_semantic` task
  with the correct `depends_on`, and no new `databricks.yml` vars are required (mirror
  `test_dab_bundle.py`).
- **Workspace-only (proven-green gate, like `ai_query` / ops psycopg):** the actual
  `CREATE VIEW WITH METRICS`, `RELY` constraint creation, and materialization refresh —
  validated at smoke on field-eng-east: all six metric views resolve and return rows for their
  headline measures, constraints appear with `RELY`, the flagship view's materialization
  populates, and a Genie/`SELECT MEASURE(...)` query reads correctly.

## Deployment notes

- Follows the finance/AI/ops DEPLOY GOTCHA: `dev` target pins `scale_profile=smoke`; the
  metric views/constraints are scale-independent, so a smoke run is sufficient to prove them
  green (no showcase deploy needed for this phase's correctness).
- Metric views require DBR 16.4+; materialization and agent metadata (synonyms) require 17.3+;
  the workspace validation must run on a compatible warehouse/runtime.

## Out of scope (later phases / follow-ups)

- **Cross-fact metrics** — sell-through, weeks-of-supply, forecast accuracy/MAPE, budget
  attainment — shown via AI/BI dashboard relationships in the blog material later.
- Genie space configuration, dashboards, and the Excel add-on (downstream, separate specs).
- The blog content itself.
- Sweeping the other fact/dim builders for the `randomSeedMethod="fixed"` correlation (a
  standing Phase-6 follow-up, unrelated to this phase).
