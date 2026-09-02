# Techmart Retail BI Data Foundation

Synthetic data generator for **Techmart**, a fictitious omnichannel big-box
electronics retailer (a Best Buy analog). It produces a governed, star-schema
lakehouse — dimensions, facts, finance, AI, an operational write-back layer, and
a semantic layer — that backs the "state-of-the-art BI on Databricks" blog
series.

All data is synthetic. This repo contains no real customer data and no secrets.

## What it generates

Generation runs on **Databricks serverless** (dbldatagen + PySpark) and writes
five Unity Catalog schemas under `<catalog>.<schema_prefix>*`:

| Schema | Contents |
|--------|----------|
| `…core` | 10 dimensions (`dim_date`, `dim_channel`, `dim_store`, `dim_vendor`, `dim_promotion`, `dim_employee`, `dim_customer`, `dim_product`) and the transactional facts (`fact_sales_line`, `fact_inventory_snapshot`, `fact_inventory_movement`, `fact_returns`, `fact_fulfillment`, `fact_loyalty_activity`, `fact_web_events`) |
| `…finance` | `dim_gl_account`, `dim_department`, and derived facts `fact_gl_actuals`, `fact_budget_plan`, `fact_inventory_valuation` (gross↔net reconciliation) |
| `…ai` | `fact_sales_forecast` (versioned weekly forecast), `product_review` and `service_case` (LLM-filled text via `ai_query`), `ai_anomaly_catalog` |
| `…ops` | Lakebase (managed Postgres) write-back: `replenishment_order`, `forecast_override`, read back through UC federation |
| `…semantic` | Six Databricks metric views (`mv_sales`, `mv_inventory`, …) + informational PK/FK `RELY` constraints over the gold star schema |

Referential integrity is guaranteed by construction; determinism is guaranteed
by a fixed run seed. See `docs/superpowers/specs/` for the full design and
`docs/blog-series/` for the blog notes.

## Development

```bash
pip install -e ".[dev]"
python -m pytest          # ~190 local Spark/pytest tests
```

Tests run a local Spark session (see `tests/conftest.py`); no workspace is
required. Workspace-only paths (`ai_query`, the Lakebase write, the synced
table, metric-view creation) are exercised by the deploy below.

## Prerequisites (deploy)

- **Databricks CLI v1.6.0+** — check with `databricks version`. If an older CLI
  (e.g. a pyenv-shimmed `v0.18.x`) is first on your `PATH`, invoke the full
  path (`/opt/homebrew/bin/databricks` on macOS Homebrew).
- A **Unity Catalog** workspace with **serverless compute** enabled and a
  catalog you can create schemas in.
- **Serverless SQL** and **Lakebase** available in the workspace (the bundle
  provisions its own instances of both — see below).
- Workspace authentication configured as a CLI profile.

## Deploy to Databricks (DAB)

Techmart ships as a **self-contained** Databricks Asset Bundle. Everything the
pipeline needs is provisioned by the bundle — a serverless SQL warehouse (for
the `ai_query` text-fill) and a Lakebase instance plus its UC federation catalog
(for the operational write-back). **No pre-existing warehouse or database, and
no `--var`, is required** for the default deploy.

1. Authenticate to your workspace (one-time):

   ```bash
   databricks auth login --host <workspace-url> --profile <profile>
   ```

2. Deploy and run the full generation pipeline (defaults to the tiny `smoke`
   profile and the `stable_classic_ppke9o` catalog):

   ```bash
   databricks bundle deploy -t dev -p <profile>
   databricks bundle run generate_facts -t dev -p <profile>
   ```

The `generate_facts` job is a serverless DAG:

```
generate_dims → generate_facts ─┬→ generate_finance
                                ├→ generate_ai ─┬→ generate_ai_text (SQL warehouse)
                                │               └→ generate_ops (Lakebase write-back)
                                └──────────────────┴→ generate_semantic (metric views + PK/FK)
```

It writes `<catalog>.<schema_prefix>{core,finance,ai,ops,semantic}` and seeds the
Lakebase write-back tables, readable back through the `techmart_lakebase` UC
federation catalog.

### `forecast_serving` synced table (two-step)

The `forecast_serving` Delta→Postgres synced table (`resources/lakebase.yml`)
mirrors `ai.fact_sales_forecast` into Lakebase. Because a bundle resource is
created at **deploy** time, its source table must already exist — so it is
deployed *after* a first generation run:

```bash
databricks bundle deploy -t dev -p <profile>         # provisions infra + jobs
databricks bundle run generate_facts -t dev -p <profile>  # creates ai.fact_sales_forecast
databricks bundle deploy -t dev -p <profile>         # attaches the synced table (source now exists)
```

Changing the synced table's spec later forces a recreate, which the CLI guards
behind `--auto-approve`. That is safe (the synced copy holds no unique data; the
Delta source is preserved), but review the plan before approving.

## Scale profiles

Set with `--var="scale_profile=<name>"`. `config/scale_profiles.yaml` defines:

| Profile | sales lines | web events | customers | SKUs | Use |
|---------|------------:|-----------:|----------:|-----:|-----|
| `smoke` | 50K | 100K | 1K | 500 | fast validation (the `dev` default) |
| `demo_lean` | 75M | 300M | 500K | 20K | lightweight demo |
| `showcase` | 750M | 3B | 5M | 200K | full demo scale (~24 min for core facts on serverless) |
| `stress` | 3B | 10B | 20M | 500K | scale testing |

**Deploy-time gotcha:** job `base_parameters` bake at **deploy** time, so a
larger profile must be set on the `deploy`, not the `run`:

```bash
databricks bundle deploy -t dev -p <profile> --var="scale_profile=showcase"
databricks bundle run generate_facts -t dev -p <profile>   # --var here alone does NOT override
```

The same applies to `--var="catalog=<your_catalog>"` and any other variable.

## Configuration (bundle variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `catalog` | `stable_classic_ppke9o` | target UC catalog |
| `schema_prefix` | `techmart_` | schema name prefix |
| `scale_profile` | `showcase` (dev target pins `smoke`) | dataset size |
| `llm_endpoint` | `databricks-meta-llama-3-1-8b-instruct` | model for `ai_query` text-fill |
| `lakebase_instance` | `techmart-lakebase` | provisioned Lakebase instance name |
| `lakebase_database` | `techmart` | Lakebase database |
| `lakebase_catalog` | `techmart_lakebase` | UC federation catalog over Lakebase |
| `lakebase_capacity` | `CU_1` | Lakebase capacity units |

The `dev` target runs in `mode: development`, which prefixes the **warehouse
display name and job name** with `[dev <user>]` (harmless) while leaving the
Lakebase instance and federation catalog names literal (they are DNS/UC-constrained).

## Validate a run

After a run, spot-check against `<catalog>.<schema_prefix>*` (any SQL warehouse):

- **Referential integrity** — anti-join each fact FK to its dimension; expect 0
  orphans.
- **Column-independence** — decorrelated attributes spread naturally, e.g.
  `SELECT round(avg(d),1) FROM (SELECT count(distinct date_sk) d FROM …core.fact_inventory_movement GROUP BY store_sk)`
  is dozens, not ~2 (guarded by `tests/test_*_spread.py` and
  `tests/test_no_fixed_seed_method.py`).
- **Finance reconciliation** — GL revenue accounts tie to core sales net of
  returns/allowances, and `fact_inventory_valuation.on_hand_cost_value` sums to
  the period-end `fact_inventory_snapshot` cost exactly.
- **AI/ops/semantic** — non-null `review_text`/`case_text`; write-back rows
  readable via the `techmart_lakebase` federation catalog; metric views return
  measures via `SELECT MEASURE(...)`.

## Repo layout

```
src/techmart/
  spark/        framework, SCD2, UC write, dimensions
  facts/        transactional fact builders + shared gen/lookups
  finance/      derived finance facts + fiscal-period helpers
  ai/           forecast + review/case builders
  ops/          Lakebase write-back builders + pg_write
  semantic/     metric-view + PK/FK constraint emitters
  jobs/         serverless entry points
notebooks/      Databricks source notebooks (deployed as job tasks)
resources/      DAB job / warehouse / lakebase resource definitions
config/         scale_profiles.yaml
docs/           design specs, implementation plans, blog notes
```

Generation is **serverless-native** — dbldatagen/PySpark, not a local engine.
All builders use `randomSeedMethod="hash_fieldname"` for independent,
deterministic per-column streams (enforced by a regression guard test).
