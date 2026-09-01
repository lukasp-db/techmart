# Techmart Ops (`techmart_ops`) — Design Spec

> Phase 5.3 (was Phase 5 sub-project 3 of 4: finance / AI / **ops write-back** / semantic).
> Builds on the completed `techmart_core` star schema and `techmart_ai` forecast in the proven
> serverless-native dbldatagen/PySpark model. Parent spec:
> `docs/superpowers/specs/2026-08-30-techmart-data-foundation-design.md` (§Operational write-back).

## Purpose

Add the `techmart_ops` schema — the **operational write-back** layer of the data foundation. A
Lakebase (managed Postgres) OLTP store holds transactional tables an app/planner mutates, closing
Blog 3's *read-analytics ⋈ write-operations* loop: analysts read the gold star schema, operators
act in Postgres, and those actions read straight back into the lakehouse.

The phase demonstrates **both Lakebase directions**:

- **Serve-to-app (Delta → Postgres):** a managed **synced table** mirrors a bounded slice of
  `fact_sales_forecast` into Postgres so an app reads AI forecasts at low latency.
- **Write-back (Postgres → lakehouse):** two **native writable Postgres tables**
  (`replenishment_order`, `forecast_override`) are seeded from real lakehouse rows; the Lakebase
  database is registered as a **Unity Catalog Postgres (federation) catalog** so operational state
  reads back into the lakehouse with no extra ETL.

## Decisions locked in brainstorming

1. **Bundle-provisioned instance** — the DAB declares a `database_instances` resource so the bundle
   creates/manages the Lakebase Postgres instance (self-contained for a public demo). DAB
   database-instance / synced-table resource maturity is a documented workspace-validation risk.
2. **Both directions** — ship the write-back tables *and* a Delta→PG synced table
   (`forecast_serving`), so the phase tells the full Lakebase story, not just half of it.
3. **psycopg write path** — the writable tables are created and seeded from a serverless notebook
   via **psycopg**: explicit DDL (PK/FK + column `COMMENT`s for Genie/lineage) and an idempotent
   truncate+reseed. Mirrors the `spark/uc_write.py` separation (build DataFrame in Spark; write via
   a target-specific path). The Spark `postgresql` connector was considered and rejected — weaker
   control over constraints and comments.
4. **Deterministic structure, workspace-only write** — all row *structure* (ids, FKs, quantities,
   statuses, timestamps) is deterministic/seeded and locally testable; only the actual Postgres
   write / synced-table / federation-catalog creation is workspace-only, behind a proven-green gate
   exactly like the AI phase's `ai_query`.
5. **Regenerate resets operational state** — the generator truncates and reseeds the writable tables
   to a deterministic baseline on each run. Regenerating the data foundation wipes app mutations by
   design; the app mutates thereafter.

## Architecture — two Lakebase directions

```
                         techmart_core / techmart_ai   (Delta, Unity Catalog)
                                    │                    ▲
              (Delta → PG synced)   │                    │  (PG → lakehouse: UC federation)
                                    ▼                    │
   ┌──────────────────────────  Lakebase Postgres instance  ──────────────────────────┐
   │  forecast_serving   (synced, read-only in PG)  │  replenishment_order  (writable) │
   │    ← synced from fact_sales_forecast           │  forecast_override    (writable) │
   └───────────────────────────────────────────────────────────────────────────────────┘
```

- `forecast_serving` is a managed synced table: a Lakeflow pipeline continuously mirrors a bounded
  slice of `fact_sales_forecast` (Delta) into Postgres. **Read-only in Postgres** (Databricks
  recommends read-only access to synced tables to protect source integrity).
- `replenishment_order` / `forecast_override` are **native Postgres tables** (no source Delta
  table) supporting full transactional read-write — this is what makes them true write-back tables.
- The Lakebase database is registered as a **UC Postgres catalog** (query federation), so the
  writable tables are directly queryable from the lakehouse — the automatic "sync back".

## Tables (`techmart_ops`, in Postgres)

Every column carries a `COMMENT` (Genie/lineage), written by the psycopg DDL emitter via
`COMMENT ON COLUMN`; each table carries a table-level `COMMENT` naming its grain.

### `replenishment_order` — grain: one suggested replenishment per product × store

Maps to `inventory.replenishment_order`/`reorder_policy`. Seeded from `fact_inventory_snapshot`
rows at the latest snapshot date where `available_qty <= reorder_point` (a genuine reorder signal),
bounded to `num_replen_orders` by deterministic rank.

| column | pg type | notes |
|---|---|---|
| `replen_id` | bigint (PK) | deterministic `xxhash64(product_sk, store_sk, date_sk)` |
| `product_sk` | bigint (FK) | from the real snapshot row → RI vs `dim_product` |
| `store_sk` | bigint (FK) | from the real snapshot row → RI vs `dim_store` |
| `suggested_qty` | int | `reorder_point + safety_stock_qty − available_qty`, clamped `>= 0` |
| `approved_qty` | int (null) | null while `Suggested`; else `suggested_qty` (± small hash tweak) |
| `status` | text | Suggested / Approved / Rejected / Ordered (hash-keyed, mostly Suggested) |
| `reorder_point` | int | carried from the snapshot row |
| `created_by` | text | `system` |
| `approved_by` | text (null) | planner pool; null while `Suggested` |
| `created_at` | timestamptz | **deterministic** from snapshot `date_sk` → `dim_date.date` |
| `updated_at` | timestamptz | deterministic (`created_at` + fixed offset per status) |

Measure invariants: `suggested_qty >= 0`; `approved_qty` is null **iff** `status = 'Suggested'`;
`approved_by` null iff `status = 'Suggested'`.

### `forecast_override` — grain: one human override of a forecast cell

Maps to `supplychain` human-in-the-loop over `fact_sales_forecast`. Seeded from sampled
`fact_sales_forecast` rows (`forecast_version = 'improved'`), bounded to `num_forecast_overrides`.

| column | pg type | notes |
|---|---|---|
| `override_id` | bigint (PK) | deterministic `xxhash64(product_sk, store_sk, date_sk)` |
| `product_sk` | bigint (FK) | from the real forecast row → RI vs `dim_product` |
| `store_sk` | bigint (FK) | from the real forecast row → RI vs `dim_store` |
| `fiscal_year` | int | degenerate period key (from the forecast row) |
| `fiscal_week` | int | degenerate period key (from the forecast row) |
| `ai_forecast_qty` | double | = the forecast row's `forecast_qty` |
| `override_qty` | double | deterministic planner delta off `ai_forecast_qty` (±, hash-keyed) |
| `override_reason` | text | small pool: Local promotion / Competitor closeout / Weather event / Known stockout recovery |
| `planner_id` | text | planner pool |
| `created_at` | timestamptz | deterministic from the forecast row's `date_sk` → date |
| `updated_at` | timestamptz | deterministic (`created_at` + fixed offset) |

Measure invariants: `override_qty >= 0`; `override_reason` and `planner_id` non-empty; every
`(product_sk, store_sk, fiscal_year, fiscal_week)` corresponds to a real forecast row.

### `forecast_serving` — synced table (Delta → Postgres), read-only

Not a `build_*` output — a managed synced table created against a bounded slice of
`fact_sales_forecast` (`forecast_version = 'improved'`, capped to `forecast_serving_rows`). Grain
matches the source forecast. Demonstrates the serve-to-app direction; the app reads this instead of
querying Delta directly.

## Generation & write path

### New package `src/techmart/ops/`

- `replenishment_order.py` — `REPLENISHMENT_ORDER_SPEC` (a `PgTableSpec`) + `build_replenishment_order(
  spark, config, *, fact_inventory_snapshot, dim_date) -> DataFrame`.
- `forecast_override.py` — `FORECAST_OVERRIDE_SPEC` + `build_forecast_override(spark, config, *,
  fact_sales_forecast, dim_date) -> DataFrame`.
- `pg_write.py` — `PgColumn` / `PgTableSpec` (columns, `primary_key`, `foreign_keys`) plus:
  - `pg_ddl(spec, schema) -> list[str]` — emit `CREATE TABLE IF NOT EXISTS` with typed columns, PK,
    FK constraints, table + column `COMMENT`s. **Pure string generation → locally testable.**
  - `pg_type(spark_type) -> str` — long→bigint, int→integer, double→double precision, string→text,
    boolean→boolean, timestamp→timestamptz, date→date.
  - `write_pg(df, spec, *, conn, schema)` — connect (workspace OAuth), run DDL, `TRUNCATE`, and seed
    via `executemany` batches. **Workspace-only** (psycopg + a live Lakebase instance).
- `registry.py` — `OPS_SPECS = [REPLENISHMENT_ORDER_SPEC, FORECAST_OVERRIDE_SPEC]`.
- Reuse `facts/gen.py` (`uniform_hash`, `bounded_int`, `xxhash64`) for all hash-keyed structure.

### `notebooks/generate_ops.py` (serverless notebook task)

Reads `fact_inventory_snapshot` and `fact_sales_forecast` via `spark.read.table`, builds both
DataFrames, then (workspace-only, via the Databricks SDK + psycopg against the provisioned
instance):

1. `write_pg` the two writable tables (idempotent truncate + reseed, with DDL/PK/FK/comments).
2. Create the `forecast_serving` **synced table** from the bounded forecast slice (created after
   `fact_sales_forecast` exists → ordering-safe, unlike a deploy-time resource).
3. Ensure the **UC Postgres federation catalog** for the instance exists (read-back path).

## Job DAG (extends the Phase-6 fan-out)

```
generate_dims → generate_facts ─┬→ generate_finance
                                └→ generate_ai ─┬→ generate_ai_text
                                                └→ generate_ops
                                                    (needs fact_inventory_snapshot + fact_sales_forecast)
```

`generate_ops` is a serverless notebook task with `depends_on: [generate_facts, generate_ai]`
(inventory from facts, forecast from AI). It runs concurrently with `generate_ai_text` and
`generate_finance` — a pure edge addition in `resources/generate_facts_job.yml`.

## Scale & config

Add to `ScaleProfile` (absolute caps, small at smoke, capped at showcase/stress — like the AI text
levers, keeping the operational corpus and synced-table volume controlled at any scale):

- `num_replen_orders` — absolute replenishment-order row target.
- `num_forecast_overrides` — absolute override row target.
- `forecast_serving_rows` — cap on the forecast slice synced to Postgres.

Suggested per-profile values: smoke ~50 / ~30 / ~500; demo_lean ~5k / ~2k / ~50k;
showcase ~50k / ~20k / ~500k; stress ~200k / ~80k / ~2M.

## Wiring

- **`databricks.yml`** — new `database_instances` resource (bundle-provisioned Postgres instance);
  new variables `lakebase_instance` (instance name), `lakebase_database` (default `techmart`),
  `lakebase_capacity` (default `CU_1`). No committed host/credentials (finance/AI discipline).
- **`resources/generate_facts_job.yml`** — add the `generate_ops` notebook task with the right
  `depends_on`.
- **`src/techmart/ops/`** — the module package above.
- **`notebooks/generate_ops.py`** — the serverless notebook.
- **`src/techmart/config.py` + `config/scale_profiles.yaml`** — the three new levers.

## Determinism & referential integrity (unchanged discipline)

- Ids via `xxhash64`; quantities/statuses/reasons via `uniform_hash`/`bounded_int`; timestamps
  derived from a source `date_sk` → `dim_date.date` plus fixed offsets — **never** `rand()`,
  `monotonically_increasing_id()`, `current_timestamp()`, or `uuid()`.
- FKs come from real source rows (snapshot / forecast) and real dims → RI by construction.
- The only non-determinism is the managed synced-table refresh timing; the seeded structure is
  fully reproducible given `randomSeed`.

## Testing (local Spark, mirrors Phase 4/5/6 tests)

- **`build_replenishment_order` / `build_forecast_override`:** schema + grain uniqueness, RI (0
  orphan FKs vs source snapshot/forecast and dims), measure invariants (§Tables), bounded counts
  honor `num_replen_orders` / `num_forecast_overrides`, determinism (same seed/inputs → identical
  structure).
- **`pg_write.pg_ddl` / `pg_type`:** pure string generation — assert type mapping, PK clause, FK
  clauses, and table + column `COMMENT` statements are emitted for each spec.
- **Registry:** `OPS_SPECS` contains both specs with distinct names.
- **Notebook + DAB coverage:** `generate_ops.py` is a Databricks source notebook referencing both
  builders (mirror `test_notebooks.py`); the job resource has a `generate_ops` task with correct
  `depends_on`, and `databricks.yml` declares the instance resource + new vars (mirror
  `test_dab_bundle.py`).
- **Workspace-only (proven-green gate, like `ai_query`):** the actual psycopg write, the synced
  table, and the federation catalog — validated at smoke on field-eng-east: writable tables exist
  with rows + PK/FK + comments, `forecast_serving` populated, and the writable tables queryable
  from the lakehouse via the UC federation catalog.

## Deployment notes

- Follows the finance/AI DEPLOY GOTCHA: `dev` target pins `scale_profile=smoke`; showcase needs
  `bundle deploy --var=scale_profile=showcase` first (base_parameters bake at deploy).
- Requires `--var=lakebase_instance=<name>` and a reachable provisioned instance. Prove green
  end-to-end at smoke on field-eng-east before scaling.

## Out of scope (later phases / follow-ups)

- `techmart_semantic` metric views (Phase 5.4 — its own spec→plan→PR cycle).
- CDC / continuous write-back sync tuning beyond the documented UC-federation read-back.
- The demo app that mutates the operational tables.
- Sweeping the other fact/dim builders for the `randomSeedMethod="fixed"` correlation (a standing
  Phase-6 follow-up, unrelated to this phase).
