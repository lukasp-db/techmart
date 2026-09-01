# Techmart Retail BI Data Foundation

Synthetic data generator for **Techmart**, a fictitious omnichannel big-box
electronics retailer. Backs the "state-of-the-art BI on Databricks" blog series.

All data is synthetic. This repo contains no real customer data and no secrets.

## Development

```bash
pip install -e ".[dev]"
python -m pytest
```

## Design

See `docs/superpowers/specs/` for the data foundation spec and
`docs/blog-series/` for the accompanying blog notes.

## Deploy to Databricks (DAB)

Techmart ships as a **self-contained** Databricks Asset Bundle. Everything the
pipeline needs is provisioned by the bundle — a serverless SQL warehouse (for the
`ai_query` text-fill) and a Lakebase (managed Postgres) instance plus its Unity
Catalog federation catalog (for the operational write-back). **No pre-existing
warehouse or database, and no `--var`, is required.** Generation runs on
**serverless**.

1. Authenticate to your workspace (one-time):

   ```bash
   databricks auth login --host <workspace-url> --profile <profile>
   ```

2. Deploy and run the full generation pipeline:

   ```bash
   databricks bundle deploy -p <profile>
   databricks bundle run generate_facts -p <profile>
   ```

   The `dev` target defaults to the tiny `smoke` profile and to the
   `stable_classic_ppke9o` catalog. Override per deploy as needed, e.g. a
   different catalog or a larger scale:

   ```bash
   databricks bundle deploy -p <profile> \
     --var="catalog=<your_catalog>,scale_profile=showcase"
   databricks bundle run generate_facts -p <profile>
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
databricks bundle deploy -p <profile>          # provisions infra + jobs
databricks bundle run generate_facts -p <profile>   # creates ai.fact_sales_forecast
databricks bundle deploy -p <profile>          # attaches the synced table (source now exists)
```

Available scale profiles: `smoke` (tiny, fast validation), `demo_lean`,
`showcase` (full demo scale), `stress`.
