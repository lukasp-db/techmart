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

Techmart ships as a Databricks Asset Bundle. Generation runs on **serverless**.

1. Authenticate to your workspace (one-time):

   ```bash
   databricks auth login --host <workspace-url> --profile <profile>
   ```

2. Validate the bundle:

   ```bash
   databricks bundle validate -p <profile>
   ```

3. Deploy and run the generation job (dims → facts, serverless notebooks):

   ```bash
   databricks bundle deploy -p <profile> \
     --var="catalog=<catalog>,schema_prefix=techmart_,scale_profile=showcase"
   databricks bundle run generate_facts -p <profile>
   ```

   For a quick smoke run (tiny data, fast validation):

   ```bash
   databricks bundle deploy -p <profile> \
     --var="catalog=<catalog>,schema_prefix=techmart_,scale_profile=smoke"
   databricks bundle run generate_facts -p <profile>
   ```

The job runs two serverless notebook tasks in sequence: `generate_dims` writes
all dimension tables to `<catalog>.<schema_prefix>core`, then `generate_facts`
reads those dims and writes `fact_sales_line` to the same schema.

Available scale profiles: `smoke` (tiny, ~50k rows), `demo_lean`, `showcase`
(default), `stress`.
