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
