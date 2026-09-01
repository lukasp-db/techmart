# Techmart AI (`techmart_ai`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `techmart_ai` schema (`fact_sales_forecast`, `product_review`, `service_case`, `ai_anomaly_catalog`), fix the pre-existing `fact_sales_line` date-spread defect it depends on, and fan the generation job out so `generate_ai` runs concurrently with `generate_finance`.

**Architecture:** Same serverless-native dbldatagen/PySpark model as Phases 3–5. Deterministic structure/measures are built in the `techmart` package (unit-tested against local Spark) and driven by a thin notebook; the LLM text columns are filled by a separate SQL task on a SQL warehouse via `ai_query`. The forecast derives from real `fact_sales_line` weekly actuals with injected, documented anomaly divergence.

**Tech Stack:** Python 3, PySpark 3.5.1, dbldatagen 0.4, Databricks Asset Bundles, Databricks `ai_query` + Foundation Model endpoint, Delta / Unity Catalog, pytest.

**Spec:** `docs/superpowers/specs/2026-09-01-techmart-ai-design.md`

## Global Constraints

- **Engine:** dbldatagen/PySpark only; no Polars. Generation targets Databricks serverless; notebooks are thin wrappers over the tested `techmart` package.
- **Determinism:** all structure/measures use hash-keyed helpers from `src/techmart/facts/gen.py` (`uniform_hash`, `bounded_int`) — never `rand()`, `monotonically_increasing_id()`, `current_timestamp()`, or `uuid()`. The only non-determinism is the `ai_query` prose (two text columns per text table).
- **Referential integrity by construction:** FK values come from real dim/fact rows; forecast keys on real `dim_product`/`dim_store` and a week-end `date_sk` that is always a real `dim_date` row.
- **Comments everywhere:** every table + column carries a `COMMENT`; tables written via `write_table_uc` (attaches column comments + emits table-level grain COMMENT).
- **Catalyst-safe over Connect/serverless:** `values=`/`weights=`, `minValue`/`maxValue`, `expr=`, `explode(sequence(...))`, `percentNulls=`, `omit=True`; always set `partitions` explicitly on any dbldatagen generator; `randomSeedMethod="hash_fieldname"` for multi-random-column generators (see Task 1).
- **Secret-free bundle:** no committed workspace host; `warehouse_id` has no committed default (supplied per-deploy like `host`).
- **CLI:** use `/opt/homebrew/bin/databricks` (v1.6.0) when deploying; do NOT set `DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION`.
- **Deploy gotcha:** `dev` target pins `scale_profile=smoke`; showcase needs `bundle deploy --var=scale_profile=showcase` first (base_parameters bake at deploy).
- **Test runner:** `python -m pytest` from repo root; local Spark session comes from the `spark` fixture in `tests/conftest.py`.

---

### Task 1: Fix `fact_sales_line` date-spread correlation

Root cause (confirmed empirically): the transaction-header `DataGenerator` uses `randomSeedMethod="fixed"`, which makes all `random=True` columns share one seeded stream, so `date_sk` becomes ~a function of `store_sk`. At 1000 stores each store lands on ~2 distinct dates. Fix: `randomSeedMethod="hash_fieldname"` (independent per-column streams, still deterministic).

**Files:**
- Modify: `src/techmart/facts/fact_sales_line.py:79`
- Test: `tests/test_fact_sales_line_date_spread.py` (create)

**Interfaces:**
- Consumes: `build_fact_sales_line(spark, config, *, dim_product, dim_date, dim_counts, rows=None, seed=None) -> DataFrame` (existing).
- Produces: no signature change; `date_sk` now decorrelated from `store_sk`.

- [ ] **Step 1: Write the failing regression test**

```python
# tests/test_fact_sales_line_date_spread.py
import dataclasses
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line


def _cfg(history_years: int) -> TechmartConfig:
    sp = ScaleProfile("t", num_stores=200, num_skus=60, history_years=history_years,
                      sales_lines_target=60000, num_customers=2000, num_vendors=20)
    return TechmartConfig(scale_profile=sp, seed=42, output_dir=Path("data"),
                          catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def test_dates_are_decorrelated_from_store(spark):
    cfg = _cfg(history_years=3)
    dd = build_dim_date(spark, cfg)
    dp = build_dim_product(spark, cfg)
    counts = {"store": 200, "customer": 2000, "employee": cfg.scale_profile.num_employees,
              "promotion": cfg.scale_profile.num_promotions, "product": 60}
    df = build_fact_sales_line(spark, cfg, dim_product=dp, dim_date=dd,
                               dim_counts=counts, rows=60000)
    per_store = (df.groupBy("store_sk")
                   .agg(F.countDistinct("date_sk").alias("d"))
                   .agg(F.avg("d").alias("avg_d")).first()["avg_d"])
    # ~100 txns/store over ~1097 dates: correlated build gives ~2; decorrelated gives dozens.
    assert per_store > 10, f"per-store distinct dates collapsed to {per_store:.1f} (date/store correlation)"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_fact_sales_line_date_spread.py -v`
Expected: FAIL — `per_store` ≈ 2, assertion error "collapsed to ~2".

- [ ] **Step 3: Apply the one-line fix**

In `src/techmart/facts/fact_sales_line.py`, in the `dg.DataGenerator(...)` header constructor, change:

```python
            randomSeedMethod="fixed",
```

to:

```python
            randomSeedMethod="hash_fieldname",
```

- [ ] **Step 4: Run tests to verify the fix and no regressions**

Run: `python -m pytest tests/test_fact_sales_line_date_spread.py tests/test_fact_sales_line.py tests/test_facts_gen.py -v`
Expected: PASS (new spread test passes; existing sales-line + basket-coherence tests still pass).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/facts/fact_sales_line.py tests/test_fact_sales_line_date_spread.py
git commit -m "fix(fact_sales_line): decorrelate date_sk from store_sk via hash_fieldname seeding"
```

---

### Task 2: AI scale levers on `ScaleProfile`

Add absolute-count and forecast-bounding levers with defaults (so existing profiles/tests keep working), plus per-profile values in the YAML.

**Files:**
- Modify: `src/techmart/config.py` (`ScaleProfile` dataclass)
- Modify: `config/scale_profiles.yaml`
- Test: `tests/test_config.py` (add cases)

**Interfaces:**
- Produces: `ScaleProfile.num_reviews: int`, `.num_service_cases: int`, `.forecast_active_products: int`, `.forecast_horizon_weeks: int` (all with defaults).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_config.py
from pathlib import Path
from techmart.config import load_profiles


def test_ai_levers_have_defaults_and_profile_values():
    profiles = load_profiles(Path("config/scale_profiles.yaml"))
    smoke = profiles["smoke"]
    show = profiles["showcase"]
    # defaults exist on the dataclass
    assert smoke.num_reviews >= 1 and smoke.num_service_cases >= 1
    assert smoke.forecast_active_products >= 1 and smoke.forecast_horizon_weeks >= 1
    # showcase corpora are bounded (absolute counts, not a fraction of 750M lines)
    assert show.num_reviews <= 200_000
    assert show.num_service_cases <= 100_000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config.py::test_ai_levers_have_defaults_and_profile_values -v`
Expected: FAIL — `AttributeError: 'ScaleProfile' object has no attribute 'num_reviews'`.

- [ ] **Step 3: Add the fields and YAML values**

In `src/techmart/config.py`, add to `ScaleProfile` (after the finance levers, keeping all defaulted so `load_profiles(**cfg)` still works):

```python
    # AI layer levers (Phase 6).
    num_reviews: int = 200
    num_service_cases: int = 100
    forecast_active_products: int = 200
    forecast_horizon_weeks: int = 26
```

In `config/scale_profiles.yaml`, add these keys under each profile (values below; keep existing keys):

```yaml
  demo_lean:
    num_reviews: 20000
    num_service_cases: 8000
    forecast_active_products: 2000
    forecast_horizon_weeks: 52
  showcase:
    num_reviews: 100000
    num_service_cases: 40000
    forecast_active_products: 5000
    forecast_horizon_weeks: 52
  smoke:
    num_reviews: 200
    num_service_cases: 100
    forecast_active_products: 100
    forecast_horizon_weeks: 26
  stress:
    num_reviews: 200000
    num_service_cases: 100000
    forecast_active_products: 10000
    forecast_horizon_weeks: 78
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/config.py config/scale_profiles.yaml tests/test_config.py
git commit -m "feat(config): add techmart_ai scale levers (reviews/cases/forecast bounds)"
```

---

### Task 3: Anomaly windows + `ai_anomaly_catalog`

A reference module that resolves anomaly windows against `dim_date` and builds the catalog table. Forecast (Task 4) consumes the window helper.

**Files:**
- Create: `src/techmart/ai/__init__.py` (empty)
- Create: `src/techmart/ai/anomalies.py`
- Test: `tests/test_ai_anomalies.py`

**Interfaces:**
- Produces:
  - `AI_ANOMALY_CATALOG_SPEC: SparkTableSpec`
  - `SUPPLY_PERIOD: int` (fiscal period the supply disruption falls in)
  - `week_calendar(dim_date) -> DataFrame` with columns `fiscal_year, fiscal_week, date_sk` (week-end), `fiscal_period`, `is_holiday_week` (bool).
  - `build_ai_anomaly_catalog(spark, config, *, dim_date) -> DataFrame` matching the spec.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ai_anomalies.py
import dataclasses
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.ai.anomalies import (
    AI_ANOMALY_CATALOG_SPEC, week_calendar, build_ai_anomaly_catalog,
)

_P = ScaleProfile("t", 8, 40, 3, 4000, 300, 20)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def test_week_calendar_gives_real_week_end_date_sks(spark):
    dd = build_dim_date(spark, _CFG)
    wc = week_calendar(spark, dd)
    # every week-end date_sk is a real dim_date row
    assert wc.select("date_sk").join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
    # holiday weeks exist over a 3-year horizon
    assert wc.filter(F.col("is_holiday_week")).count() > 0


def test_catalog_documents_five_anomalies_with_two_realized(spark):
    dd = build_dim_date(spark, _CFG)
    cat = build_ai_anomaly_catalog(spark, _CFG, dim_date=dd)
    assert cat.columns == AI_ANOMALY_CATALOG_SPEC.column_names
    assert cat.count() == 5
    assert cat.filter(F.col("realized_in") == "fact_sales_forecast").count() == 2
    # windows are real dim_date rows
    assert cat.select("start_date_sk").withColumnRenamed("start_date_sk", "date_sk") \
        .join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ai_anomalies.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.ai'`.

- [ ] **Step 3: Implement the module**

```python
# src/techmart/ai/__init__.py
```

```python
# src/techmart/ai/anomalies.py
"""Anomaly windows (resolved against dim_date) and the ai_anomaly_catalog table."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec

# The supply disruption is realized in this fiscal period of the latest fiscal year.
SUPPLY_PERIOD = 6

AI_ANOMALY_CATALOG_SPEC = SparkTableSpec(
    schema="ai",
    name="ai_anomaly_catalog",
    grain="one row per documented anomaly",
    columns=[
        SparkColumn("anomaly_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("anomaly_type", "string", "Anomaly category", nullable=False),
        SparkColumn("description", "string", "Human-readable narrative"),
        SparkColumn("start_date_sk", "long", "Window start date FK (dim_date)", is_key=True),
        SparkColumn("end_date_sk", "long", "Window end date FK (dim_date)", is_key=True),
        SparkColumn("affected_dimension", "string", "Dimension the anomaly acts on"),
        SparkColumn("expected_signal", "string", "What a detector should observe"),
        SparkColumn("realized_in", "string", "Where the signal is materialized"),
    ],
)


def week_calendar(spark: SparkSession, dim_date: DataFrame) -> DataFrame:
    """Per (fiscal_year, fiscal_week): week-end date_sk, fiscal_period, is_holiday_week."""
    return (
        dim_date.groupBy("fiscal_year", "fiscal_week").agg(
            F.max("date_sk").alias("date_sk"),
            F.max("fiscal_period").alias("fiscal_period"),
            F.max(F.when(F.col("selling_season") == "Holiday", F.lit(1)).otherwise(F.lit(0)))
                .alias("_hol"),
        )
        .withColumn("is_holiday_week", F.col("_hol") == F.lit(1))
        .drop("_hol")
    )


def build_ai_anomaly_catalog(
    spark: SparkSession, config: TechmartConfig, *, dim_date: DataFrame
) -> DataFrame:
    max_fy = dim_date.agg(F.max("fiscal_year")).first()[0]

    # Holiday-demand-spike window: the Holiday selling season of the latest fiscal year.
    hol = dim_date.filter(
        (F.col("fiscal_year") == F.lit(max_fy)) & (F.col("selling_season") == "Holiday")
    ).agg(F.min("date_sk").alias("s"), F.max("date_sk").alias("e")).first()

    # Vendor-supply-disruption window: SUPPLY_PERIOD of the latest fiscal year.
    sup = dim_date.filter(
        (F.col("fiscal_year") == F.lit(max_fy)) & (F.col("fiscal_period") == F.lit(SUPPLY_PERIOD))
    ).agg(F.min("date_sk").alias("s"), F.max("date_sk").alias("e")).first()

    rows = [
        (1, "holiday-demand-spike",
         "Under-forecast demand during the holiday selling season.",
         int(hol["s"]), int(hol["e"]), "product/category",
         "forecast_qty << actual for holiday weeks (baseline)", "fact_sales_forecast"),
        (2, "vendor-supply-disruption",
         "Stockouts suppress actual demand; naive forecast over-forecasts.",
         int(sup["s"]), int(sup["e"]), "vendor/product-band",
         "forecast_qty >> actual for the disruption period (baseline)", "fact_sales_forecast"),
        (3, "pricing-error",
         "A margin dip from a mispriced subcategory (documented; core-fact injection deferred).",
         int(sup["s"]), int(sup["e"]), "product/subcategory",
         "gross_margin dip localized to a subcategory", "catalog-only"),
        (4, "return-fraud-cluster",
         "A cluster of suspicious returns (documented; core-fact injection deferred).",
         int(sup["s"]), int(sup["e"]), "customer/store",
         "elevated is_fraud_suspected returns in a store", "catalog-only"),
        (5, "data-quality-blemish",
         "A data-quality blemish for cleansing demos (documented; injection deferred).",
         int(sup["s"]), int(sup["e"]), "misc",
         "null/So outlier rows in a narrow window", "catalog-only"),
    ]
    df = spark.createDataFrame(
        rows,
        "anomaly_id long, anomaly_type string, description string, start_date_sk long, "
        "end_date_sk long, affected_dimension string, expected_signal string, realized_in string",
    )
    return AI_ANOMALY_CATALOG_SPEC.select_ordered(df)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_ai_anomalies.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ai/__init__.py src/techmart/ai/anomalies.py tests/test_ai_anomalies.py
git commit -m "feat(ai): add anomaly windows + ai_anomaly_catalog"
```

---

### Task 4: `fact_sales_forecast`

Weekly forecast derived from `fact_sales_line` actuals with two versions and injected anomaly divergence.

**Files:**
- Create: `src/techmart/ai/fact_sales_forecast.py`
- Test: `tests/test_fact_sales_forecast.py`

**Interfaces:**
- Consumes: `week_calendar`, `SUPPLY_PERIOD` (Task 3); `uniform_hash` (`facts/gen.py`).
- Produces:
  - `FACT_SALES_FORECAST_SPEC: SparkTableSpec`
  - `FORECAST_VERSIONS: tuple[str, str] = ("baseline", "improved")`
  - `build_fact_sales_forecast(spark, config, *, fact_sales_line, dim_date) -> DataFrame`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_fact_sales_forecast.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.ai.fact_sales_forecast import (
    FACT_SALES_FORECAST_SPEC, FORECAST_VERSIONS, build_fact_sales_forecast,
)

_P = ScaleProfile("t", 20, 60, 3, 60000, 2000, 20,
                  forecast_active_products=60, forecast_horizon_weeks=26)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 20, "customer": 2000, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 60}


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=60000)
    return build_fact_sales_forecast(spark, _CFG, fact_sales_line=sales, dim_date=dd), dd


def test_schema_grain_and_versions(spark):
    df, _ = _build(spark)
    assert df.columns == FACT_SALES_FORECAST_SPEC.column_names
    grain = ["product_sk", "store_sk", "date_sk", "forecast_version"]
    assert df.groupBy(*grain).count().filter(F.col("count") > 1).count() == 0
    assert {r[0] for r in df.select("forecast_version").distinct().collect()} == set(FORECAST_VERSIONS)


def test_interval_ordering_and_ri(spark):
    df, dd = _build(spark)
    assert df.filter(~((F.col("lower_bound") <= F.col("forecast_qty")) &
                       (F.col("forecast_qty") <= F.col("upper_bound")))).count() == 0
    assert df.select("date_sk").distinct().join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*"), F.round(F.sum("forecast_qty"), 2)).first()
    b = _build(spark)[0].agg(F.count("*"), F.round(F.sum("forecast_qty"), 2)).first()
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_fact_sales_forecast.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.ai.fact_sales_forecast'`.

- [ ] **Step 3: Implement the builder**

```python
# src/techmart/ai/fact_sales_forecast.py
"""Weekly demand forecast derived from fact_sales_line, with injected anomaly divergence."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec
from .anomalies import SUPPLY_PERIOD, week_calendar

FORECAST_VERSIONS: tuple[str, str] = ("baseline", "improved")
_INTERVAL_BAND = 0.15  # ±15% prediction interval

FACT_SALES_FORECAST_SPEC = SparkTableSpec(
    schema="ai",
    name="fact_sales_forecast",
    grain="one row per product x store x fiscal week x forecast version",
    columns=[
        SparkColumn("date_sk", "long", "Week-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("forecast_version", "string", "Forecast model version", nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_week", "int", "Retail fiscal week"),
        SparkColumn("forecast_qty", "double", "Projected units"),
        SparkColumn("forecast_amount", "double", "Projected net sales amount"),
        SparkColumn("lower_bound", "double", "Lower prediction bound (qty)"),
        SparkColumn("upper_bound", "double", "Upper prediction bound (qty)"),
        SparkColumn("model_name", "string", "Forecast model name"),
        SparkColumn("forecast_generated_date", "date", "As-of date the forecast was produced"),
    ],
)


def build_fact_sales_forecast(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile
    wc = week_calendar(spark, dim_date)  # fiscal_year, fiscal_week, date_sk, fiscal_period, is_holiday_week

    # --- weekly actuals for active products ---
    fw = dim_date.select("date_sk", "fiscal_year", "fiscal_week")
    agg = (
        fact_sales_line
        .filter(F.col("product_sk") <= F.lit(sp.forecast_active_products))
        .join(fw, "date_sk")
        .groupBy("product_sk", "store_sk", "fiscal_year", "fiscal_week")
        .agg(F.sum("quantity").alias("actual_qty"),
             F.sum("net_sales_amount").alias("actual_net"))
    )

    # --- restrict to the most recent forecast_horizon_weeks distinct weeks ---
    weeks = (
        agg.select("fiscal_year", "fiscal_week").distinct()
        .orderBy(F.col("fiscal_year").desc(), F.col("fiscal_week").desc())
        .limit(sp.forecast_horizon_weeks)
    )
    agg = agg.join(F.broadcast(weeks), ["fiscal_year", "fiscal_week"])

    # --- attach week-end date_sk + anomaly flags ---
    base = agg.join(F.broadcast(wc), ["fiscal_year", "fiscal_week"])
    max_fy = dim_date.agg(F.max("fiscal_year")).first()[0]
    is_holiday = F.col("is_holiday_week") & (F.col("fiscal_year") == F.lit(max_fy))
    is_supply = (
        (F.col("fiscal_period") == F.lit(SUPPLY_PERIOD))
        & (F.col("fiscal_year") == F.lit(max_fy))
        & (F.col("product_sk") <= F.lit(max(1, sp.forecast_active_products // 4)))
    )
    anomaly_mult = F.when(is_holiday, F.lit(0.6)).when(is_supply, F.lit(1.4)).otherwise(F.lit(1.0))
    bias = uniform_hash(F.col("product_sk"), F.col("store_sk"), F.col("fiscal_week"),
                        salt="bias") * F.lit(0.10) - F.lit(0.05)  # ±5%

    # --- explode into forecast versions ---
    versioned = base.withColumn("forecast_version",
                                F.explode(F.array(*[F.lit(v) for v in FORECAST_VERSIONS])))
    avg_price = F.col("actual_net") / F.greatest(F.col("actual_qty"), F.lit(1))
    qty = F.when(
        F.col("forecast_version") == F.lit("baseline"),
        F.col("actual_qty") * (F.lit(1.0) + bias) * anomaly_mult,
    ).otherwise(F.col("actual_qty") * (F.lit(1.0) + bias * F.lit(0.4)))

    df = (
        versioned
        .withColumn("forecast_qty", F.round(F.greatest(qty, F.lit(0.0)), 2))
        .withColumn("forecast_amount", F.round(F.col("forecast_qty") * avg_price, 2))
        .withColumn("lower_bound", F.round(F.col("forecast_qty") * F.lit(1.0 - _INTERVAL_BAND), 2))
        .withColumn("upper_bound", F.round(F.col("forecast_qty") * F.lit(1.0 + _INTERVAL_BAND), 2))
        .withColumn("model_name", F.when(F.col("forecast_version") == F.lit("baseline"),
                                         F.lit("seasonal_naive_v1")).otherwise(F.lit("gbt_v2")))
        .withColumn("forecast_generated_date", F.lit(config.end_date).cast("date"))
    )
    return FACT_SALES_FORECAST_SPEC.select_ordered(df)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_fact_sales_forecast.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ai/fact_sales_forecast.py tests/test_fact_sales_forecast.py
git commit -m "feat(ai): add fact_sales_forecast with versioned, anomaly-aware forecasts"
```

---

### Task 5: `product_review` (structure + prompts)

Builds every column except the LLM text; attaches deterministic `prompt` (review body) and `title_prompt` columns for the SQL fill task. Reviews attach to real sampled sales lines.

**Files:**
- Create: `src/techmart/ai/product_review.py`
- Test: `tests/test_product_review.py`

**Interfaces:**
- Consumes: `uniform_hash`, `bounded_int` (`facts/gen.py`).
- Produces:
  - `PRODUCT_REVIEW_SPEC: SparkTableSpec` (final table: `review_text`, `review_title` are the LLM columns).
  - `PRODUCT_REVIEW_STAGING_SPEC: SparkTableSpec` (structure + `prompt`, `title_prompt`; name `_product_review_staging`).
  - `build_product_review_staging(spark, config, *, fact_sales_line, dim_product, dim_date) -> DataFrame`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_product_review.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.ai.product_review import (
    PRODUCT_REVIEW_SPEC, PRODUCT_REVIEW_STAGING_SPEC, build_product_review_staging,
)

_P = ScaleProfile("t", 20, 60, 2, 40000, 2000, 20, num_reviews=150)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 20, "customer": 2000, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 60}


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=40000)
    return build_product_review_staging(spark, _CFG, fact_sales_line=sales,
                                        dim_product=dp, dim_date=dd), dp


def test_staging_schema_bounded_count_and_prompts(spark):
    df, _ = _build(spark)
    assert df.columns == PRODUCT_REVIEW_STAGING_SPEC.column_names
    # bounded by num_reviews
    assert df.count() <= _P.num_reviews
    # prompts are non-empty; final text columns are NOT present in staging
    assert df.filter((F.length("prompt") == 0) | F.col("prompt").isNull()).count() == 0
    assert "review_text" not in df.columns
    # the staging columns minus the two prompts equal the final columns minus the two text cols
    assert set(PRODUCT_REVIEW_STAGING_SPEC.column_names) - {"prompt", "title_prompt"} == \
           set(PRODUCT_REVIEW_SPEC.column_names) - {"review_text", "review_title"}


def test_verified_purchase_and_ri(spark):
    df, dp = _build(spark)
    assert df.filter(~F.col("verified_purchase")).count() == 0  # all tied to real sales
    assert df.select("product_sk").distinct() \
        .join(dp.select("product_sk"), "product_sk", "left_anti").count() == 0
    assert df.filter((F.col("rating") < 1) | (F.col("rating") > 5)).count() == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_product_review.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.ai.product_review'`.

- [ ] **Step 3: Implement the builder**

```python
# src/techmart/ai/product_review.py
"""product_review: deterministic structure + ai_query prompts (text filled by the SQL task)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec

# Final table (post ai_query fill).
PRODUCT_REVIEW_SPEC = SparkTableSpec(
    schema="ai",
    name="product_review",
    grain="one row per product review",
    columns=[
        SparkColumn("review_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Review date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("rating", "int", "Star rating 1-5", nullable=False),
        SparkColumn("review_title", "string", "LLM-generated review title"),
        SparkColumn("review_text", "string", "LLM-generated review body"),
        SparkColumn("verified_purchase", "boolean", "Tied to a real purchase", nullable=False),
        SparkColumn("helpful_votes", "int", "Helpful-vote count"),
    ],
)

# Staging table (structure + prompts; text columns absent).
PRODUCT_REVIEW_STAGING_SPEC = SparkTableSpec(
    schema="ai",
    name="_product_review_staging",
    grain="staging: product review structure + ai_query prompts",
    columns=[
        SparkColumn("review_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Review date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("rating", "int", "Star rating 1-5", nullable=False),
        SparkColumn("verified_purchase", "boolean", "Tied to a real purchase", nullable=False),
        SparkColumn("helpful_votes", "int", "Helpful-vote count"),
        SparkColumn("prompt", "string", "ai_query prompt for the review body", nullable=False),
        SparkColumn("title_prompt", "string", "ai_query prompt for the review title", nullable=False),
    ],
)


def build_product_review_staging(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    # Deterministically sample real sales lines up to num_reviews.
    total = fact_sales_line.count()
    frac = min(1.0, (sp.num_reviews * 3.0) / max(total, 1))
    pick = uniform_hash(F.col("transaction_id"), F.col("line_number"), salt="review_pick")
    candidates = (
        fact_sales_line
        .select("transaction_id", "line_number", "product_sk", "customer_sk", "date_sk")
        .withColumn("_r", pick)
        .filter(F.col("_r") < F.lit(frac))
        .orderBy("_r", "transaction_id", "line_number")
        .limit(sp.num_reviews)
    )

    prod = dim_product.select(
        F.col("product_sk").alias("_p"), "product_name", "category_name"
    )
    j = candidates.join(prod, candidates["product_sk"] == prod["_p"], "left").drop("_p")

    rating = bounded_int(F.col("transaction_id"), F.col("line_number"), salt="rating", lo=1, hi=5)
    # skew toward 4-5: map a second uniform through a simple weighting
    skew = uniform_hash(F.col("transaction_id"), F.col("line_number"), salt="skew")
    rating = F.when(skew < F.lit(0.7), F.greatest(rating, F.lit(4))).otherwise(rating)

    # Deterministic review_id from stable keys (never monotonically_increasing_id / uuid).
    df = (
        j
        .withColumn("rating", rating)
        .withColumn("review_id", F.xxhash64(F.col("transaction_id"), F.col("line_number"), F.lit("review")))
        .withColumn("verified_purchase", F.lit(True))
        .withColumn("helpful_votes",
                    bounded_int(F.col("transaction_id"), F.col("line_number"), salt="votes", lo=0, hi=50))
        .withColumn(
            "prompt",
            F.concat(
                F.lit("Write a concise, realistic customer review body (2-4 sentences) for the product '"),
                F.coalesce(F.col("product_name"), F.lit("this product")),
                F.lit("' in the category '"),
                F.coalesce(F.col("category_name"), F.lit("electronics")),
                F.lit("'. The reviewer gave "), F.col("rating").cast("string"),
                F.lit(" out of 5 stars. Match the sentiment to the rating. Do not include a title or rating."),
            ),
        )
        .withColumn(
            "title_prompt",
            F.concat(
                F.lit("Write a short review title (max 8 words) for a "),
                F.col("rating").cast("string"),
                F.lit("-star review of '"),
                F.coalesce(F.col("product_name"), F.lit("this product")),
                F.lit("'. Title only, no quotes."),
            ),
        )
    )
    return PRODUCT_REVIEW_STAGING_SPEC.select_ordered(df)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_product_review.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ai/product_review.py tests/test_product_review.py
git commit -m "feat(ai): add product_review staging (structure + ai_query prompts)"
```

---

### Task 6: `service_case` (structure + prompts)

Structure + prompts for the "Geek Squad" service-case corpus; `resolution_notes` prompt only when the case is Resolved/Closed.

**Files:**
- Create: `src/techmart/ai/service_case.py`
- Test: `tests/test_service_case.py`

**Interfaces:**
- Consumes: `uniform_hash`, `bounded_int` (`facts/gen.py`); `shifted_date_sk` is not needed.
- Produces:
  - `SERVICE_CASE_SPEC: SparkTableSpec` (final: `case_notes`, `resolution_notes` are LLM columns).
  - `SERVICE_CASE_STAGING_SPEC: SparkTableSpec` (name `_service_case_staging`; adds `notes_prompt`, `resolution_prompt`).
  - `build_service_case_staging(spark, config, *, fact_sales_line, dim_date) -> DataFrame`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_service_case.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.ai.service_case import (
    SERVICE_CASE_SPEC, SERVICE_CASE_STAGING_SPEC, build_service_case_staging,
)

_P = ScaleProfile("t", 20, 60, 2, 40000, 2000, 20, num_service_cases=120)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 20, "customer": 2000, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 60}


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=40000)
    return build_service_case_staging(spark, _CFG, fact_sales_line=sales, dim_date=dd)


def test_staging_schema_bounded_and_prompts(spark):
    df = _build(spark)
    assert df.columns == SERVICE_CASE_STAGING_SPEC.column_names
    assert df.count() <= _P.num_service_cases
    assert df.filter((F.length("notes_prompt") == 0) | F.col("notes_prompt").isNull()).count() == 0
    # resolution_prompt is null exactly when status is Open/In-Progress
    open_like = F.col("status").isin("Open", "In-Progress")
    assert df.filter(open_like & F.col("resolution_prompt").isNotNull()).count() == 0
    assert df.filter(~open_like & F.col("resolution_prompt").isNull()).count() == 0


def test_domain_values(spark):
    df = _build(spark)
    assert df.filter(~F.col("case_type").isin("Repair", "Warranty", "Support")).count() == 0
    assert df.filter((F.col("csat_score") < 1) | (F.col("csat_score") > 5)).count() == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_service_case.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.ai.service_case'`.

- [ ] **Step 3: Implement the builder**

```python
# src/techmart/ai/service_case.py
"""service_case: deterministic structure + ai_query prompts (text filled by the SQL task)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec

SERVICE_CASE_SPEC = SparkTableSpec(
    schema="ai",
    name="service_case",
    grain="one row per service case",
    columns=[
        SparkColumn("case_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Case open date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("case_type", "string", "Repair/Warranty/Support", nullable=False),
        SparkColumn("channel", "string", "Phone/In-Store/Online", nullable=False),
        SparkColumn("status", "string", "Open/In-Progress/Resolved/Closed", nullable=False),
        SparkColumn("case_notes", "string", "LLM-generated case notes"),
        SparkColumn("resolution_notes", "string", "LLM-generated resolution (null if unresolved)"),
        SparkColumn("csat_score", "int", "Customer satisfaction 1-5", nullable=False),
    ],
)

SERVICE_CASE_STAGING_SPEC = SparkTableSpec(
    schema="ai",
    name="_service_case_staging",
    grain="staging: service case structure + ai_query prompts",
    columns=[
        SparkColumn("case_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("date_sk", "long", "Case open date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("case_type", "string", "Repair/Warranty/Support", nullable=False),
        SparkColumn("channel", "string", "Phone/In-Store/Online", nullable=False),
        SparkColumn("status", "string", "Open/In-Progress/Resolved/Closed", nullable=False),
        SparkColumn("csat_score", "int", "Customer satisfaction 1-5", nullable=False),
        SparkColumn("notes_prompt", "string", "ai_query prompt for case notes", nullable=False),
        SparkColumn("resolution_prompt", "string", "ai_query prompt for resolution (null if unresolved)"),
    ],
)

_CASE_TYPES = ("Repair", "Warranty", "Support")
_CHANNELS = ("Phone", "In-Store", "Online")
_STATUSES = ("Open", "In-Progress", "Resolved", "Closed")


def build_service_case_staging(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    # Sample real (customer, product, store, date) tuples from sales lines up to num_service_cases.
    total = fact_sales_line.count()
    frac = min(1.0, (sp.num_service_cases * 3.0) / max(total, 1))
    pick = uniform_hash(F.col("transaction_id"), F.col("line_number"), salt="case_pick")
    src = (
        fact_sales_line
        .select("transaction_id", "line_number", "product_sk", "customer_sk", "store_sk", "date_sk")
        .withColumn("_r", pick)
        .filter(F.col("_r") < F.lit(frac))
        .orderBy("_r", "transaction_id", "line_number")
        .limit(sp.num_service_cases)
    )

    def _pick(salt: str, values: tuple[str, ...]):
        idx = bounded_int(F.col("transaction_id"), F.col("line_number"), salt=salt,
                          lo=1, hi=len(values))
        return F.element_at(F.array(*[F.lit(v) for v in values]), idx)

    df = (
        src
        .withColumn("case_id", F.xxhash64(F.col("transaction_id"), F.col("line_number"), F.lit("case")))
        .withColumn("case_type", _pick("ctype", _CASE_TYPES))
        .withColumn("channel", _pick("cchan", _CHANNELS))
        .withColumn("status", _pick("cstat", _STATUSES))
        .withColumn("csat_score",
                    bounded_int(F.col("transaction_id"), F.col("line_number"), salt="csat", lo=1, hi=5))
        .withColumn(
            "notes_prompt",
            F.concat(
                F.lit("Write brief support-case notes (1-3 sentences) for a "),
                F.col("case_type"),
                F.lit(" case opened via "), F.col("channel"),
                F.lit(". Describe the customer's reported issue for a consumer-electronics product."),
            ),
        )
        .withColumn(
            "resolution_prompt",
            F.when(
                F.col("status").isin("Resolved", "Closed"),
                F.concat(
                    F.lit("Write a brief resolution note (1-2 sentences) for a resolved "),
                    F.col("case_type"), F.lit(" case. State how it was fixed."),
                ),
            ).otherwise(F.lit(None).cast("string")),
        )
    )
    return SERVICE_CASE_STAGING_SPEC.select_ordered(df)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_service_case.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ai/service_case.py tests/test_service_case.py
git commit -m "feat(ai): add service_case staging (structure + ai_query prompts)"
```

---

### Task 7: `techmart_ai` registry

Collect the AI specs, mirroring `finance/registry.py`.

**Files:**
- Create: `src/techmart/ai/registry.py`
- Test: `tests/test_ai_registry.py`

**Interfaces:**
- Consumes: all `*_SPEC` from Tasks 3–6.
- Produces: `AI_SPECS: list[SparkTableSpec]` (final tables + catalog + the two staging specs).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ai_registry.py
from techmart.ai.registry import AI_SPECS


def test_registry_covers_ai_tables():
    names = {s.name for s in AI_SPECS}
    assert {"fact_sales_forecast", "product_review", "service_case", "ai_anomaly_catalog",
            "_product_review_staging", "_service_case_staging"} <= names
    assert all(s.schema == "ai" for s in AI_SPECS)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ai_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.ai.registry'`.

- [ ] **Step 3: Implement the registry**

```python
# src/techmart/ai/registry.py
"""Registry of techmart_ai table specs."""
from __future__ import annotations

from ..spark.framework import SparkTableSpec
from .anomalies import AI_ANOMALY_CATALOG_SPEC
from .fact_sales_forecast import FACT_SALES_FORECAST_SPEC
from .product_review import PRODUCT_REVIEW_SPEC, PRODUCT_REVIEW_STAGING_SPEC
from .service_case import SERVICE_CASE_SPEC, SERVICE_CASE_STAGING_SPEC

AI_SPECS: list[SparkTableSpec] = [
    FACT_SALES_FORECAST_SPEC,
    AI_ANOMALY_CATALOG_SPEC,
    PRODUCT_REVIEW_STAGING_SPEC,
    SERVICE_CASE_STAGING_SPEC,
    PRODUCT_REVIEW_SPEC,
    SERVICE_CASE_SPEC,
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_ai_registry.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ai/registry.py tests/test_ai_registry.py
git commit -m "feat(ai): add techmart_ai table registry"
```

---

### Task 8: `generate_ai` notebook

Thin serverless notebook: reads persisted core tables, writes `fact_sales_forecast`, `ai_anomaly_catalog`, and the two staging tables. (Text fill happens in the SQL task, Task 9.)

**Files:**
- Create: `notebooks/generate_ai.py`
- Test: `tests/test_notebooks.py` (add coverage)

**Interfaces:**
- Consumes: builders from Tasks 3–6; `write_table_uc`.
- Produces: source notebook at `notebooks/generate_ai.py`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_notebooks.py
def test_generate_ai_notebook_covers_builders():
    text = _read("generate_ai.py")
    assert text.splitlines()[0] == "# Databricks notebook source"
    assert "dbutils.widgets" in text
    assert "write_table_uc" in text
    for b in ["build_fact_sales_forecast", "build_ai_anomaly_catalog",
              "build_product_review_staging", "build_service_case_staging"]:
        assert b in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_notebooks.py::test_generate_ai_notebook_covers_builders -v`
Expected: FAIL — `FileNotFoundError`/read error for `generate_ai.py`.

- [ ] **Step 3: Create the notebook**

```python
# notebooks/generate_ai.py
# Databricks notebook source
# MAGIC %pip install dbldatagen jmespath pyparsing
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import sys
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")
dbutils.widgets.text("seed", "42")
# COMMAND ----------
from pathlib import Path
from techmart.config import load_config
from techmart.spark.uc_write import write_table_uc
from techmart.ai.anomalies import AI_ANOMALY_CATALOG_SPEC, build_ai_anomaly_catalog
from techmart.ai.fact_sales_forecast import FACT_SALES_FORECAST_SPEC, build_fact_sales_forecast
from techmart.ai.product_review import PRODUCT_REVIEW_STAGING_SPEC, build_product_review_staging
from techmart.ai.service_case import SERVICE_CASE_STAGING_SPEC, build_service_case_staging

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
ai = f"{catalog}.{schema_prefix}ai"

dim_date = spark.read.table(f"{core}.dim_date")
dim_product = spark.read.table(f"{core}.dim_product")
sales = spark.read.table(f"{core}.fact_sales_line")

# --- anomaly catalog ---
print("wrote", write_table_uc(spark, build_ai_anomaly_catalog(spark, config, dim_date=dim_date),
                              AI_ANOMALY_CATALOG_SPEC, catalog, schema_prefix))

# --- forecast (derived from sales actuals) ---
fc = build_fact_sales_forecast(spark, config, fact_sales_line=sales, dim_date=dim_date)
print("wrote", write_table_uc(spark, fc, FACT_SALES_FORECAST_SPEC, catalog, schema_prefix))

# --- review/case staging (text filled by the generate_ai_text SQL task) ---
rev = build_product_review_staging(spark, config, fact_sales_line=sales,
                                   dim_product=dim_product, dim_date=dim_date)
print("wrote", write_table_uc(spark, rev, PRODUCT_REVIEW_STAGING_SPEC, catalog, schema_prefix))
cases = build_service_case_staging(spark, config, fact_sales_line=sales, dim_date=dim_date)
print("wrote", write_table_uc(spark, cases, SERVICE_CASE_STAGING_SPEC, catalog, schema_prefix))

for t in ("ai_anomaly_catalog", "fact_sales_forecast",
          "_product_review_staging", "_service_case_staging"):
    print(t, spark.table(f"{ai}.{t}").count())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_notebooks.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add notebooks/generate_ai.py tests/test_notebooks.py
git commit -m "feat(ai): add generate_ai serverless notebook"
```

---

### Task 9: SQL text-fill task + job fan-out + bundle variables

Add the `ai_query` SQL file, wire `generate_ai` (parallel to `generate_finance`) and `generate_ai_text` (SQL task on a warehouse) into the job, and add the `warehouse_id` / `llm_endpoint` bundle variables.

**Files:**
- Create: `resources/generate_ai_text.sql`
- Modify: `resources/generate_facts_job.yml`
- Modify: `databricks.yml`
- Test: `tests/test_notebooks.py` and `tests/test_dab_bundle.py` (add cases)

**Interfaces:**
- Consumes: staging tables written by Task 8.
- Produces: job tasks `generate_ai` (notebook), `generate_ai_text` (sql_task); vars `warehouse_id`, `llm_endpoint`.

- [ ] **Step 1: Write the failing tests**

```python
# add to tests/test_dab_bundle.py
def test_ai_bundle_variables_present():
    import yaml
    bundle = yaml.safe_load((_ROOT / "databricks.yml").read_text())
    assert {"warehouse_id", "llm_endpoint"} <= set(bundle["variables"])
    # warehouse_id must have NO committed default (supplied per-deploy, like host)
    wid = bundle["variables"]["warehouse_id"]
    assert "default" not in wid or wid.get("default") in (None, "")


def test_ai_tasks_wired_with_fanout():
    import yaml
    job = yaml.safe_load((_ROOT / "resources" / "generate_facts_job.yml").read_text())["resources"]["jobs"]["generate_facts"]
    by_key = {t["task_key"]: t for t in job["tasks"]}
    # generate_ai is a notebook task depending only on generate_facts (parallel to finance)
    assert "generate_ai" in by_key
    ai_deps = {d["task_key"] for d in by_key["generate_ai"].get("depends_on", [])}
    assert ai_deps == {"generate_facts"}
    assert "notebook_task" in by_key["generate_ai"]
    # finance still depends only on generate_facts (not chained behind AI)
    fin_deps = {d["task_key"] for d in by_key["generate_finance"].get("depends_on", [])}
    assert fin_deps == {"generate_facts"}
    # generate_ai_text is a SQL task on the warehouse, depending on generate_ai
    assert "generate_ai_text" in by_key
    txt = by_key["generate_ai_text"]
    assert "sql_task" in txt
    assert "${var.warehouse_id}" in str(txt["sql_task"].get("warehouse_id", ""))
    assert {d["task_key"] for d in txt.get("depends_on", [])} == {"generate_ai"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dab_bundle.py -v`
Expected: FAIL — `warehouse_id` var missing / `generate_ai` task missing.

- [ ] **Step 3: Create the SQL file and wire the job + variables**

Create `resources/generate_ai_text.sql` (named parameters `:catalog`, `:schema_prefix`, `:llm_endpoint`; `IDENTIFIER()` builds the dynamic table names). Verify exact `ai_query` signature at deploy — this file is validated on the workspace, not locally:

```sql
-- Fill review text from the staging table.
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') AS
SELECT
  review_id, product_sk, customer_sk, date_sk, rating,
  ai_query(:llm_endpoint, title_prompt) AS review_title,
  ai_query(:llm_endpoint, prompt)       AS review_text,
  verified_purchase, helpful_votes
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._product_review_staging');

-- Fill service-case text from the staging table (resolution only when resolved/closed).
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') AS
SELECT
  case_id, customer_sk, product_sk, store_sk, date_sk, case_type, channel, status,
  ai_query(:llm_endpoint, notes_prompt) AS case_notes,
  CASE WHEN resolution_prompt IS NULL THEN NULL
       ELSE ai_query(:llm_endpoint, resolution_prompt) END AS resolution_notes,
  csat_score
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._service_case_staging');
```

In `resources/generate_facts_job.yml`, append two tasks after `generate_finance` (leave `generate_finance` unchanged — it stays `depends_on: generate_facts`, giving the finance ∥ AI fan-out):

```yaml
        - task_key: generate_ai
          depends_on:
            - task_key: generate_facts
          notebook_task:
            notebook_path: ../notebooks/generate_ai.py
            base_parameters:
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              scale_profile: ${var.scale_profile}
        - task_key: generate_ai_text
          depends_on:
            - task_key: generate_ai
          sql_task:
            warehouse_id: ${var.warehouse_id}
            file:
              path: ../resources/generate_ai_text.sql
            parameters:
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              llm_endpoint: ${var.llm_endpoint}
```

In `databricks.yml`, add to `variables:` (no committed default for `warehouse_id`):

```yaml
  warehouse_id:
    description: SQL warehouse id for the ai_query text-fill task (supplied per-deploy).
  llm_endpoint:
    description: Foundation Model endpoint for ai_query text generation.
    default: databricks-meta-llama-3-1-8b-instruct
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_dab_bundle.py tests/test_notebooks.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add resources/generate_ai_text.sql resources/generate_facts_job.yml databricks.yml tests/test_dab_bundle.py
git commit -m "feat(ai): fan out job (finance || ai) + ai_query SQL text-fill task"
```

---

### Task 10: Full suite + package sanity

**Files:**
- Test: whole suite.

- [ ] **Step 1: Run the full test suite**

Run: `python -m pytest -q`
Expected: PASS (all prior + new tests; ~130+ tests).

- [ ] **Step 2: Verify the AI package imports cleanly**

Run: `python -c "import techmart.ai.registry as r; print(len(r.AI_SPECS), 'ai specs')"`
Expected: prints `6 ai specs`.

- [ ] **Step 3: Commit any lint/import cleanups (if needed)**

```bash
git add -A && git commit -m "chore(ai): suite green; package sanity" || echo "nothing to commit"
```

---

## Workspace validation (post-merge-candidate, before PR sign-off)

Not a pytest task — the on-workspace proven-green gate (mirrors finance). Run against `field-eng-east`:

1. `/opt/homebrew/bin/databricks bundle deploy -t dev -p field-eng-east` (smoke; supply `--var=warehouse_id=<serverless SQL warehouse id>`).
2. `/opt/homebrew/bin/databricks bundle run generate_facts -t dev -p field-eng-east --var=warehouse_id=<id>`.
3. Confirm: `generate_finance` and `generate_ai` ran concurrently (job graph); `techmart_ai` has `fact_sales_forecast`, `ai_anomaly_catalog`, `product_review`, `service_case`; text columns non-null with sane length; 0 orphan FKs; per-store sales date spread now healthy (Task 1 fix visible in `fact_sales_line`).

## Self-review (completed)

- **Spec coverage:** §0 fix → Task 1; scale levers → Task 2; anomaly catalog + windows → Task 3; `fact_sales_forecast` (versions, anomaly divergence, intervals) → Task 4; `product_review` → Task 5; `service_case` → Task 6; registry → Task 7; notebook → Task 8; `ai_query` SQL split + job fan-out + `warehouse_id`/`llm_endpoint` → Task 9; determinism/RI enforced in each builder task; workspace `ai_query` validation → Workspace section. Follow-up (other builders' `fixed`-seed sweep) intentionally out of scope per spec.
- **Placeholder scan:** no TODO/TBD; every code step is complete. `review_id`/`case_id` use deterministic `xxhash64` of stable keys (never an RNG id).
- **Type consistency:** builder signatures, `*_SPEC` names, `AI_SPECS`, `FORECAST_VERSIONS`, and staging/final column sets are consistent across tasks and match `write_table_uc`/`SparkTableSpec` from the existing codebase.

## Next plan

Phase 7 — `techmart_ops` (Lakebase/Postgres operational write-back) or `techmart_semantic` (UC metric views over core+finance+ai). Also queued: the documented follow-up to sweep the other fact/dim builders for the `randomSeedMethod="fixed"` column-correlation found in Task 1.
