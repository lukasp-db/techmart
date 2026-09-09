# Periodic, Sales-Calibrated Inventory Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the daily 7-day full-grid `fact_inventory_snapshot` with a fiscal-period-end periodic snapshot spanning the full history, with a realistic per-store SKU assortment and on-hand calibrated to sales velocity.

**Architecture:** The snapshot becomes a sales-linked fact. It takes fiscal period-end dates from `period_end_lookup(dim_date)` (~`history_years × 12` dates), restricts each store to a deterministic hash-based SKU assortment (weighted by product popularity and store size), and derives on-hand from per-`(store, product)` sales velocity × a per-period seasonal index × deterministic noise, with an explicit small out-of-stock draw. Grain `(date_sk, store_sk, product_sk)` and the table schema are unchanged, so `fact_inventory_valuation`, the metric views, and the dashboard datasets need no schema changes — the valuation now simply spans all periods.

**Tech Stack:** Python, PySpark, dbldatagen (unused in this fact), pytest with a shared local `spark` fixture (`tests/conftest.py`), Databricks Asset Bundle for deploy.

**Spec:** `docs/superpowers/specs/2026-09-08-inventory-snapshot-periodic-calibrated-design.md`

## Global Constraints

- Determinism: derive all per-row randomness from `uniform_hash(...)` in `src/techmart/facts/gen.py` (pure hash of stable keys). Never use `rand()`.
- Grain and schema of `fact_inventory_snapshot` are fixed by `FACT_INVENTORY_SNAPSHOT_SPEC` — do not add/remove/rename columns.
- `select_ordered(df)` projects to exactly the spec columns in order; extra helper columns on `df` are dropped automatically — no manual `.drop()` needed for helpers.
- Snapshot must remain referentially valid: every `date_sk` must be a real `dim_date` key (period-ends are, by construction), `store_sk ∈ [1, num_stores]`, `product_sk ∈ [1, num_skus]`.
- Preserve existing measure invariants: `on_hand_qty ≥ 0`, `available_qty ≥ 0`, `available_qty ≤ on_hand_qty`, `is_out_of_stock ⇔ on_hand_qty = 0`, `on_hand_cost_value = round(on_hand_qty × unit_cost, 2)`.
- Run tests from the repo root with the project's venv: `python -m pytest <path> -v`. The `spark` fixture is session-scoped and local (`tests/conftest.py`).
- No dashboard/metric-view code changes in this plan (verify-only downstream).

---

### Task 1: Config — add assortment/velocity levers, retire `inventory_snapshot_days`

**Files:**
- Modify: `src/techmart/config.py:24` (ScaleProfile fields)
- Modify: `config/scale_profiles.yaml` (all four profiles)
- Test: `tests/test_config.py:50-68`

**Interfaces:**
- Produces: `ScaleProfile.assortment_rate: float` (default `0.30`), `ScaleProfile.target_wos_weeks: int` (default `8`). `ScaleProfile.inventory_snapshot_days` is REMOVED.

- [ ] **Step 1: Update the config tests to the new levers**

In `tests/test_config.py`, replace `test_scale_profiles_have_phase4_knobs` (lines 50-61) and `test_scale_profile_defaults_keep_positional_construction` (lines 64-68) with:

```python
def test_scale_profiles_have_phase4_knobs():
    profiles = load_profiles(PROFILES)
    for name in ("smoke", "demo_lean", "showcase", "stress"):
        p = profiles[name]
        assert 0.0 < p.assortment_rate <= 1.0
        assert p.target_wos_weeks >= 1
        assert p.inventory_movements_target >= 1
        assert p.web_events_target >= 1
    # smoke is intentionally tiny so the deploy proof is fast
    smoke = profiles["smoke"]
    assert smoke.inventory_movements_target == 20000
    assert smoke.web_events_target == 100000


def test_scale_profile_defaults_keep_positional_construction():
    p = ScaleProfile("t", 5, 500, 1, 50000, 1000, 20)
    assert p.assortment_rate == 0.30
    assert p.target_wos_weeks == 8
    assert p.inventory_movements_target == 1000
    assert p.web_events_target == 1000
```

- [ ] **Step 2: Run the config tests to verify they fail**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL — `AttributeError: 'ScaleProfile' object has no attribute 'assortment_rate'` (and the YAML still carries `inventory_snapshot_days`, which will fail to construct once the field is removed in Step 3).

- [ ] **Step 3: Update `ScaleProfile` in `src/techmart/config.py`**

Remove the `inventory_snapshot_days` field (line 24) and add the two new levers. The block starting at line 24 becomes:

```python
    inventory_movements_target: int = 1000
    web_events_target: int = 1000
    # Inventory snapshot levers (periodic, sales-calibrated).
    assortment_rate: float = 0.30      # avg fraction of catalog a store carries
    target_wos_weeks: int = 8          # target weeks-of-supply the on-hand calibration aims for
    # Finance reconciliation levers (behavioral; shared across profiles via defaults).
    allowance_rate: float = 0.010
```

(Delete the line `inventory_snapshot_days: int = 7`.)

- [ ] **Step 4: Update `config/scale_profiles.yaml`**

In each of the four profiles, delete the `inventory_snapshot_days:` line and add `assortment_rate` + `target_wos_weeks`:

- `demo_lean`: `assortment_rate: 0.25`, `target_wos_weeks: 8`
- `showcase`: `assortment_rate: 0.20`, `target_wos_weeks: 8`
- `smoke`: `assortment_rate: 0.50`, `target_wos_weeks: 6`
- `stress`: `assortment_rate: 0.15`, `target_wos_weeks: 8`

- [ ] **Step 5: Run the config tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS (all tests in the file).

- [ ] **Step 6: Commit**

```bash
git add src/techmart/config.py config/scale_profiles.yaml tests/test_config.py
git commit -m "Add assortment/WOS levers; retire inventory_snapshot_days

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Rewrite the `fact_inventory_snapshot` builder + its unit tests

**Files:**
- Modify: `src/techmart/facts/fact_inventory_snapshot.py` (rewrite `build_fact_inventory_snapshot`; SPEC unchanged)
- Test: `tests/test_fact_inventory_snapshot.py` (rewrite)

**Interfaces:**
- Consumes: `ScaleProfile.assortment_rate`, `ScaleProfile.target_wos_weeks` (Task 1); `period_end_lookup`, `date_periods` from `src/techmart/finance/periods.py`; `product_economics` from `src/techmart/facts/lookups.py`; `uniform_hash` from `src/techmart/facts/gen.py`.
- Produces: new signature
  `build_fact_inventory_snapshot(spark, config, *, dim_store, dim_product, dim_date, fact_sales_line) -> DataFrame`
  (adds required keyword `fact_sales_line`; keeps `dim_store`, `dim_product`, `dim_date`).

- [ ] **Step 1: Rewrite the unit tests**

Replace the entire body of `tests/test_fact_inventory_snapshot.py` with:

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.spark.dimensions.dim_store import build_dim_store
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_inventory_snapshot import (
    FACT_INVENTORY_SNAPSHOT_SPEC,
    build_fact_inventory_snapshot,
)
from techmart.finance.periods import period_end_lookup

_P = ScaleProfile("t", 4, 30, 1, 6000, 300, 20)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 4, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 30}


def _dims(spark):
    return (
        build_dim_store(spark, _CFG),
        build_dim_product(spark, _CFG),
        build_dim_date(spark, _CFG),
    )


def _build(spark):
    ds, dp, dd = _dims(spark)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd,
                                  dim_counts=_COUNTS, rows=6000)
    return build_fact_inventory_snapshot(
        spark, _CFG, dim_store=ds, dim_product=dp, dim_date=dd, fact_sales_line=sales,
    ), dd


def test_schema_and_grain(spark):
    df, _ = _build(spark)
    assert df.columns == FACT_INVENTORY_SNAPSHOT_SPEC.column_names
    # one row per (store, sku, period-end)
    assert df.groupBy("store_sk", "product_sk", "date_sk").count().filter("count > 1").count() == 0


def test_dates_are_period_ends_and_span_history(spark):
    df, dd = _build(spark)
    pe = period_end_lookup(dd).select(F.col("period_end_date_sk").alias("date_sk"))
    # every snapshot date is a fiscal period-end
    assert df.select("date_sk").distinct().join(pe, "date_sk", "left_anti").count() == 0
    # spans (approximately) one row per fiscal period across the year
    n_periods = pe.count()
    assert df.select("date_sk").distinct().count() == n_periods
    assert n_periods >= 10  # ~12 fiscal periods in a 1-year window


def test_assortment_is_sparse_and_popularity_ordered(spark):
    df, _ = _build(spark)
    pairs = df.select("store_sk", "product_sk").distinct()
    # each store carries a strict subset of the 30-SKU catalog
    assert pairs.count() < 4 * 30
    # hot (low product_sk) SKUs are carried in >= as many stores as the cold tail
    half = 15
    hot = pairs.filter(F.col("product_sk") <= half).count()
    cold = pairs.filter(F.col("product_sk") > half).count()
    assert hot >= cold


def test_measure_invariants(spark):
    df, _ = _build(spark)
    bad = df.filter(
        (F.col("on_hand_qty") < 0)
        | (F.col("available_qty") < 0)
        | (F.col("available_qty") > F.col("on_hand_qty"))
        | (F.col("is_out_of_stock") != (F.col("on_hand_qty") == 0))
        | (F.abs(F.col("on_hand_cost_value") - F.round(F.col("on_hand_qty") * F.col("unit_cost"), 2)) > 0.01)
    ).count()
    assert bad == 0


def test_days_of_supply_in_sane_band(spark):
    df, _ = _build(spark)
    # median-ish days-of-supply should sit near target_wos_weeks * 7 (56d), not the
    # old pathological range; check the in-stock population stays bounded.
    r = df.filter(~F.col("is_out_of_stock")).agg(
        F.expr("percentile_approx(days_of_supply, 0.5)").alias("p50")
    ).first()
    assert 7.0 <= r["p50"] <= 200.0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.sum("on_hand_qty").alias("q"),
                             F.round(F.sum("on_hand_cost_value"), 2).alias("v")).first()
    b = _build(spark)[0].agg(F.sum("on_hand_qty").alias("q"),
                             F.round(F.sum("on_hand_cost_value"), 2).alias("v")).first()
    assert a == b
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_fact_inventory_snapshot.py -v`
Expected: FAIL — `TypeError: build_fact_inventory_snapshot() got an unexpected keyword argument 'fact_sales_line'` (old signature has no `fact_sales_line`).

- [ ] **Step 3: Rewrite the builder**

Replace `build_fact_inventory_snapshot` in `src/techmart/facts/fact_inventory_snapshot.py`. Keep `FACT_INVENTORY_SNAPSHOT_SPEC` exactly as-is. Update imports at the top of the file to:

```python
"""fact_inventory_snapshot: store x SKU x fiscal-period-end stock position.

Periodic snapshot (one row per store, carried SKU, and fiscal period-end),
calibrated to sales velocity. Replaces the former daily full-grid snapshot.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..finance.periods import date_periods, period_end_lookup
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import uniform_hash
from .lookups import product_economics
```

(The `from datetime import timedelta` import is no longer needed — remove it. `SparkColumn`/`SparkTableSpec` stay for the SPEC.)

Add module constants below the SPEC:

```python
_MIN_DAILY = 0.05          # slow-mover demand floor (units/day)
_OOS_RATE = 0.04           # fraction of snapshot cells forced out of stock
_SQFT_LO, _SQFT_HI = 15000.0, 45000.0   # dim_store.square_footage range
```

Replace the function body with:

```python
def build_fact_inventory_snapshot(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    dim_store: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
    fact_sales_line: DataFrame,
) -> DataFrame:
    """Periodic (fiscal-period-end) snapshot over the full history.

    Each store carries a deterministic SKU assortment (weighted by product
    popularity and store size); on-hand is calibrated to per-(store, product)
    sales velocity times a per-period seasonal index, with an explicit small
    out-of-stock draw. Grain: (date_sk, store_sk, product_sk).
    """
    sp = config.scale_profile
    assortment_rate = float(sp.assortment_rate)
    target_wos_weeks = float(sp.target_wos_weeks)
    n_days = float(dim_date.count())  # contiguous daily history
    num_products = float(dim_product.count())

    # --- period-end snapshot dates (~history_years * 12) with pidx ---
    pe = period_end_lookup(dim_date).select(
        F.col("period_end_date_sk").alias("date_sk"), "pidx",
    )

    # --- per-period seasonal demand index (normalized to mean 1.0) ---
    periods = date_periods(dim_date).select("date_sk", "pidx")
    period_units = (
        fact_sales_line.select("date_sk", "quantity")
        .join(periods, "date_sk")
        .groupBy("pidx").agg(F.sum("quantity").alias("period_units"))
    )
    avg_period_units = period_units.agg(F.avg("period_units")).first()[0] or 1.0
    seasonal = period_units.withColumn(
        "seasonal_factor",
        F.greatest(F.col("period_units") / F.lit(float(avg_period_units)), F.lit(0.1)),
    ).select("pidx", "seasonal_factor")

    # --- per-(store, product) sales velocity ---
    velocity = (
        fact_sales_line.groupBy("store_sk", "product_sk")
        .agg(F.sum("quantity").alias("_total_units"))
    )

    # --- stores (breadth from square footage) & product economics + popularity ---
    stores = dim_store.select(
        "store_sk",
        (F.lit(0.5) + (F.col("square_footage").cast("double") - F.lit(_SQFT_LO))
         / F.lit(_SQFT_HI - _SQFT_LO)).alias("_store_factor"),
    )
    prods = product_economics(dim_product).select(
        "product_sk",
        F.round(F.col("list_price"), 2).alias("list_price"),
        F.round(F.col("standard_cost"), 2).alias("unit_cost"),
        # popularity: high for low product_sk (mirrors the sales pow(u,3) tail)
        (F.lit(1.0) - (F.col("product_sk").cast("double") - F.lit(1.0)) / F.lit(num_products)).alias("_pop"),
    )

    # --- assortment: deterministic carried (store, product) pairs ---
    assort = (
        stores.crossJoin(prods)
        .withColumn(
            "_carry_prob",
            F.least(
                F.lit(1.0),
                F.lit(2.0 * assortment_rate) * F.col("_pop") * F.col("_store_factor"),
            ),
        )
        .filter(uniform_hash(F.col("store_sk"), F.col("product_sk"), salt="assort") < F.col("_carry_prob"))
    )

    # --- cross with period-ends; attach velocity + seasonal factor ---
    grid = (
        assort.crossJoin(pe)
        .join(velocity, ["store_sk", "product_sk"], "left")
        .join(seasonal, "pidx", "left")
        .withColumn("_total_units", F.coalesce(F.col("_total_units"), F.lit(0)))
        .withColumn("seasonal_factor", F.coalesce(F.col("seasonal_factor"), F.lit(1.0)))
        .withColumn("_avg_daily", F.greatest(F.col("_total_units") / F.lit(n_days), F.lit(_MIN_DAILY)))
    )

    def cell(salt: str):
        return uniform_hash(F.col("store_sk"), F.col("product_sk"), F.col("date_sk"), salt=salt)

    df = (
        grid
        .withColumn("_noise", F.lit(0.85) + cell("oh") * F.lit(0.30))
        .withColumn("_oos", cell("oos") < F.lit(_OOS_RATE))
        .withColumn(
            "on_hand_qty",
            F.when(F.col("_oos"), F.lit(0)).otherwise(
                F.greatest(
                    F.round(
                        F.lit(target_wos_weeks) * F.lit(7.0) * F.col("_avg_daily")
                        * F.col("seasonal_factor") * F.col("_noise")
                    ).cast("int"),
                    F.lit(0),
                )
            ),
        )
        .withColumn("reserved_qty", F.floor(cell("rs") * F.lit(5)).cast("int"))
        .withColumn("available_qty", F.greatest(F.col("on_hand_qty") - F.col("reserved_qty"), F.lit(0)))
        .withColumn("safety_stock_qty", F.ceil(F.col("_avg_daily") * F.lit(3.0)).cast("int"))
        .withColumn("reorder_point", F.ceil(F.col("_avg_daily") * F.lit(7.0)).cast("int"))
        .withColumn(
            "on_order_qty",
            F.when(F.col("on_hand_qty") < F.col("reorder_point"),
                   F.ceil(F.col("_avg_daily") * F.lit(14.0)).cast("int")).otherwise(F.lit(0)),
        )
        .withColumn(
            "in_transit_qty",
            F.when(F.col("on_order_qty") > F.lit(0),
                   F.floor(cell("it") * F.col("on_order_qty")).cast("int")).otherwise(F.lit(0)),
        )
        .withColumn("on_hand_retail_value", F.round(F.col("on_hand_qty") * F.col("list_price"), 2))
        .withColumn("on_hand_cost_value", F.round(F.col("on_hand_qty") * F.col("unit_cost"), 2))
        .withColumn("days_of_supply", F.round(F.col("on_hand_qty") / F.col("_avg_daily"), 1))
        .withColumn("is_out_of_stock", F.col("on_hand_qty") == F.lit(0))
    )
    return FACT_INVENTORY_SNAPSHOT_SPEC.select_ordered(df)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_fact_inventory_snapshot.py -v`
Expected: PASS (all six tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/facts/fact_inventory_snapshot.py tests/test_fact_inventory_snapshot.py
git commit -m "Rewrite fact_inventory_snapshot as periodic sales-calibrated snapshot

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: Wire the snapshot as a sales-linked fact (job + notebook)

**Files:**
- Modify: `src/techmart/jobs/generate_facts.py:101-106`
- Modify: `notebooks/generate_facts.py:50`

**Interfaces:**
- Consumes: `build_fact_inventory_snapshot(..., fact_sales_line=...)` (Task 2). In both entrypoints, the persisted `sales` DataFrame already exists before the snapshot is built (`generate_facts.py` reads it back at line 98; the notebook builds `sales` at line 45).

- [ ] **Step 1: Update the serverless job entrypoint**

In `src/techmart/jobs/generate_facts.py`, the snapshot is currently in the "standalone facts" block (lines 101-106), which runs after `sales` is read back (line 98). Add the `fact_sales_line=sales` argument:

```python
    # --- inventory snapshot (sales-linked: calibrated to velocity) ---
    target = write_table_uc(
        spark,
        build_fact_inventory_snapshot(
            spark, config,
            dim_store=dim_store, dim_product=dim_product, dim_date=dim_date,
            fact_sales_line=sales,
        ),
        FACT_INVENTORY_SNAPSHOT_SPEC, config.catalog, config.schema_prefix,
    )
    print(f"wrote {target}")
```

(No reordering needed — `sales` is already materialized above this block.)

- [ ] **Step 2: Update the notebook path**

In `notebooks/generate_facts.py`, line 45 builds `sales`; line 50 builds the snapshot. Change the snapshot call to pass `fact_sales_line=sales`:

```python
print("wrote", write_table_uc(spark, build_fact_inventory_snapshot(spark, config, dim_store=dim_store, dim_product=dim_product, dim_date=dim_date, fact_sales_line=sales), FACT_INVENTORY_SNAPSHOT_SPEC, catalog, schema_prefix))
```

Note: the notebook builds `sales` in memory (line 45) rather than reading it back. That is acceptable here because `build_fact_sales_line` is deterministic; if the notebook writes sales before this line and a read-back variable is preferred for exact parity with the persisted table, reuse whatever `sales`/read-back variable already exists at line 50. Do not introduce a second sales build.

- [ ] **Step 3: Verify nothing else calls the old signature**

Run: `grep -rn "build_fact_inventory_snapshot(" src notebooks tests | grep -v fact_sales_line | grep -v "def build_fact_inventory_snapshot"`
Expected: no output (every call site now passes `fact_sales_line=`).

- [ ] **Step 4: Run the notebook import/smoke test and the facts generation test**

Run: `python -m pytest tests/test_notebooks.py tests/test_generate_facts.py -v`
Expected: PASS (these exercise notebook parsing / local sales generation; they must remain green after the wiring change).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/jobs/generate_facts.py notebooks/generate_facts.py
git commit -m "Wire fact_inventory_snapshot as a sales-linked fact

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: Strengthen the valuation test to assert full-period coverage

**Files:**
- Test: `tests/test_fact_inventory_valuation.py:23-30` (update `_build` to pass `fact_sales_line` into the snapshot) and add one test.

**Interfaces:**
- Consumes: the new snapshot signature (Task 2). The valuation builder (`build_fact_inventory_valuation`) is unchanged.

- [ ] **Step 1: Update `_build` and add a coverage test**

In `tests/test_fact_inventory_valuation.py`, the current `_build` (lines 23-30) builds the snapshot without `fact_sales_line`. Update it so the snapshot receives the sales fact, and add a test asserting the valuation now spans all fiscal periods. Replace `_build` with:

```python
def _build(spark):
    dd = build_dim_date(spark, _CFG); dp = build_dim_product(spark, _CFG)
    ds = build_dim_store(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=4000)
    snap = build_fact_inventory_snapshot(spark, _CFG, dim_store=ds, dim_product=dp,
                                         dim_date=dd, fact_sales_line=sales)
    val = build_fact_inventory_valuation(spark, _CFG, fact_inventory_snapshot=snap,
                                         fact_sales_line=sales, dim_product=dp, dim_date=dd)
    return val, snap, dp, dd
```

Then add this test at the end of the file:

```python
def test_valuation_spans_all_fiscal_periods(spark):
    val, snap, dp, dd = _build(spark)
    from techmart.finance.periods import period_end_lookup
    n_periods = period_end_lookup(dd).count()
    # valuation now carries every fiscal period-end, not just the one that used
    # to fall inside the old 7-day snapshot window.
    assert val.select("date_sk").distinct().count() == n_periods
    assert n_periods >= 10
```

- [ ] **Step 2: Run the valuation tests to verify they pass**

Run: `python -m pytest tests/test_fact_inventory_valuation.py -v`
Expected: PASS — including the existing `test_cost_value_ties_to_snapshot` (snapshot period-end cost sum still equals valuation cost sum) and the new coverage test.

- [ ] **Step 3: Verify downstream consumers need no change (read-only checks)**

These are verification steps, not edits — confirm and record findings:

Run: `grep -n "MAX(date)" src/techmart/dashboards/datasets.py`
Expected: the inventory, bridge, and lost-sales datasets filter `WHERE date = (SELECT MAX(date) FROM mv_inventory)` — cadence-agnostic, so they correctly read the latest period-end. No change required.

Run: `python -m pytest tests/test_dashboard_datasets.py tests/test_metric_views_registry.py tests/test_metric_view.py -v`
Expected: PASS (no dashboard/metric-view code changed).

- [ ] **Step 4: Run the full suite**

Run: `python -m pytest -q`
Expected: PASS (no regressions across the suite).

- [ ] **Step 5: Commit**

```bash
git add tests/test_fact_inventory_valuation.py
git commit -m "Assert inventory valuation spans all fiscal periods

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Post-implementation: regeneration (executed by Lukas, not part of this plan)

After merge, against the Azure workspace:
1. `databricks bundle deploy` with the showcase profile.
2. Run the core facts job (regenerates `fact_inventory_snapshot`), then the finance job (regenerates `fact_inventory_valuation`).
3. Validate:
   - `SELECT COUNT(DISTINCT date_sk) FROM core.fact_inventory_snapshot` ≈ `history_years × 12` (≈36 for showcase).
   - `SELECT MIN(date_sk), MAX(date_sk) FROM core.fact_inventory_snapshot` — min near `start_date`, max near `end_date`.
   - `days_of_supply` distribution centered near `target_wos_weeks × 7`.
   - `SELECT COUNT(DISTINCT date_sk) FROM finance.fact_inventory_valuation` spans all periods.

## Follow-ups (out of scope here)

- Inventory time-series widgets on the merch exec dashboard now that ~36 period-ends of history exist.
- Revisit the bridge dataset's index framing (`datasets.py:60-63`) — absolute WOS/GMROI are now calibrated to sales, so the "indices only" caveat can be relaxed.

## Self-Review

- **Spec coverage:** cadence → Task 2 (period-end dates); assortment → Task 2 (hash membership); velocity calibration + seasonal trend + OOS → Task 2; config levers + retire `inventory_snapshot_days` → Task 1; wiring as sales-linked → Task 3; valuation full-period coverage + downstream verify → Task 4. Volume/testing/rollout captured in spec + Task 4 Step 3-4 + post-implementation section. No gaps.
- **Placeholder scan:** none — every code step has concrete code and exact run/expected lines.
- **Type consistency:** `build_fact_inventory_snapshot(..., fact_sales_line=...)` signature is defined in Task 2 and consumed identically in Tasks 3 and 4; `assortment_rate`/`target_wos_weeks` defined in Task 1 and read in Task 2; `period_end_lookup`/`date_periods` used consistently.
