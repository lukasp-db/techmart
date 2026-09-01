# Techmart Seed Decorrelation Sweep — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the shared-seed column-correlation bug from every remaining dbldatagen builder by standardizing on `randomSeedMethod="hash_fieldname"`, guarded by per-builder independence tests and a workspace smoke re-validation.

**Architecture:** Four one-line changes (`"fixed"` → `"hash_fieldname"`) — one in the shared `build_scd2_dim` (covers 5 dims), one in `dim_product`, one each in `fact_inventory_movement` and `fact_web_events`. Each change is guarded by a new spread/independence test modeled on `tests/test_fact_sales_line_date_spread.py`. `fact_sales_line` already uses `hash_fieldname`; `dim_date`/`dim_channel` are `createDataFrame` and exempt.

**Tech Stack:** Python, PySpark, dbldatagen, pytest (session-scoped `spark` fixture in `tests/conftest.py`).

**Spec:** `docs/superpowers/specs/2026-09-01-techmart-seed-decorrelation-design.md`

## Global Constraints

- **Only change** `randomSeedMethod="fixed"` → `randomSeedMethod="hash_fieldname"`. No other production-code edits (no new columns, no signature changes, no reordering).
- **No new config levers** (`ScaleProfile`) and **no new `databricks.yml` vars**. Output schemas unchanged.
- **Determinism preserved:** `hash_fieldname` is deterministic given the run seed; existing `test_deterministic`-style tests must still pass.
- **RI preserved:** FK ranges come from `dim_counts`/hash expressions, never the seed method — existing referential-integrity tests must still pass.
- **New tests must be genuine guards:** each MUST be confirmed to FAIL against the current `"fixed"` code before the flip, and PASS after (fail-first is mandatory — see each task).
- **Git:** push via SSH alias `github.com-lukasp`. Commit messages end with `Co-authored-by: Isaac <no-reply@databricks.com>`. Databricks pre-commit hooks are active; never use `--no-verify`.

---

## Task 1: Decorrelate `fact_inventory_movement`

**Files:**
- Create: `tests/test_fact_inventory_movement_spread.py`
- Modify: `src/techmart/facts/fact_inventory_movement.py:52` (the `randomSeedMethod="fixed"` argument)

**Interfaces:**
- Consumes: `build_fact_inventory_movement(spark, config, *, dim_date, dim_product, dim_counts, rows=None, seed=None)`, `build_dim_date`, `build_dim_product`, `ScaleProfile`, `TechmartConfig`.
- Produces: nothing downstream depends on this task's test.

Under `"fixed"`, the seeded streams `date_sk`, `product_sk`, `store_sk`, `movement_type`, `abs_qty` share one seed and come out rank-correlated (each store's movements concentrate onto ~2 dates — the fact_sales_line symptom). Columns built via `expr`/`F.hash` (`vendor_sk`, `reference_doc_id`, `reason_code`, `unit_cost`) are unaffected.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_fact_inventory_movement_spread.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_inventory_movement import build_fact_inventory_movement

_P = ScaleProfile("t", num_stores=5, num_skus=40, history_years=2,
                  sales_lines_target=3000, num_customers=200, num_vendors=20,
                  inventory_movements_target=10000)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 100, "vendor": 20, "product": 40}


def test_dates_are_decorrelated_from_store(spark):
    df = build_fact_inventory_movement(
        spark, _CFG, dim_date=build_dim_date(spark, _CFG),
        dim_product=build_dim_product(spark, _CFG), dim_counts=_COUNTS, rows=10000,
    )
    avg_d = (df.groupBy("store_sk").agg(F.countDistinct("date_sk").alias("d"))
               .agg(F.avg("d").alias("a")).first()["a"])
    # ~100 movements/store over ~730 dates: the correlated ("fixed") build collapses
    # each store to a couple of distinct dates; decorrelated gives dozens.
    assert avg_d > 10, f"per-store distinct dates collapsed to {avg_d:.1f}"
```

- [ ] **Step 2: Run the test against the CURRENT (`"fixed"`) code and confirm it FAILS**

Run: `pytest tests/test_fact_inventory_movement_spread.py -v`
Expected: FAIL (`avg_d` is small, ~1–4). **This is mandatory.** If it PASSES, the chosen metric does not expose the correlation — adjust the grouping column or threshold (e.g. group by `product_sk` and count distinct `date_sk`, or raise `dim_counts["store"]`) until it genuinely fails on `"fixed"`. If no seeded pair shows correlation on this builder, stop and report the finding rather than force a test.

- [ ] **Step 3: Flip the seed method**

In `src/techmart/facts/fact_inventory_movement.py` line 52, change `randomSeedMethod="fixed"` to `randomSeedMethod="hash_fieldname"`. Change nothing else.

- [ ] **Step 4: Run the new test and confirm it PASSES**

Run: `pytest tests/test_fact_inventory_movement_spread.py -v`
Expected: PASS (`avg_d` now dozens).

- [ ] **Step 5: Run the existing builder tests and fix any that broke**

Run: `pytest tests/test_fact_inventory_movement.py -v`
Expected: PASS. These tests are structural (schema, counts, RI, sign logic) and should be unaffected. If any asserts a distribution that shifted, update the assertion to the new deterministic value (do NOT weaken an RI/schema check).

- [ ] **Step 6: Commit**

```bash
git add tests/test_fact_inventory_movement_spread.py src/techmart/facts/fact_inventory_movement.py
git commit -m "Decorrelate fact_inventory_movement seeded columns (hash_fieldname)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 2: Decorrelate `fact_web_events`

**Files:**
- Create: `tests/test_fact_web_events_spread.py`
- Modify: `src/techmart/facts/fact_web_events.py:59` (the `randomSeedMethod="fixed"` argument)

**Interfaces:**
- Consumes: `build_fact_web_events(spark, config, *, dim_date, dim_counts, rows=None, seed=None)`, `build_dim_date`, `ScaleProfile`, `TechmartConfig`.
- Produces: nothing downstream depends on this task's test.

Under `"fixed"`, the seeded header streams `date_sk`, `channel_sk`, `device_num`, `referrer_num`, `num_events` share one seed → the device/channel mix becomes degenerate per day. Post-build columns (`customer_sk`, `event_type`, `product_sk`, `search_term`, `cart_value`, `event_ts`) use `uniform_hash`/`F.hash` and are unaffected.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_fact_web_events_spread.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.facts.fact_web_events import build_fact_web_events

_P = ScaleProfile("t", num_stores=5, num_skus=40, history_years=2,
                  sales_lines_target=3000, num_customers=200, num_vendors=20,
                  web_events_target=40000)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"customer": 200, "product": 40}


def test_device_is_decorrelated_from_date(spark):
    df = build_fact_web_events(spark, _CFG, dim_date=build_dim_date(spark, _CFG),
                               dim_counts=_COUNTS, rows=40000)
    avg_dev = (df.groupBy("date_sk").agg(F.countDistinct("device_type").alias("d"))
                 .agg(F.avg("d").alias("a")).first()["a"])
    # With ~28 sessions/day, the correlated ("fixed") build ties ~one device band to
    # each date (~1 distinct device/day); decorrelated shows all three (~3).
    assert avg_dev > 2.0, f"device/day distinct collapsed to {avg_dev:.2f}"
```

- [ ] **Step 2: Run the test against the CURRENT (`"fixed"`) code and confirm it FAILS**

Run: `pytest tests/test_fact_web_events_spread.py -v`
Expected: FAIL (`avg_dev` ~1.0–1.3). **Mandatory.** Note: `date_sk` uses weighted sampling, which may consume the RNG differently than the plain streams, so the correlation could be weaker than for the min/max columns. If this pair does not fail, try `referrer` instead of `device_type`, or measure `countDistinct(struct("date_sk","device_type")) / countDistinct("date_sk")` and require `> 1.8`. If, after trying the seeded pairs, no correlation appears under `"fixed"`, report that this builder is empirically unaffected instead of forcing a failing test.

- [ ] **Step 3: Flip the seed method**

In `src/techmart/facts/fact_web_events.py` line 59, change `randomSeedMethod="fixed"` to `randomSeedMethod="hash_fieldname"`. Change nothing else.

- [ ] **Step 4: Run the new test and confirm it PASSES**

Run: `pytest tests/test_fact_web_events_spread.py -v`
Expected: PASS.

- [ ] **Step 5: Run the existing builder tests and fix any that broke**

Run: `pytest tests/test_fact_web_events.py -v`
Expected: PASS (structural: schema, volume band, RI, event_ts-on-date, determinism). Update only a genuinely shifted distribution assertion; never weaken an RI/schema/nullability check.

- [ ] **Step 6: Commit**

```bash
git add tests/test_fact_web_events_spread.py src/techmart/facts/fact_web_events.py
git commit -m "Decorrelate fact_web_events seeded columns (hash_fieldname)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 3: Decorrelate `dim_product`

**Files:**
- Create: `tests/test_dim_product_spread.py`
- Modify: `src/techmart/spark/dimensions/dim_product.py:126` (the `randomSeedMethod="fixed"` argument)

**Interfaces:**
- Consumes: `build_dim_product(spark, config)`, `ScaleProfile`, `TechmartConfig`.
- Produces: nothing downstream depends on this task's test.

`dim_product` has ~15 seeded streams. `color` (`values=_COLORS`, 10) and `primary_vendor_sk` (`minValue=1..num_vendors`) are two independent seeded columns; under `"fixed"` they are functionally linked. Derived/hashed fields (hierarchy from the taxonomy join, `is_marketplace`, `private_label_flag`, ids) are unaffected.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dim_product_spread.py
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_product import build_dim_product

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 300, 1, 50000, 500, 20),  # num_skus=300, num_vendors=20
    seed=42, output_dir=Path("data"), catalog="c", schema_prefix="techmart_",
    end_date=date(2026, 1, 31),
)


def test_color_and_vendor_are_independent(spark):
    df = build_dim_product(spark, _CFG)
    combos = df.select("color", "primary_vendor_sk").distinct().count()
    # correlated ("fixed") build functionally ties color to vendor (~30 combos);
    # independent streams populate the 10x20 grid (>150 combos with 300 rows).
    assert combos > 60, f"color/vendor combos collapsed to {combos}"
```

- [ ] **Step 2: Run the test against the CURRENT (`"fixed"`) code and confirm it FAILS**

Run: `pytest tests/test_dim_product_spread.py -v`
Expected: FAIL (`combos` ~30 or fewer). **Mandatory.** If it passes, pick another seeded pair (e.g. `uom` × `lifecycle_status`, or a `struct` of three seeded cols) or raise the threshold until it genuinely fails on `"fixed"`.

- [ ] **Step 3: Flip the seed method**

In `src/techmart/spark/dimensions/dim_product.py` line 126, change `randomSeedMethod="fixed"` to `randomSeedMethod="hash_fieldname"`. Change nothing else.

- [ ] **Step 4: Run the new test and confirm it PASSES**

Run: `pytest tests/test_dim_product_spread.py -v`
Expected: PASS.

- [ ] **Step 5: Run the existing builder test and fix any that broke**

Run: `pytest tests/test_dim_product_spark.py -v`
Expected: PASS (schema, SK uniqueness, hierarchy populated, vendor range, JSON specs, lifecycle/discontinue logic — all structural). Update only a genuinely shifted distribution assertion.

- [ ] **Step 6: Commit**

```bash
git add tests/test_dim_product_spread.py src/techmart/spark/dimensions/dim_product.py
git commit -m "Decorrelate dim_product seeded columns (hash_fieldname)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 4: Decorrelate the shared `build_scd2_dim` (5 dims) + regression guard

**Files:**
- Create: `tests/test_dim_seed_independence.py`
- Create: `tests/test_no_fixed_seed_method.py`
- Modify: `src/techmart/spark/dim_builder.py:39` (the `randomSeedMethod="fixed"` argument)

**Interfaces:**
- Consumes: `build_dim_store`, `build_dim_vendor`, `build_dim_promotion`, `build_dim_employee`, `build_dim_customer` (each `(spark, config) -> DataFrame`), `ScaleProfile`, `TechmartConfig`. All five route through `build_scd2_dim`, so the single line-39 change fixes all five at once.
- Produces: nothing downstream depends on these tests.

One code change (`dim_builder.py:39`) decorrelates all five shared-builder dims. Each dim's chosen independent seeded pair, and its correlated-vs-decorrelated expectation:
- **dim_store** — `(city, state)` (both 15-value pools). Fixed: ~15 diagonal combos. Decorrelated: ~190 (of the 225 grid) with 300 rows.
- **dim_vendor** — `vendor_name` = `stem_idx`(14) × `tail_idx`(6). Fixed: ~14 distinct names. Decorrelated: ~80.
- **dim_promotion** — `(promo_type, discount_method, channel_scope, funding_source)` (5×3×3×2). Fixed: ~10 combos. Decorrelated: ~70.
- **dim_employee** — `full_name` = `fi`(20) × `li`(20). Fixed: ~20 distinct names. Decorrelated: ~350.
- **dim_customer** — `(first_name, last_name)` (20×20). Fixed: ~20 combos. Decorrelated: ~350.

`test_no_fixed_seed_method.py` is a permanent regression guard: after this task, no source file uses `randomSeedMethod="fixed"`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_dim_seed_independence.py
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_store import build_dim_store
from techmart.spark.dimensions.dim_vendor import build_dim_vendor
from techmart.spark.dimensions.dim_promotion import build_dim_promotion
from techmart.spark.dimensions.dim_employee import build_dim_employee
from techmart.spark.dimensions.dim_customer import build_dim_customer


def _cfg(**kw):
    base = dict(num_stores=300, num_skus=40, history_years=2, sales_lines_target=3000,
                num_customers=600, num_vendors=200)
    base.update(kw)
    return TechmartConfig(scale_profile=ScaleProfile("t", **base), seed=42,
                          output_dir=Path("data"), catalog="c", schema_prefix="techmart_",
                          end_date=date(2026, 1, 31))


def test_store_city_and_state_independent(spark):
    df = build_dim_store(spark, _cfg(num_stores=300))
    combos = df.select("city", "state").distinct().count()
    # fixed: ~15 diagonal combos; decorrelated: ~190 of the 15x15 grid.
    assert combos > 60, f"store city/state combos collapsed to {combos}"


def test_vendor_name_not_collapsed(spark):
    df = build_dim_vendor(spark, _cfg(num_vendors=200))
    n = df.select("vendor_name").distinct().count()
    # stem(14) x tail(6): fixed ~14 names; decorrelated ~80.
    assert n > 30, f"vendor_name distinct collapsed to {n}"


def test_promotion_attrs_independent(spark):
    df = build_dim_promotion(spark, _cfg(history_years=2))  # 120 promotions
    combos = df.select("promo_type", "discount_method", "channel_scope",
                       "funding_source").distinct().count()
    # fixed: ~10 combos; decorrelated: ~70 of the 5x3x3x2 grid.
    assert combos > 30, f"promotion attr combos collapsed to {combos}"


def test_employee_full_name_not_collapsed(spark):
    df = build_dim_employee(spark, _cfg(num_stores=10))  # 400 employees
    n = df.select("full_name").distinct().count()
    # first(20) x last(20): fixed ~20 names; decorrelated ~350.
    assert n > 60, f"employee full_name distinct collapsed to {n}"


def test_customer_name_not_collapsed(spark):
    df = build_dim_customer(spark, _cfg(num_customers=600))
    combos = df.select("first_name", "last_name").distinct().count()
    # first(20) x last(20): fixed ~20 combos; decorrelated ~350.
    assert combos > 60, f"customer name combos collapsed to {combos}"
```

```python
# tests/test_no_fixed_seed_method.py
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src" / "techmart"


def test_no_builder_uses_fixed_seed_method():
    offenders = [str(p.relative_to(_SRC)) for p in _SRC.rglob("*.py")
                 if 'randomSeedMethod="fixed"' in p.read_text()]
    assert offenders == [], (
        f"randomSeedMethod=\"fixed\" correlates seeded columns; use "
        f"\"hash_fieldname\". Offending files: {offenders}"
    )
```

- [ ] **Step 2: Run the tests against the CURRENT (`"fixed"`) code and confirm they FAIL**

Run: `pytest tests/test_dim_seed_independence.py tests/test_no_fixed_seed_method.py -v`
Expected: all five independence tests FAIL (collapsed counts), and the guard FAILS listing `spark/dim_builder.py`, `spark/dimensions/dim_product.py`, `facts/fact_inventory_movement.py`, `facts/fact_web_events.py` (whichever remain — depends on task order; when run as SDD Task 4, only `dim_builder.py` should remain). **Fail-first is mandatory per dim.** If any single dim's chosen pair does not fail, swap to another seeded pair for that dim (candidates above) or adjust the threshold until it genuinely fails on `"fixed"`; if a dim shows no seeded correlation at all, report that finding rather than forcing a test.

- [ ] **Step 3: Flip the seed method**

In `src/techmart/spark/dim_builder.py` line 39, change `randomSeedMethod="fixed"` to `randomSeedMethod="hash_fieldname"`. Change nothing else. (This is the last remaining `"fixed"` once Tasks 1–3 are done.)

- [ ] **Step 4: Run the new tests and confirm they PASS**

Run: `pytest tests/test_dim_seed_independence.py tests/test_no_fixed_seed_method.py -v`
Expected: all PASS (the guard now finds zero offenders).

- [ ] **Step 5: Run the existing dim tests and fix any that broke**

Run: `pytest tests/test_dim_store_spark.py tests/test_dim_vendor_spark.py tests/test_dim_promotion_spark.py tests/test_dim_employee_spark.py tests/test_dim_customer_spark.py -v`
Expected: PASS (all structural: schema, SK ranges, null-logic). Update only a genuinely shifted distribution assertion; never weaken a schema/RI/nullability check.

- [ ] **Step 6: Run the full suite**

Run: `pytest -q`
Expected: all green (the 57 prior tests + the new spread/independence/guard tests). Investigate and fix any regression.

- [ ] **Step 7: Commit**

```bash
git add tests/test_dim_seed_independence.py tests/test_no_fixed_seed_method.py src/techmart/spark/dim_builder.py
git commit -m "Decorrelate shared SCD2 dim builder + add fixed-seed regression guard

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Task 5: Workspace smoke re-validation (controller-executed, NOT a subagent task)

This task cannot run in an implementer subagent — it requires the field-eng-east workspace credentials and a `bundle deploy`/`run`. The controller (or the human) runs it after Tasks 1–4 are merged-ready and the full local suite is green. It is the in-PR validation gate the spec calls for.

**Files:** none (deploy + query only).

- [ ] **Step 1: Deploy and run the pipeline at smoke scale**

```bash
/opt/homebrew/bin/databricks bundle deploy -t dev -p field-eng-east
/opt/homebrew/bin/databricks bundle run generate_facts -t dev -p field-eng-east
```
(CLI must be v1.6.0 at `/opt/homebrew/bin/databricks`; dev target defaults `scale_profile=smoke`, `catalog=stable_classic_ppke9o`.)

- [ ] **Step 2: Spot-check the shifted distributions landed cleanly**

Query `stable_classic_ppke9o.techmart_core` and confirm:
- dim attribute spread looks natural (no single-value collapse): e.g. `SELECT COUNT(DISTINCT city), COUNT(DISTINCT state) FROM dim_store;` well above 1; `COUNT(DISTINCT full_name) FROM dim_employee` large relative to row count; `COUNT(DISTINCT vendor_name) FROM dim_vendor` near the stem×tail ceiling.
- per-`store_sk` distinct `date_sk` in `fact_inventory_movement` is healthy (dozens, not ~2).
- per-`date_sk` `device_type`/`channel_sk` spread in `fact_web_events` is healthy (multiple devices/day).
- **RI still 0 orphans** across the affected facts (anti-join each FK to its dim).
- **finance reconciliation still exact** (finance derives from core facts, so totals shift with the new distributions but must still reconcile) — re-run the finance reconciliation checks used in Phase 5.1 validation.

- [ ] **Step 3: Record results**

Note the observed spot-check values in the SDD ledger / PR description. Any failure here blocks the PR and gets fixed before merge.

---

## Self-Review Notes

- **Spec coverage:** all four `"fixed"` sites are covered (Tasks 1–4); the 8 independence tests map to the 8 affected builders (5 dims in Task 4 + dim_product + 2 facts); the fixed-seed regression guard prevents reintroduction; the workspace smoke re-validation (Task 5) is the in-PR gate the spec requires.
- **Fail-first is the empirical safety net:** thresholds are starting estimates; each task confirms the test fails on `"fixed"` and passes after the flip, with a documented escape hatch if a builder proves empirically uncorrelated.
- **No placeholders:** every test file and edit is fully specified.
- **Type/interface consistency:** builder signatures and `ScaleProfile`/`TechmartConfig` construction match existing tests (`num_employees`/`num_promotions` are derived properties, so they are set via `num_stores`/`history_years`).
