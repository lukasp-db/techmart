# Techmart Phase 3.5 — Serverless-Native Generation (dbldatagen/PySpark + Notebooks + DAB) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-express all Techmart data generation (dimensions + `fact_sales_line`) in dbldatagen/PySpark, run it on Databricks serverless via notebooks deployed by a DAB, write Unity Catalog Delta tables with column comments, give `fact_sales_line` basket-coherent transaction ids, and prove the whole pipeline with a live smoke-scale deploy.

**Architecture:** A generalized Spark schema framework (`SparkTableSpec`/`SparkColumn`/`validate_spark_schema`/`select_ordered`) plus SCD2 and UC-write helpers back a set of dbldatagen/PySpark builders in `techmart/spark/dimensions/` and the rewritten `techmart/facts/fact_sales_line.py`. Builders are pure functions `build_x(spark, config) -> DataFrame`, unit-tested against local Spark. Thin Databricks notebooks (`notebooks/`) import the builders and run them on serverless, writing to `<catalog>.<schema_prefix>core`. A DAB (`databricks.yml` + `resources/`) deploys two dependency-ordered `notebook_task`s (dims → facts). The superseded Polars generation stack is removed only after the Spark path is proven on the workspace.

**Tech Stack:** Python 3.10+, PySpark 3.5, `dbldatagen` 0.4, Databricks Asset Bundles, Databricks serverless, pytest.

## Global Constraints

*(Every task's requirements implicitly include this section. Values copied verbatim from the spec.)*

- **Serverless-native Spark:** all generation is dbldatagen/PySpark, run on Databricks serverless via notebooks; no local Polars in the generation path, no classic clusters. Builders take an injected `spark` session (the notebook global on serverless; a local session in tests).
- **dbldatagen determinism:** every generator sets `randomSeed=config.seed, randomSeedMethod="fixed"`; per-line fact attributes are derived deterministically from `hash(...)` (partition-independent), not `rand()`. Runs are reproducible.
- **Referential integrity by construction:** fact FK ranges are sized from the **actual dimension row counts** read at generation time (`dim.count()`), never from the scale profile — every FK points at a real surrogate key. Surrogate keys are sequential `1..N` (`withIdOutput()` + `expr="id + 1"`).
- **Comments (Genie):** every column carries a comment; `SparkTableSpec.select_ordered` attaches it as `StructField` metadata and `saveAsTable` propagates it to the Delta column comment.
- **Single catalog, `techmart_*` schemas:** tables are written to `<catalog>.<schema_prefix>core` (default `stable_classic_ppke9o.techmart_core`). The UC-write helper runs `CREATE SCHEMA IF NOT EXISTS` first. Idempotent overwrite (`mode("overwrite").option("overwriteSchema","true")`).
- **Basket coherence:** `fact_sales_line` lines sharing a `transaction_id` carry identical `date_sk`/`store_sk`/`customer_sk`/`employee_sk`/`channel_sk` (built via a header row exploded over `sequence(1, basket_size)`).
- **Money as `double`, keys as `long` (BIGINT).** Column names/types/comments of each dimension match the retired Polars `DIM_*_SPEC` exactly (the star schema and any downstream stay valid), except the SCD2 timestamps become Spark `timestamp`.
- **Catalyst-safe generation** so builders run locally and on serverless: `values=`/`weights=`, `minValue`/`maxValue`, `expr=`, `begin`/`end`/`interval`, `percentNulls=`, `omit=True`, `withIdOutput()`, `explode(sequence(...))`, `element_at(array(...), idx)`, `hash`/`pmod`. Always set `partitions` explicitly.

---

## File Structure

- `src/techmart/spark/framework.py` (modify) — rename `FactColumn`→`SparkColumn`, `FactSpec`→`SparkTableSpec`, `validate_fact_schema`→`validate_spark_schema`; keep `select_ordered`.
- `src/techmart/spark/scd2.py` (create) — `scd2_columns()`, `with_scd2_current(df, start)` for Spark.
- `src/techmart/spark/uc_write.py` (create) — `write_table_uc(spark, df, spec, catalog, schema_prefix)`.
- `src/techmart/reference/pools.py` (create) — shared name/geo pools (`FIRST_NAMES`, `LAST_NAMES`, `US_STATES`, `CITIES`).
- `src/techmart/spark/dimensions/__init__.py` (create) and `dim_channel.py`, `dim_date.py`, `dim_store.py`, `dim_vendor.py`, `dim_promotion.py`, `dim_employee.py`, `dim_customer.py`, `dim_product.py` (create).
- `src/techmart/spark/calendar.py` (create) — reused fiscal-4-5-4 + holiday pure functions (moved from the Polars `dim_date`).
- `src/techmart/facts/fact_sales_line.py` (modify) — basket rewrite; FK ranges from dim counts.
- `src/techmart/facts/lookups.py` (modify) — `product_economics(dim_product_spark)`, `date_seasonality_weights` (unchanged); drop `polars_to_spark`.
- `notebooks/generate_dims.py`, `notebooks/generate_facts.py` (create) — Databricks source notebooks.
- `databricks.yml` (modify), `resources/generate_facts_job.yml` (modify) — two serverless `notebook_task`s.
- `config/scale_profiles.yaml` (modify) — add `smoke`.
- `README.md` (modify) — update deploy section.
- Tests: `tests/test_spark_framework.py` (rename refs), `tests/test_scd2_spark.py`, `tests/test_uc_write.py`, `tests/test_dim_*_spark.py` (8), `tests/test_fact_sales_line.py` (rewrite), `tests/test_dab_bundle.py` (update).
- **Removed in the final task** (superseded): `src/techmart/dimensions/`, `src/techmart/framework/`, `src/techmart/registry.py`, `src/techmart/cli.py`, `src/techmart/rng.py`, and their tests.

---

### Task 1: Generalize the Spark framework; add SCD2, UC-write, and shared pools

**Files:**
- Modify: `src/techmart/spark/framework.py`
- Create: `src/techmart/spark/scd2.py`, `src/techmart/spark/uc_write.py`, `src/techmart/reference/pools.py`
- Modify: `src/techmart/facts/fact_sales_line.py`, `src/techmart/facts/registry.py`, `tests/test_fact_sales_line.py`
- Rename/Modify test: `tests/test_fact_framework.py` → keep name, update symbols
- Test: `tests/test_scd2_spark.py`, `tests/test_uc_write.py`

**Interfaces:**
- Produces: `SparkColumn(name, dtype, comment, is_key=False, nullable=True)`, `SparkTableSpec(schema, name, grain, columns)` with `.column_names`, `.struct_type()`, `.select_ordered(df)`; `validate_spark_schema(df, spec)` raising `SparkSchemaMismatchError`. `scd2_columns() -> list[SparkColumn]`; `with_scd2_current(df, start: date) -> DataFrame`. `write_table_uc(spark, df, spec, catalog, schema_prefix) -> str`.

- [ ] **Step 1: Rename framework symbols**

In `src/techmart/spark/framework.py` rename (mechanical, whole-file): `FactColumn`→`SparkColumn`, `FactSpec`→`SparkTableSpec`, `FactSchemaMismatchError`→`SparkSchemaMismatchError`, `validate_fact_schema`→`validate_spark_schema`. Keep `select_ordered` and the `metadata={"comment": ...}` behavior. Update the module docstring/comments to say "Spark table spec (dims and facts)".

- [ ] **Step 2: Update framework tests to the new symbols**

In `tests/test_fact_framework.py`, update imports/usages to `SparkColumn`/`SparkTableSpec`/`validate_spark_schema`/`SparkSchemaMismatchError`. Keep every existing assertion (including the comment-metadata and extra-column tests).

Run: `python -m pytest tests/test_fact_framework.py -q` → Expected: 7 passed.

- [ ] **Step 3: Point the fact and its tests at the renamed symbols**

In `src/techmart/facts/fact_sales_line.py`, `src/techmart/facts/registry.py`, and `tests/test_fact_sales_line.py`, replace `FactColumn`/`FactSpec`/`validate_fact_schema` with the new names. (The fact's generation logic is rewritten in Task 7; here only the symbol names change.)

Run: `python -m pytest tests/test_fact_sales_line.py tests/test_generate_facts.py -q` → Expected: all pass (same counts as before).

- [ ] **Step 4: Write the SCD2 helper test**

Create `tests/test_scd2_spark.py`:

```python
from datetime import date

from pyspark.sql.types import BooleanType, IntegerType, TimestampType

from techmart.spark.scd2 import scd2_columns, with_scd2_current


def test_scd2_columns_shape():
    cols = scd2_columns()
    assert [c.name for c in cols] == [
        "effective_start_ts", "effective_end_ts", "is_current", "version"
    ]
    assert cols[0].nullable is False and cols[1].nullable is True


def test_with_scd2_current_values(spark):
    df = spark.createDataFrame([(1,), (2,)], "store_sk long")
    out = with_scd2_current(df, date(2023, 2, 1))
    assert set(["effective_start_ts", "effective_end_ts", "is_current", "version"]).issubset(out.columns)
    assert isinstance(out.schema["effective_start_ts"].dataType, TimestampType)
    assert isinstance(out.schema["is_current"].dataType, BooleanType)
    assert isinstance(out.schema["version"].dataType, IntegerType)
    row = out.orderBy("store_sk").first()
    assert row["is_current"] is True and row["version"] == 1
    assert row["effective_end_ts"] is None
    assert row["effective_start_ts"].date() == date(2023, 2, 1)
```

- [ ] **Step 5: Run it (fails)** — `python -m pytest tests/test_scd2_spark.py -q` → FAIL (`No module named 'techmart.spark.scd2'`).

- [ ] **Step 6: Implement the SCD2 helper**

Create `src/techmart/spark/scd2.py`:

```python
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, functions as F

from .framework import SparkColumn


def scd2_columns() -> list[SparkColumn]:
    """The four SCD Type 2 control columns appended to every SCD2 dimension."""
    return [
        SparkColumn("effective_start_ts", "timestamp", "SCD2 effective start timestamp", nullable=False),
        SparkColumn("effective_end_ts", "timestamp", "SCD2 effective end timestamp; null when current"),
        SparkColumn("is_current", "boolean", "True for the current version of the row", nullable=False),
        SparkColumn("version", "int", "SCD2 version number (1-based)", nullable=False),
    ]


def with_scd2_current(df: DataFrame, start: date) -> DataFrame:
    """Append SCD2 columns marking every row as the current (version 1) record."""
    start_ts = datetime(start.year, start.month, start.day)
    return (
        df.withColumn("effective_start_ts", F.lit(start_ts).cast("timestamp"))
        .withColumn("effective_end_ts", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("version", F.lit(1).cast("int"))
    )
```

Note: `framework.py` must map `"timestamp"` in `_SPARK_TYPES`. Add `from pyspark.sql.types import TimestampType` and `"timestamp": TimestampType()` to `_SPARK_TYPES` in `framework.py`.

- [ ] **Step 7: Run it (passes)** — `python -m pytest tests/test_scd2_spark.py -q` → 2 passed.

- [ ] **Step 8: Write the UC-write helper test**

`write_table_uc` calls `saveAsTable`, which needs a real metastore, so the test covers the pieces that run locally: schema validation + comment projection. Create `tests/test_uc_write.py`:

```python
from techmart.spark.framework import SparkColumn, SparkTableSpec
from techmart.spark.uc_write import target_table_name

SPEC = SparkTableSpec(
    schema="core", name="dim_demo", grain="one per demo",
    columns=[SparkColumn("demo_sk", "long", "Surrogate key", is_key=True, nullable=False)],
)


def test_target_table_name():
    assert target_table_name(SPEC, "cat", "techmart_") == "cat.techmart_core.dim_demo"
```

- [ ] **Step 9: Run it (fails)** — `python -m pytest tests/test_uc_write.py -q` → FAIL.

- [ ] **Step 10: Implement the UC-write helper**

Create `src/techmart/spark/uc_write.py`:

```python
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .framework import SparkTableSpec, validate_spark_schema


def target_table_name(spec: SparkTableSpec, catalog: str, schema_prefix: str) -> str:
    """Fully-qualified UC table name: <catalog>.<schema_prefix><group>.<name>."""
    return f"{catalog}.{schema_prefix}{spec.schema}.{spec.name}"


def write_table_uc(
    spark: SparkSession,
    df: DataFrame,
    spec: SparkTableSpec,
    catalog: str,
    schema_prefix: str,
) -> str:
    """Validate, attach comments, and write a Delta table to Unity Catalog (idempotent)."""
    validate_spark_schema(df, spec)
    schema_fqn = f"{catalog}.{schema_prefix}{spec.schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
    target = target_table_name(spec, catalog, schema_prefix)
    (
        spec.select_ordered(df)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    return target
```

- [ ] **Step 11: Run it (passes)** — `python -m pytest tests/test_uc_write.py -q` → 1 passed.

- [ ] **Step 12: Create the shared reference pools**

Create `src/techmart/reference/pools.py` (copied verbatim from the Polars `dimensions/support.py` lists — these are engine-agnostic and outlive the Polars removal):

```python
from __future__ import annotations

US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA",
    "NC", "MI", "WA", "AZ", "MA", "CO", "OR",
]
CITIES = [
    "Springfield", "Riverside", "Franklin", "Greenville", "Bristol",
    "Fairview", "Salem", "Georgetown", "Madison", "Clinton",
    "Arlington", "Ashland", "Dover", "Auburn", "Hudson",
]
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard",
    "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Carlos", "Maria",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Lee",
]
```

- [ ] **Step 13: Full suite + commit**

Run: `python -m pytest -q` → Expected: all pass (existing Polars tests untouched + new Spark tests).

```bash
git add src/techmart/spark/ src/techmart/reference/pools.py src/techmart/facts/ tests/test_fact_framework.py tests/test_fact_sales_line.py tests/test_generate_facts.py tests/test_scd2_spark.py tests/test_uc_write.py
git commit -m "feat: generalize Spark framework; add SCD2, UC-write helpers and shared pools"
```

---

### Task 2: `dim_date` and `dim_channel` (deterministic, PySpark)

**Files:**
- Create: `src/techmart/spark/calendar.py`, `src/techmart/spark/dimensions/__init__.py`, `src/techmart/spark/dimensions/dim_date.py`, `src/techmart/spark/dimensions/dim_channel.py`
- Test: `tests/test_dim_date_spark.py`, `tests/test_dim_channel_spark.py`

**Interfaces:**
- Produces: `DIM_DATE_SPEC`, `build_dim_date(spark, config) -> DataFrame`; `DIM_CHANNEL_SPEC`, `build_dim_channel(spark, config) -> DataFrame`. Both are deterministic (not sampled) and built via `spark.createDataFrame` over computed rows.

- [ ] **Step 1: Move the calendar logic**

Create `src/techmart/spark/calendar.py` by copying — verbatim — these pure functions and constants from the Polars `src/techmart/dimensions/dim_date.py`: `_DAY_NAMES`, `_MONTH_NAMES`, `_MONTH_SEASON`, `_PERIOD_WEEKS`, `_first_sunday_of_february`, `_nth_weekday`, `_last_weekday`, `fiscal_attrs`, `holiday_name`. (Engine-agnostic; no Polars import.)

- [ ] **Step 2: Write the dim_date test**

Create `tests/test_dim_date_spark.py`:

```python
from datetime import date

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_date_schema_and_span(spark):
    df = build_dim_date(spark, _CFG)
    validate_spark_schema(df, DIM_DATE_SPEC)
    assert df.columns == DIM_DATE_SPEC.column_names
    # One row per day across the history window.
    span_days = (_CFG.end_date - _CFG.start_date).days + 1
    assert df.count() == span_days


def test_dim_date_known_values(spark):
    df = build_dim_date(spark, _CFG)
    from pyspark.sql import functions as F
    row = df.filter(F.col("date_sk") == 20251225).first()
    assert row["is_holiday"] is True and row["holiday_name"] == "Christmas Day"
    assert row["month_name"] == "December"
```

- [ ] **Step 3: Run it (fails)** — FAIL (`No module named ...dim_date`).

- [ ] **Step 4: Implement dim_date**

Create `src/techmart/spark/dimensions/__init__.py` (empty) and `src/techmart/spark/dimensions/dim_date.py`:

```python
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ..calendar import (
    _DAY_NAMES, _MONTH_NAMES, _MONTH_SEASON, fiscal_attrs, holiday_name,
)
from ..framework import SparkColumn, SparkTableSpec

DIM_DATE_SPEC = SparkTableSpec(
    schema="core",
    name="dim_date",
    grain="one row per calendar day",
    columns=[
        SparkColumn("date_sk", "long", "Surrogate key in yyyymmdd form", is_key=True, nullable=False),
        SparkColumn("date", "string", "Calendar date (ISO yyyy-mm-dd)", nullable=False),
        SparkColumn("day_of_week", "int", "ISO day of week (1=Mon..7=Sun)"),
        SparkColumn("day_name", "string", "Full day name (e.g. 'Monday')"),
        SparkColumn("week", "int", "ISO week number"),
        SparkColumn("month", "int", "Calendar month (1-12)"),
        SparkColumn("month_name", "string", "Full month name (e.g. 'January')"),
        SparkColumn("quarter", "int", "Calendar quarter (1-4)"),
        SparkColumn("year", "int", "Calendar year"),
        SparkColumn("fiscal_year", "int", "Retail 4-5-4 fiscal year"),
        SparkColumn("fiscal_week", "int", "Retail fiscal week (1-53)"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("fiscal_quarter", "int", "Retail fiscal quarter (1-4)"),
        SparkColumn("is_weekend", "boolean", "True on Saturday or Sunday"),
        SparkColumn("is_holiday", "boolean", "True on a recognized US holiday"),
        SparkColumn("holiday_name", "string", "Holiday name, else null"),
        SparkColumn("selling_season", "string", "Retail selling-season label"),
    ],
)


def build_dim_date(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    start, end = config.start_date, config.end_date
    rows = []
    d = start
    while d <= end:
        fy, fw, fp, fq = fiscal_attrs(d)
        hn = holiday_name(d)
        rows.append((
            d.year * 10000 + d.month * 100 + d.day,
            d.isoformat(),
            d.isoweekday(), _DAY_NAMES[d.weekday()], d.isocalendar()[1],
            d.month, _MONTH_NAMES[d.month - 1], (d.month - 1) // 3 + 1, d.year,
            fy, fw, fp, fq,
            d.weekday() >= 5, hn is not None, hn, _MONTH_SEASON[d.month],
        ))
        d += timedelta(days=1)
    return spark.createDataFrame(rows, schema=DIM_DATE_SPEC.struct_type())
```

- [ ] **Step 5: Run it (passes)** — 2 passed.

- [ ] **Step 6: Write the dim_channel test**

Create `tests/test_dim_channel_spark.py`:

```python
from datetime import date

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_channel(spark):
    df = build_dim_channel(spark, _CFG)
    validate_spark_schema(df, DIM_CHANNEL_SPEC)
    rows = {r["channel_sk"]: r for r in df.collect()}
    assert df.count() == 5
    assert rows[1]["channel_name"] == "In-Store" and rows[1]["channel_type"] == "Physical"
    assert rows[4]["channel_name"] == "Marketplace" and rows[4]["channel_type"] == "Digital"
```

- [ ] **Step 7: Run it (fails), then implement**

Create `src/techmart/spark/dimensions/dim_channel.py`:

```python
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ..framework import SparkColumn, SparkTableSpec

DIM_CHANNEL_SPEC = SparkTableSpec(
    schema="core",
    name="dim_channel",
    grain="one row per sales/interaction channel",
    columns=[
        SparkColumn("channel_sk", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("channel_id", "string", "Business key", nullable=False),
        SparkColumn("channel_name", "string", "Channel name"),
        SparkColumn("channel_type", "string", "Physical or Digital"),
    ],
)

_CHANNELS = [
    ("In-Store", "Physical"), ("Web", "Digital"), ("Mobile-App", "Digital"),
    ("Marketplace", "Digital"), ("Call-Center", "Physical"),
]


def build_dim_channel(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    rows = [
        (i, f"CH{i:02d}", name, ctype)
        for i, (name, ctype) in enumerate(_CHANNELS, start=1)
    ]
    return spark.createDataFrame(rows, schema=DIM_CHANNEL_SPEC.struct_type())
```

- [ ] **Step 8: Run both tests, then commit**

Run: `python -m pytest tests/test_dim_date_spark.py tests/test_dim_channel_spark.py -q` → 4 passed.

```bash
git add src/techmart/spark/calendar.py src/techmart/spark/dimensions/ tests/test_dim_date_spark.py tests/test_dim_channel_spark.py
git commit -m "feat: add Spark dim_date and dim_channel builders"
```

---

### Task 3: `dim_store` and `dim_vendor` (dbldatagen + SCD2)

**Files:**
- Create: `src/techmart/spark/dimensions/dim_store.py`, `src/techmart/spark/dimensions/dim_vendor.py`
- Test: `tests/test_dim_store_spark.py`, `tests/test_dim_vendor_spark.py`

**Interfaces:**
- Produces: `DIM_STORE_SPEC`/`build_dim_store(spark, config)`, `DIM_VENDOR_SPEC`/`build_dim_vendor(spark, config)`. Row counts `config.scale_profile.num_stores` / `num_vendors`; sequential SKs; SCD2 current-row columns; column names/types/comments match the retired Polars `DIM_STORE_SPEC`/`DIM_VENDOR_SPEC` (SCD2 timestamps as Spark `timestamp`).

**Notes for the implementer:**
- Standard builder shape: `dg.DataGenerator(spark, name=..., rows=n, partitions=max(1, min(64, n // 100_000)), randomSeed=config.seed, randomSeedMethod="fixed").withIdOutput()`, then `withColumn(...)`, then `.build().drop("id")`, then `with_scd2_current(df, config.start_date)`, then `return SPEC.select_ordered(df)`.
- Sequential SK: `.withColumn("store_sk", "long", expr="id + 1", baseColumn="id")`.
- Business key: `.withColumn("store_id", "string", expr="concat('STORE', lpad(cast(id + 1 as string), 5, '0'))", baseColumn="id")`.
- Name-from-index: pick an index then map, e.g. region — `.withColumn("region_num", "int", minValue=1, maxValue=5, random=True, omit=True)` then `.withColumn("region_name", "string", expr="element_at(array('Northeast','Southeast','Midwest','Southwest','West'), region_num)", baseColumn="region_num")` and `.withColumn("region_id", "string", expr="concat('RGN', cast(region_num as string))", baseColumn="region_num")`.
- Get the exact columns/comments and value lists from the retired Polars `src/techmart/dimensions/dim_store.py` and `dim_vendor.py` (still present this task) — reproduce every column with the same name/comment/type; SCD2 columns come from `scd2_columns()`.

- [ ] **Step 1: Write the store test** — Create `tests/test_dim_store_spark.py`:

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_store import DIM_STORE_SPEC, build_dim_store
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 50, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_store(spark):
    df = build_dim_store(spark, _CFG)
    validate_spark_schema(df, DIM_STORE_SPEC)
    assert df.count() == 50
    r = df.agg(F.min("store_sk").alias("lo"), F.max("store_sk").alias("hi"),
               F.countDistinct("store_sk").alias("d")).first()
    assert r["lo"] == 1 and r["hi"] == 50 and r["d"] == 50
    assert df.filter(~F.col("is_current")).count() == 0
    assert df.filter(F.col("country") != "US").count() == 0
```

- [ ] **Step 2: Run (fails), implement `dim_store.py`, run (passes).** Port every column from the Polars `dim_store.py` spec. Expected: test passes.

- [ ] **Step 3: Write the vendor test** — Create `tests/test_dim_vendor_spark.py` (same shape; `num_vendors=20`, assert count 20, SK 1..20, `active_flag` all true, `vendor_scorecard_rating` between 1 and 5):

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_vendor import DIM_VENDOR_SPEC, build_dim_vendor
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 50, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_vendor(spark):
    df = build_dim_vendor(spark, _CFG)
    validate_spark_schema(df, DIM_VENDOR_SPEC)
    assert df.count() == 20
    r = df.agg(F.min("vendor_scorecard_rating").alias("lo"),
               F.max("vendor_scorecard_rating").alias("hi")).first()
    assert r["lo"] >= 1 and r["hi"] <= 5
    assert df.filter(~F.col("active_flag")).count() == 0
```

- [ ] **Step 4: Run (fails), implement `dim_vendor.py`, run (passes).** Port every column from the Polars `dim_vendor.py`. `vendor_name` via `expr="concat(element_at(array(<stems>), s), ' ', element_at(array(<tails>), t))"` with omitted index columns `s`/`t`.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/spark/dimensions/dim_store.py src/techmart/spark/dimensions/dim_vendor.py tests/test_dim_store_spark.py tests/test_dim_vendor_spark.py
git commit -m "feat: add Spark dim_store and dim_vendor builders"
```

---

### Task 4: `dim_promotion` and `dim_employee` (dbldatagen + SCD2)

**Files:**
- Create: `src/techmart/spark/dimensions/dim_promotion.py`, `src/techmart/spark/dimensions/dim_employee.py`
- Test: `tests/test_dim_promotion_spark.py`, `tests/test_dim_employee_spark.py`

**Interfaces:**
- Produces: `DIM_PROMOTION_SPEC`/`build_dim_promotion(spark, config)` (rows `num_promotions`), `DIM_EMPLOYEE_SPEC`/`build_dim_employee(spark, config)` (rows `num_employees`). Columns/types/comments match the retired Polars specs.

**Notes for the implementer:**
- **dim_promotion date window:** `start_date`/`end_date` are `date` (Spark `date`). Compute a start-day offset in `[0, span-30)` and a duration in `[3, 30)` as omitted int columns, then derive dates with `expr="date_add(to_date('<start_iso>'), start_offset)"` and `expr="least(date_add(start_date, duration), to_date('<end_iso>'))"` (clamp to `config.end_date`). Pass `config.start_date.isoformat()`/`config.end_date.isoformat()` into the exprs.
- **dim_employee:** `store_sk` FK is `minValue=1, maxValue=config.scale_profile.num_stores`; `manager_employee_sk` is `minValue=1, maxValue=n` but **null for Managers** — generate `role` first, then `.withColumn("manager_employee_sk", "long", expr="case when role = 'Manager' then null else pmod(abs(hash(id, 'mgr')), <n>) + 1 end", baseColumn=["id","role"])`. `term_date` is null (Spark `date`): `expr="cast(null as date)"`. `full_name` via `element_at(array(<FIRST_NAMES>), fi)` + `' '` + `element_at(array(<LAST_NAMES>), li)` using omitted index columns and `techmart.reference.pools`.

- [ ] **Step 1: Write the promotion test** — Create `tests/test_dim_promotion_spark.py`:

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 50, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_promotion(spark):
    df = build_dim_promotion(spark, _CFG)
    validate_spark_schema(df, DIM_PROMOTION_SPEC)
    assert df.count() == _CFG.scale_profile.num_promotions
    # end never after the history window, and not before start.
    assert df.filter(F.col("end_date") > F.lit(_CFG.end_date)).count() == 0
    assert df.filter(F.col("end_date") < F.col("start_date")).count() == 0
```

- [ ] **Step 2: Run (fails), implement `dim_promotion.py`, run (passes).**

- [ ] **Step 3: Write the employee test** — Create `tests/test_dim_employee_spark.py`:

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_employee import DIM_EMPLOYEE_SPEC, build_dim_employee
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 1000, 20),  # num_employees = 40*5 = 200
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_employee(spark):
    df = build_dim_employee(spark, _CFG)
    validate_spark_schema(df, DIM_EMPLOYEE_SPEC)
    assert df.count() == _CFG.scale_profile.num_employees  # 200
    r = df.agg(F.min("store_sk").alias("lo"), F.max("store_sk").alias("hi")).first()
    assert r["lo"] >= 1 and r["hi"] <= _CFG.scale_profile.num_stores
    # Managers have no manager; non-managers do.
    assert df.filter((F.col("role") == "Manager") & F.col("manager_employee_sk").isNotNull()).count() == 0
    assert df.filter((F.col("role") != "Manager") & F.col("manager_employee_sk").isNull()).count() == 0
```

- [ ] **Step 4: Run (fails), implement `dim_employee.py`, run (passes).**

- [ ] **Step 5: Commit**

```bash
git add src/techmart/spark/dimensions/dim_promotion.py src/techmart/spark/dimensions/dim_employee.py tests/test_dim_promotion_spark.py tests/test_dim_employee_spark.py
git commit -m "feat: add Spark dim_promotion and dim_employee builders"
```

---

### Task 5: `dim_customer` (dbldatagen, loyalty null logic)

**Files:**
- Create: `src/techmart/spark/dimensions/dim_customer.py`
- Test: `tests/test_dim_customer_spark.py`

**Interfaces:**
- Produces: `DIM_CUSTOMER_SPEC`/`build_dim_customer(spark, config)` (rows `num_customers`). Columns/types/comments match the retired Polars spec.

**Notes for the implementer:**
- `customer_id` = `concat('CUST', lpad(cast(id+1 as string), 8, '0'))`. `email` via `expr="concat(lower(first_name), '.', lower(last_name), '.', customer_id, '@example.com')"` with `baseColumn=["first_name","last_name","customer_id"]`.
- `loyalty_member_flag` boolean (`expr="pmod(abs(hash(id,'m')),2) = 0"`). `loyalty_tier`: `element_at(array('Bronze','Silver','Gold','Platinum'), tier_num)` when member else `'None'`. `loyalty_enroll_date`: a random date when member, else null — generate an omitted offset and use `expr="case when loyalty_member_flag then date_add(to_date('2015-01-01'), off) else cast(null as date) end"`.
- `first_name`/`last_name`/`city`/`state` via `element_at(array(...), idx)` over the `techmart.reference.pools` lists.

- [ ] **Step 1: Write the test** — Create `tests/test_dim_customer_spark.py`:

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_customer import DIM_CUSTOMER_SPEC, build_dim_customer
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 500, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_customer(spark):
    df = build_dim_customer(spark, _CFG)
    validate_spark_schema(df, DIM_CUSTOMER_SPEC)
    assert df.count() == 500
    # Non-members have no tier and no enroll date; members do.
    assert df.filter((~F.col("loyalty_member_flag")) & (F.col("loyalty_tier") != "None")).count() == 0
    assert df.filter((~F.col("loyalty_member_flag")) & F.col("loyalty_enroll_date").isNotNull()).count() == 0
    assert df.filter(F.col("loyalty_member_flag") & F.col("loyalty_enroll_date").isNull()).count() == 0
    assert df.filter(~F.col("email").contains("@")).count() == 0
```

- [ ] **Step 2: Run (fails), implement `dim_customer.py`, run (passes).**

- [ ] **Step 3: Commit**

```bash
git add src/techmart/spark/dimensions/dim_customer.py tests/test_dim_customer_spark.py
git commit -m "feat: add Spark dim_customer builder"
```

---

### Task 6: `dim_product` (taxonomy join)

**Files:**
- Create: `src/techmart/spark/dimensions/dim_product.py`
- Test: `tests/test_dim_product_spark.py`

**Interfaces:**
- Produces: `DIM_PRODUCT_SPEC`/`build_dim_product(spark, config)` (rows `num_skus`). Columns/types/comments match the retired Polars `DIM_PRODUCT_SPEC`.

**Notes for the implementer:**
- Reuse `techmart.reference.taxonomy` (engine-agnostic). Build a **paths-with-brand lookup DataFrame** once from `subcategory_paths()`: for each path (division/department/category/subcategory) expand over that category's brands into rows, assign a 0-based `path_brand_idx`, columns `division_id/name, department_id/name, category_id/name, subcategory_id/name, brand_name` (+ `brand_id = upper(replace(brand_name,' ',''))`). Create it via `spark.createDataFrame(rows)`.
- Generate the product base with dbldatagen: `product_sk` (id+1), `sku`/`model_number`/`gtin`, `path_brand_idx` (`minValue=0, maxValue=<num_path_brands-1>, random=True`), `primary_vendor_sk` (`minValue=1, maxValue=num_vendors`), `color`, prices (`msrp` then `list_price`/`standard_cost` via `expr` on msrp), `weight_kg`, `dimensions`, `uom`, flags, lifecycle. `is_marketplace` via `expr="pmod(abs(hash(id,'mkt')),100) < 15"`; `marketplace_seller_id` null unless marketplace.
- `join` the product base to the paths-with-brand lookup on `path_brand_idx` (drop the helper index). `manufacturer` = `brand_name` (differentiation deferred). `spec_attributes` as a JSON string via `to_json(struct(color, weight_kg, brand_name))` (robust JSON — resolves the earlier Phase-2C carry-forward). `product_name`/`product_description` via `expr` concatenations. `launch_date` random in [2015-01-01, 2024-06-01]; `discontinue_date` non-null only when `lifecycle_status='Discontinued'`, clamped `<= config.end_date` (resolves the earlier carry-forward), else null.

- [ ] **Step 1: Write the test** — Create `tests/test_dim_product_spark.py`:

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 300, 1, 50000, 500, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_product(spark):
    df = build_dim_product(spark, _CFG)
    validate_spark_schema(df, DIM_PRODUCT_SPEC)
    assert df.count() == 300
    r = df.agg(F.min("product_sk").alias("lo"), F.max("product_sk").alias("hi"),
               F.countDistinct("product_sk").alias("d")).first()
    assert r["lo"] == 1 and r["hi"] == 300 and r["d"] == 300
    # Hierarchy always populated; primary_vendor_sk in range; JSON specs parse.
    assert df.filter(F.col("division_name").isNull() | F.col("brand_name").isNull()).count() == 0
    assert df.filter((F.col("primary_vendor_sk") < 1) | (F.col("primary_vendor_sk") > 20)).count() == 0
    assert df.filter(F.get_json_object(F.col("spec_attributes"), "$.brand").isNull()).count() == 0
    # discontinue_date only when discontinued, and within the window.
    assert df.filter((F.col("lifecycle_status") != "Discontinued") & F.col("discontinue_date").isNotNull()).count() == 0
    assert df.filter(F.col("discontinue_date") > F.lit(_CFG.end_date)).count() == 0
```

- [ ] **Step 2: Run (fails), implement `dim_product.py`, run (passes).**

- [ ] **Step 3: Commit**

```bash
git add src/techmart/spark/dimensions/dim_product.py tests/test_dim_product_spark.py
git commit -m "feat: add Spark dim_product builder (taxonomy join, JSON specs)"
```

---

### Task 7: `fact_sales_line` — basket rewrite (header → explode)

**Files:**
- Modify: `src/techmart/facts/fact_sales_line.py`, `src/techmart/facts/lookups.py`
- Rewrite test: `tests/test_fact_sales_line.py`; Modify: `tests/test_lookups.py`, `tests/test_generate_facts.py`, `src/techmart/jobs/generate_facts.py`

**Interfaces:**
- Produces: `FACT_SALES_LINE_SPEC` (unchanged columns) and
  `build_fact_sales_line(spark, config, *, dim_product, dim_date, dim_counts, rows=None, seed=None) -> DataFrame`
  where `dim_product`/`dim_date` are **Spark** DataFrames (read from UC or built locally) and
  `dim_counts` is a dict `{"store": int, "customer": int, "employee": int, "promotion": int, "product": int}`
  giving the actual dimension row counts (FK ranges — RI by construction).
- `lookups.py`: `product_economics(dim_product_spark) -> DataFrame` (select `product_sk,list_price,standard_cost,msrp`); `date_seasonality_weights(dim_date_spark)` unchanged. Remove `polars_to_spark`.

**Notes for the implementer (verified patterns):**
- `AVG_BASKET = 2.9`; `num_transactions = max(1, round(target_lines / AVG_BASKET))` where `target_lines = rows if rows is not None else config.scale_profile.sales_lines_target`.
- Header via dbldatagen (`randomSeed`, `randomSeedMethod="fixed"`, `withIdOutput()`): `transaction_id = id+1`; `date_sk` (`values=date_sks, weights=weights` from `date_seasonality_weights`); `store_sk`/`customer_sk`/`employee_sk` (`minValue=1, maxValue=dim_counts[...]`); `channel_sk` (`values=[1,2,3,4,5], weights=[50,28,15,5,2]`); `basket_size` (`values=[1..8], weights=[25,25,18,12,8,6,4,2]`).
- Explode to lines: `.withColumn("line_number", F.explode(F.sequence(F.lit(1), F.col("basket_size")))).drop("basket_size")`.
- Deterministic per-line attrs via `hash` (partition-independent — this is why runs reproduce):
  - `u(salt) = pmod(hash(transaction_id, line_number, lit(salt)), 1_000_000) / 1_000_000.0`
  - `product_sk = (floor(pow(u('p'), 3.0) * dim_counts['product']) + 1).cast('long')` (long-tail; RI in `1..product`)
  - `quantity = (pmod(hash(transaction_id, line_number, lit('q')), 5) + 1).cast('int')`
  - `promotion_sk = when(u('pr') < 0.22, (pmod(hash(transaction_id, line_number, lit('ps')), dim_counts['promotion']) + 1).cast('long')).otherwise(lit(None).cast('long'))`
  - `tender_type = element_at(array('Card','Card','Card','Cash','Gift Card','Mobile Pay'), (pmod(hash(transaction_id, line_number, lit('t')), 6) + 1))`
- Join `product_economics(dim_product)` on `product_sk` (LEFT, alias+drop the econ key). Measure chain unchanged: `unit_price=round(list_price,2)`, `unit_cost=round(standard_cost,2)`, `receipt_id="RCPT-"||transaction_id`, `gross=round(quantity*unit_price,2)`, `discount = when(promotion_sk is not null, round(gross*0.12,2)).otherwise(0.0)`, `net=round(gross-discount,2)`, `tax=round(net*0.07,2)`, `cogs=round(quantity*unit_cost,2)`, `gross_margin=round(net-cogs,2)`, `loyalty_points_earned=floor(net).cast('long')`, `is_return=false`, `is_marketplace=(channel_sk==4)`. Final `return FACT_SALES_LINE_SPEC.select_ordered(df)`.

- [ ] **Step 1: Rewrite the fact test** — Create `tests/test_fact_sales_line.py` (build dims with the new Spark builders; assert schema, ~row count, RI for all FKs, measure invariants, basket coherence, determinism):

```python
from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line

_P = ScaleProfile("t", 10, 40, 1, 3000, 200, 20)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 10, "customer": 200, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "product": 40}


def _build(spark, rows=3000):
    dp = build_dim_product(spark, _CFG)
    dd = build_dim_date(spark, _CFG)
    return build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=rows)


def test_schema_and_columns(spark):
    df = _build(spark)
    assert df.columns == FACT_SALES_LINE_SPEC.column_names
    assert df.count() > 0


def test_referential_integrity(spark):
    df = _build(spark)
    r = df.agg(
        F.min("product_sk").alias("plo"), F.max("product_sk").alias("phi"),
        F.min("store_sk").alias("slo"), F.max("store_sk").alias("shi"),
        F.min("customer_sk").alias("culo"), F.max("customer_sk").alias("cuhi"),
        F.min("employee_sk").alias("elo"), F.max("employee_sk").alias("ehi"),
        F.min("channel_sk").alias("chlo"), F.max("channel_sk").alias("chhi"),
        F.count(F.when(F.col("unit_price").isNull(), 1)).alias("nullprice"),
    ).first()
    assert 1 <= r["plo"] and r["phi"] <= 40
    assert 1 <= r["slo"] and r["shi"] <= 10
    assert 1 <= r["culo"] and r["cuhi"] <= 200
    assert 1 <= r["elo"] and r["ehi"] <= _P.num_employees
    assert 1 <= r["chlo"] and r["chhi"] <= 5
    assert r["nullprice"] == 0
    promo = df.filter(F.col("promotion_sk").isNotNull()).agg(
        F.min("promotion_sk").alias("lo"), F.max("promotion_sk").alias("hi")).first()
    assert promo["lo"] >= 1 and promo["hi"] <= _P.num_promotions


def test_measures_and_marketplace(spark):
    df = _build(spark)
    bad = df.filter(
        (F.col("quantity") < 1)
        | (F.abs(F.col("net_sales_amount") - (F.col("gross_sales_amount") - F.col("discount_amount"))) > 0.01)
        | (F.abs(F.col("gross_margin_amount") - (F.col("net_sales_amount") - F.col("cogs_amount"))) > 0.01)
        | ((F.col("channel_sk") == 4) != F.col("is_marketplace"))
    ).count()
    assert bad == 0
    assert df.filter(F.col("promotion_sk").isNull() & (F.col("discount_amount") > 0)).count() == 0


def test_basket_coherence(spark):
    df = _build(spark)
    incoherent = df.groupBy("transaction_id").agg(
        F.countDistinct("store_sk").alias("s"), F.countDistinct("date_sk").alias("d"),
        F.countDistinct("customer_sk").alias("c"), F.countDistinct("channel_sk").alias("ch"),
        F.count("*").alias("n"), F.max("line_number").alias("mx"), F.min("line_number").alias("mn"),
    ).filter("s > 1 or d > 1 or c > 1 or ch > 1 or n <> mx or mn <> 1").count()
    assert incoherent == 0


def test_deterministic(spark):
    agg = lambda: _build(spark).agg(
        F.round(F.sum("net_sales_amount"), 2).alias("net"), F.sum("quantity").alias("q"),
        F.count(F.when(F.col("promotion_sk").isNull(), 1)).alias("np")).first()
    assert agg() == agg()
```

- [ ] **Step 2: Run (fails), rewrite `fact_sales_line.py` and `lookups.py`, run (passes).** Expected: 5 passed.

- [ ] **Step 3: Update `generate_facts.py` and its test** — `generate_sales_line_local(spark, config, dim_product, dim_date, dim_counts, rows=None)` computes `dim_counts` from the passed Spark dims' `.count()` and calls `build_fact_sales_line`. `main()` reads all dims from UC, builds `dim_counts` from `.count()` of each, generates, `write_table_uc`. Update `tests/test_generate_facts.py` to the Spark dim builders. Run: `python -m pytest tests/test_generate_facts.py -q` → passes.

- [ ] **Step 4: Full suite + commit**

Run: `python -m pytest -q` → all pass.

```bash
git add src/techmart/facts/ src/techmart/jobs/generate_facts.py tests/test_fact_sales_line.py tests/test_lookups.py tests/test_generate_facts.py
git commit -m "feat: rewrite fact_sales_line with basket-coherent transactions and RI from dim counts"
```

---

### Task 8: Generation notebooks

**Files:**
- Create: `notebooks/generate_dims.py`, `notebooks/generate_facts.py`
- Test: `tests/test_notebooks.py` (structural)

**Interfaces:**
- Produces: two Databricks source notebooks (first line `# Databricks notebook source`) that run on serverless, import the `techmart` builders from the DAB-synced `src/`, and write UC Delta tables.

**Notes for the implementer:**
- Notebooks are validated structurally by a test (they contain the required cells) and functionally by the Task 10 deploy-run — they are not unit-tested.
- Each notebook: (1) `%pip install dbldatagen jmespath pyparsing`; (2) `dbutils.widgets` for `catalog`, `schema_prefix`, `scale_profile`, `seed`; (3) add the synced repo `src/` to `sys.path`; (4) `load_config` and run builders; write via `write_table_uc`.

- [ ] **Step 1: Write the structural test** — Create `tests/test_notebooks.py`:

```python
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _read(name):
    return (_ROOT / "notebooks" / name).read_text()


def test_notebooks_are_databricks_sources():
    for name in ["generate_dims.py", "generate_facts.py"]:
        text = _read(name)
        assert text.splitlines()[0] == "# Databricks notebook source"
        assert "%pip install dbldatagen" in text
        assert "dbutils.widgets" in text
        assert "write_table_uc" in text


def test_dims_notebook_covers_all_dims():
    text = _read("generate_dims.py")
    for b in ["build_dim_date", "build_dim_channel", "build_dim_store", "build_dim_vendor",
              "build_dim_promotion", "build_dim_employee", "build_dim_customer", "build_dim_product"]:
        assert b in text
```

- [ ] **Step 2: Run (fails), then create the notebooks.**

`notebooks/generate_dims.py`:

```python
# Databricks notebook source
# MAGIC %pip install dbldatagen jmespath pyparsing
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import sys
# DAB syncs the bundle root; this notebook lives in notebooks/, package in src/.
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")
dbutils.widgets.text("seed", "42")
# COMMAND ----------
from pathlib import Path
from techmart.config import load_config
from techmart.spark.uc_write import write_table_uc
from techmart.spark.dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from techmart.spark.dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
from techmart.spark.dimensions.dim_store import DIM_STORE_SPEC, build_dim_store
from techmart.spark.dimensions.dim_vendor import DIM_VENDOR_SPEC, build_dim_vendor
from techmart.spark.dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion
from techmart.spark.dimensions.dim_employee import DIM_EMPLOYEE_SPEC, build_dim_employee
from techmart.spark.dimensions.dim_customer import DIM_CUSTOMER_SPEC, build_dim_customer
from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
builders = [
    (DIM_DATE_SPEC, build_dim_date), (DIM_CHANNEL_SPEC, build_dim_channel),
    (DIM_STORE_SPEC, build_dim_store), (DIM_VENDOR_SPEC, build_dim_vendor),
    (DIM_PROMOTION_SPEC, build_dim_promotion), (DIM_EMPLOYEE_SPEC, build_dim_employee),
    (DIM_CUSTOMER_SPEC, build_dim_customer), (DIM_PRODUCT_SPEC, build_dim_product),
]
for spec, build in builders:
    target = write_table_uc(spark, build(spark, config), spec, catalog, schema_prefix)
    print("wrote", target, spark.table(target).count())
```

`notebooks/generate_facts.py`:

```python
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
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
dim_product = spark.read.table(f"{core}.dim_product")
dim_date = spark.read.table(f"{core}.dim_date")
dim_counts = {
    "store": spark.table(f"{core}.dim_store").count(),
    "customer": spark.table(f"{core}.dim_customer").count(),
    "employee": spark.table(f"{core}.dim_employee").count(),
    "promotion": spark.table(f"{core}.dim_promotion").count(),
    "product": dim_product.count(),
}
df = build_fact_sales_line(spark, config, dim_product=dim_product, dim_date=dim_date, dim_counts=dim_counts)
target = write_table_uc(spark, df, FACT_SALES_LINE_SPEC, catalog, schema_prefix)
print("wrote", target, spark.table(target).count())
```

- [ ] **Step 3: Run the test (passes), commit**

Run: `python -m pytest tests/test_notebooks.py -q` → 2 passed.

```bash
git add notebooks/ tests/test_notebooks.py
git commit -m "feat: add serverless generation notebooks (dims, facts)"
```

---

### Task 9: DAB serverless notebook jobs + smoke profile

**Files:**
- Modify: `databricks.yml`, `resources/generate_facts_job.yml`, `config/scale_profiles.yaml`, `README.md`, `tests/test_dab_bundle.py`

**Notes for the implementer:**
- Replace the `spark_python_task` with **two `notebook_task`s** in one job: `generate_dims` then `generate_facts` (with `depends_on`). Serverless (no `job_clusters`; an `environments` block or serverless notebook default). Wire widgets via `base_parameters` = `{catalog: ${var.catalog}, schema_prefix: ${var.schema_prefix}, scale_profile: ${var.scale_profile}}`.
- Add a `smoke` profile to `scale_profiles.yaml`.

- [ ] **Step 1: Update the bundle test** — In `tests/test_dab_bundle.py`, keep `test_databricks_yml_structure`; replace the serverless-task assertion with:

```python
def test_generate_facts_job_is_serverless_notebooks():
    import yaml
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    job = yaml.safe_load((root / "resources" / "generate_facts_job.yml").read_text())["resources"]["jobs"]["generate_facts"]
    assert "job_clusters" not in job
    keys = {t["task_key"] for t in job["tasks"]}
    assert {"generate_dims", "generate_facts"} <= keys
    for t in job["tasks"]:
        assert "notebook_task" in t
    facts = next(t for t in job["tasks"] if t["task_key"] == "generate_facts")
    assert any(d["task_key"] == "generate_dims" for d in facts.get("depends_on", []))
```

Add:

```python
def test_smoke_profile_exists():
    import yaml
    from pathlib import Path
    profiles = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "scale_profiles.yaml").read_text())["profiles"]
    assert "smoke" in profiles
    assert profiles["smoke"]["num_stores"] <= 10
```

- [ ] **Step 2: Run (fails), then update the files.**

Add to `config/scale_profiles.yaml`:

```yaml
  smoke:
    num_stores: 5
    num_skus: 500
    history_years: 1
    sales_lines_target: 50000
    num_customers: 1000
    num_vendors: 20
```

Replace `resources/generate_facts_job.yml`:

```yaml
resources:
  jobs:
    generate_facts:
      name: techmart-generate
      tasks:
        - task_key: generate_dims
          notebook_task:
            notebook_path: ../notebooks/generate_dims.py
            base_parameters:
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              scale_profile: ${var.scale_profile}
        - task_key: generate_facts
          depends_on:
            - task_key: generate_dims
          notebook_task:
            notebook_path: ../notebooks/generate_facts.py
            base_parameters:
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              scale_profile: ${var.scale_profile}
```

(`databricks.yml` keeps its `variables` block; ensure `scale_profile` default is `showcase` — the smoke run overrides it at run time.) Update the README deploy section to show `--var="scale_profile=smoke"` and the `databricks bundle run generate_facts` command.

- [ ] **Step 3: Run tests + validate bundle locally**

Run: `python -m pytest tests/test_dab_bundle.py -q` → passes.
Run: `python -m pytest -q` → full suite passes.

- [ ] **Step 4: Commit**

```bash
git add databricks.yml resources/generate_facts_job.yml config/scale_profiles.yaml README.md tests/test_dab_bundle.py
git commit -m "feat: DAB serverless notebook jobs (dims -> facts) + smoke profile"
```

---

### Task 10: Deploy and prove on the workspace (controller-executed)

**This task is executed by the controller directly (needs the live `field-eng-east` workspace), not a subagent.**

- [ ] **Step 1: Validate the bundle** — `databricks bundle validate -p field-eng-east` → "Validation OK!".
- [ ] **Step 2: Deploy at smoke scale** — `databricks bundle deploy -p field-eng-east --var="scale_profile=smoke"`.
- [ ] **Step 3: Run the job** — `databricks bundle run generate_facts -p field-eng-east` and wait for success.
- [ ] **Step 4: Verify in UC** — query `stable_classic_ppke9o.techmart_core`: all 8 dims + `fact_sales_line` exist and are non-empty; spot-check `SELECT count(*), count(distinct transaction_id) FROM ...fact_sales_line` (baskets), a receipt roll-up, and that column comments are present (`DESCRIBE`). Record counts.
- [ ] **Step 5: Fix forward if the serverless run surfaces issues** (e.g., a notebook `sys.path`/`%pip` detail, a dbldatagen-on-serverless nuance). Any code change re-runs the covering local test + re-deploys. Commit fixes.
- [ ] **Step 6: Commit** any deploy-driven adjustments with a clear message; record the proven counts in the branch ledger.

---

### Task 11: Remove the superseded Polars generation stack

**Only after Task 10 proves the Spark path green.**

**Files (delete):** `src/techmart/dimensions/` (all), `src/techmart/framework/` (all), `src/techmart/registry.py`, `src/techmart/cli.py`, `src/techmart/rng.py`, and their tests: `tests/test_dim_*.py` (Polars ones), `tests/test_dimension_support.py`, `tests/test_product_support.py`, `tests/test_taxonomy.py` (only if it imports the removed package — keep taxonomy tests if they test `reference/taxonomy.py`), `tests/test_registry.py`, `tests/test_cli.py`, `tests/test_rng.py`, `tests/test_scd2.py`, `tests/test_writer.py`, `tests/test_package.py` (adjust if it asserts removed modules).

**Notes:** `reference/taxonomy.py` and `reference/pools.py` stay. `config.py` stays. Verify nothing under `src/techmart/spark/` or `src/techmart/facts/` imports the removed modules before deleting.

- [ ] **Step 1: Confirm no live imports** — `grep -rn "techmart.dimensions\|techmart.framework\|techmart.registry\|techmart.cli\|techmart.rng\|from ..rng\|from .rng" src/techmart/spark src/techmart/facts` returns nothing (fix any stragglers first).
- [ ] **Step 2: Delete the superseded modules and their tests** (list above).
- [ ] **Step 3: Update `pyproject.toml`** if it references removed entry points; ensure `polars`/`numpy` remain only if still used (they are not in the generation path — move them out of core deps if nothing else needs them; keep if `reference/` uses numpy).
- [ ] **Step 4: Full suite** — `python -m pytest -q` → all remaining (Spark) tests pass; no import errors.
- [ ] **Step 5: Commit**

```bash
git commit -am "refactor: remove superseded Polars generation stack (dims/framework/cli/registry)"
```

---

## Self-Review

- **Spec coverage:** serverless-native dbldatagen/PySpark generation for all 8 dims + fact (Tasks 2–7); notebooks on serverless (Task 8); DAB notebook jobs (Task 9); UC Delta with comments (Task 1 `uc_write` + `select_ordered`); basket coherence (Task 7); RI by construction from dim counts (Task 7); proven by a live smoke deploy (Task 10); Polars stack removed (Task 11). Deferred (later phases, per spec): the other six facts, finance/AI/ops, semantic metric views, anomalies, reconciliation deltas, LLM text.
- **Placeholder scan:** builders in Tasks 3–6 intentionally say "port every column from the retired Polars `dim_*.py`" — the exact column list/comments live in those files (present until Task 11) and each task's `validate_spark_schema` + assertions enforce fidelity; the framework/helpers/date/channel/fact/notebook/DAB code is complete inline.
- **Type consistency:** `build_x(spark, config)` uniform across dims; `build_fact_sales_line(spark, config, *, dim_product, dim_date, dim_counts, rows, seed)` matches its callers (test, `generate_sales_line_local`, `main`, notebook); `write_table_uc(spark, df, spec, catalog, schema_prefix)` matches every call site; SCD2 timestamps are Spark `timestamp` in both `scd2_columns()` and `with_scd2_current`.

## Next plan

**Phase 4 — remaining core facts** in this proven serverless-notebook model: `fact_inventory_snapshot` (store×SKU×day — the big perf-tuning target), `fact_inventory_movement`, `fact_fulfillment`, `fact_returns` (begins the gross↔net reconciliation), `fact_web_events`, `fact_loyalty_activity`. Reuse the Spark framework, `write_table_uc`, the notebook/DAB pattern, and RI-from-dim-counts. Then finance/AI/ops schemas and the semantic metric views.

**Carry-forward (older cosmetic minors now folded into the Spark rewrite or still open):** `dim_product.spec_attributes` JSON via `to_json(struct(...))` and `discontinue_date <= end_date` clamp are **done** in Task 6; `manufacturer` still equals `brand_name` (differentiate later); `dim_store.region_id` is zero-padded-or-not per the port (revisit if desired); a shared `write_fact`/`write_dim` helper is now `write_table_uc` (done).
