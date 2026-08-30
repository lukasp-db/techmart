# Techmart Phase 3 — Sales Fact on Spark/dbldatagen + DAB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the Spark/`dbldatagen` generation path and a Databricks Asset Bundle (DAB), and build `fact_sales_line` — the central sales fact — as the first fact table, generated deterministically and joined to conformed dimensions.

**Architecture:** A new `techmart.spark` package provides a Spark session helper (returns the platform session on serverless, builds a guarded `local[*]` session for tests) and a Spark-flavored `FactSpec` schema/validation framework paralleling the existing Polars `TableSpec`. A new `techmart.facts` package holds fact specs and `dbldatagen`-based generators; `fact_sales_line` draws referential-integrity-safe foreign keys over the conformed-dimension surrogate-key ranges (`1..N`), weights `date_sk` by seasonality derived from `dim_date`, joins a product-economics lookup derived from `dim_product` for realistic price/cost, and derives the gross→net→margin measure chain. A `databricks.yml` DAB defines a serverless job (`techmart.jobs.generate_facts`) that reads the merged Delta dimensions from Unity Catalog, generates the fact, and writes it back. Generation logic is TDD'd locally against PySpark; the DAB deploy + serverless run happen against the field-eng workspace.

**Tech Stack:** Python 3.10+, PySpark 3.5 (matches serverless DBR), `dbldatagen` 0.4, Polars/NumPy (existing dims), Databricks Asset Bundles, pytest.

## Global Constraints

*(Every task's requirements implicitly include this section. Values copied verbatim from the design spec.)*

- **Serverless-only:** all generation and deployment code must run on Databricks serverless — no dependency on classic clusters or instance-specific runtime features. The Spark session helper must therefore reuse the platform-provided session when one exists and never hard-code cluster config.
- **DAB deployment:** everything ships via a Databricks Asset Bundle (`databricks.yml`) for one-command deploy; the bundle is parameterized by `catalog`, `schema_prefix`, and `scale_profile` variables — no workspace URLs, tokens, or account ids committed (public, secret-free repo; `.gitignore` covers env/secrets).
- **Single catalog, `techmart_*` schemas:** core facts and dims live in the `<catalog>.<schema_prefix>core` schema (e.g. `techmart.techmart_core`).
- **Keys:** surrogate keys are `*_sk` BIGINT (Spark `long`), sequential `1..N` in every dimension; business/degenerate keys (`*_id`, `transaction_id`) preserved. Referential integrity holds across all FKs — every fact FK value is a valid surrogate key in its dimension.
- **Comments:** every fact column carries a `comment` (fuels Genie), just as dims do.
- **Deterministic & re-runnable:** generation is reproducible run-to-run from the base seed (`dbldatagen` `randomSeed=<seed>, randomSeedMethod="fixed"`); jobs are idempotent (overwrite semantics).
- **Scale via profiles:** row volumes derive from `config.scale_profile` (`demo_lean` / `showcase` (default) / `stress`); generators accept a `rows` override so tests run at tiny scale.
- **Money as `double`:** monetary measures use Spark `double` (rounded to 2 dp), consistent with the `Float64` price/cost columns in `dim_product`.

---

## File Structure

- `pyproject.toml` — add a `spark` optional-dependency group.
- `.gitignore` — add local-Spark artifacts and packaging output.
- `tests/conftest.py` (create) — session-scoped `spark` pytest fixture.
- `src/techmart/spark/__init__.py` (create)
- `src/techmart/spark/session.py` (create) — `get_spark()` session helper.
- `src/techmart/spark/framework.py` (create) — `FactColumn`, `FactSpec`, `validate_fact_schema`.
- `src/techmart/facts/__init__.py` (create)
- `src/techmart/facts/lookups.py` (create) — `polars_to_spark`, `product_economics`, `date_seasonality_weights`.
- `src/techmart/facts/fact_sales_line.py` (create) — `FACT_SALES_LINE_SPEC`, `build_fact_sales_line`.
- `src/techmart/facts/registry.py` (create) — `FACT_SPECS`.
- `src/techmart/jobs/__init__.py` (create)
- `src/techmart/jobs/generate_facts.py` (create) — local assembly helper + serverless `main()`.
- `databricks.yml` (create) — DAB bundle definition.
- `resources/generate_facts_job.yml` (create) — DAB job resource.
- `README.md` (modify) — add a "Deploy to Databricks (DAB)" section.
- `tests/test_spark_session.py`, `tests/test_fact_framework.py`, `tests/test_lookups.py`, `tests/test_fact_sales_line.py`, `tests/test_generate_facts.py`, `tests/test_dab_bundle.py` (create)

---

### Task 1: Spark dependencies, session helper, and test fixture

**Files:**
- Modify: `pyproject.toml`
- Modify: `.gitignore`
- Create: `src/techmart/spark/__init__.py`
- Create: `src/techmart/spark/session.py`
- Create: `tests/conftest.py`
- Test: `tests/test_spark_session.py`

**Interfaces:**
- Produces: `techmart.spark.session.get_spark(app_name: str = "techmart", *, local_partitions: int = 4) -> pyspark.sql.SparkSession`. Returns the active session if one exists (serverless), else builds a guarded `local[*]` session. A session-scoped pytest fixture named `spark` yields `get_spark("techmart-tests")`.

- [ ] **Step 1: Add the `spark` optional-dependency group**

In `pyproject.toml`, under `[project.optional-dependencies]`, add the `spark` group (keep the existing `dev` line):

```toml
[project.optional-dependencies]
dev = ["pytest>=8.0.0"]
spark = [
    "pyspark>=3.5,<3.6",
    "dbldatagen>=0.4",
    "pyparsing>=3.0",
    "jmespath>=1.0",
    "pandas>=2.0",
]
```

- [ ] **Step 2: Ignore local-Spark and packaging artifacts**

In `.gitignore`, under the `# Data artifacts` block (after `*.delta`), add:

```gitignore

# Local Spark / packaging artifacts
spark-warehouse/
metastore_db/
derby.log
*.egg-info/
```

- [ ] **Step 3: Create the spark package init**

Create `src/techmart/spark/__init__.py`:

```python
```

(Empty file — package marker.)

- [ ] **Step 4: Write the failing session test**

Create `tests/test_spark_session.py`:

```python
from techmart.spark.session import get_spark


def test_get_spark_returns_session(spark):
    assert spark.version.startswith("3.")


def test_get_spark_is_idempotent(spark):
    again = get_spark("techmart-tests")
    assert again is spark
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `python -m pytest tests/test_spark_session.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.spark.session'` (and the `spark` fixture does not yet exist).

- [ ] **Step 6: Implement the session helper**

Create `src/techmart/spark/session.py`:

```python
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "techmart", *, local_partitions: int = 4) -> SparkSession:
    """Return a SparkSession suitable for both serverless and local use.

    On Databricks serverless a session already exists and is reused unchanged.
    Locally (tests / laptop) a small ``local[*]`` session is built. A stale
    ``SPARK_HOME`` pointing at a removed distribution would shadow pyspark's
    bundled runtime, so it is dropped before building.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    home = os.environ.get("SPARK_HOME")
    if home and not os.path.isdir(home):
        os.environ.pop("SPARK_HOME", None)

    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(local_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
```

- [ ] **Step 7: Create the pytest fixture**

Create `tests/conftest.py`:

```python
import pytest

from techmart.spark.session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("techmart-tests")
    yield session
    session.stop()
```

- [ ] **Step 8: Run the test to verify it passes**

Run: `python -m pytest tests/test_spark_session.py -q`
Expected: PASS (2 passed). Spark logs/warnings on stderr are fine.

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml .gitignore src/techmart/spark/ tests/conftest.py tests/test_spark_session.py
git commit -m "feat: add Spark session helper and pytest fixture"
```

---

### Task 2: Spark fact-schema framework

**Files:**
- Create: `src/techmart/spark/framework.py`
- Test: `tests/test_fact_framework.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `FactColumn(name: str, dtype: str, comment: str, is_key: bool = False, nullable: bool = True)` — `dtype` is one of `"long"`, `"int"`, `"double"`, `"string"`, `"boolean"`.
  - `FactSpec(schema: str, name: str, grain: str, columns: list[FactColumn])` with `.column_names -> list[str]` and `.struct_type() -> pyspark.sql.types.StructType`.
  - `validate_fact_schema(df, spec) -> None` — raises `FactSchemaMismatchError` if `df.columns` (as a set) or any field type differs from the spec.

- [ ] **Step 1: Write the failing framework test**

Create `tests/test_fact_framework.py`:

```python
import pytest
from pyspark.sql.types import LongType, StringType, StructType

from techmart.spark.framework import (
    FactColumn,
    FactSchemaMismatchError,
    FactSpec,
    validate_fact_schema,
)

SPEC = FactSpec(
    schema="core",
    name="fact_demo",
    grain="one row per demo event",
    columns=[
        FactColumn("id_sk", "long", "Surrogate key", is_key=True, nullable=False),
        FactColumn("label", "string", "A label"),
    ],
)


def test_column_names():
    assert SPEC.column_names == ["id_sk", "label"]


def test_struct_type_maps_dtypes():
    st = SPEC.struct_type()
    assert isinstance(st, StructType)
    assert [f.name for f in st.fields] == ["id_sk", "label"]
    assert isinstance(st["id_sk"].dataType, LongType)
    assert isinstance(st["label"].dataType, StringType)
    assert st["id_sk"].nullable is False
    assert st["label"].nullable is True


def test_validate_accepts_matching_dataframe(spark):
    df = spark.createDataFrame([(1, "a")], SPEC.struct_type())
    validate_fact_schema(df, SPEC)  # no raise


def test_validate_rejects_missing_column(spark):
    df = spark.createDataFrame([(1,)], StructType([SPEC.struct_type()["id_sk"]]))
    with pytest.raises(FactSchemaMismatchError):
        validate_fact_schema(df, SPEC)


def test_validate_rejects_wrong_type(spark):
    # label is string in the spec; supply long instead.
    df = spark.createDataFrame([(1, 2)], "id_sk long, label long")
    with pytest.raises(FactSchemaMismatchError):
        validate_fact_schema(df, SPEC)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_fact_framework.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.spark.framework'`.

- [ ] **Step 3: Implement the framework**

Create `src/techmart/spark/framework.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

_SPARK_TYPES: dict[str, DataType] = {
    "long": LongType(),
    "int": IntegerType(),
    "double": DoubleType(),
    "string": StringType(),
    "boolean": BooleanType(),
}


class FactSchemaMismatchError(ValueError):
    """Raised when a DataFrame's columns/types do not match its FactSpec."""


@dataclass(frozen=True)
class FactColumn:
    name: str
    dtype: str  # one of _SPARK_TYPES
    comment: str
    is_key: bool = False
    nullable: bool = True


@dataclass(frozen=True)
class FactSpec:
    schema: str  # target schema group, e.g. "core"
    name: str  # table name, e.g. "fact_sales_line"
    grain: str  # one-line description of the row grain
    columns: list[FactColumn]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def struct_type(self) -> StructType:
        return StructType(
            [
                StructField(c.name, _SPARK_TYPES[c.dtype], c.nullable)
                for c in self.columns
            ]
        )


def validate_fact_schema(df: DataFrame, spec: FactSpec) -> None:
    expected = {c.name: _SPARK_TYPES[c.dtype] for c in spec.columns}
    actual = dict(df.dtypes)  # name -> simpleString type

    missing = [n for n in expected if n not in actual]
    extra = [n for n in actual if n not in expected]
    if missing or extra:
        raise FactSchemaMismatchError(
            f"{spec.name}: column mismatch (missing={missing}, extra={extra})"
        )

    mismatches = [
        (name, actual[name], dtype.simpleString())
        for name, dtype in expected.items()
        if actual[name] != dtype.simpleString()
    ]
    if mismatches:
        raise FactSchemaMismatchError(
            f"{spec.name}: dtype mismatch (name, actual, expected): {mismatches}"
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_fact_framework.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/spark/framework.py tests/test_fact_framework.py
git commit -m "feat: add Spark fact-schema framework (FactSpec + validation)"
```

---

### Task 3: Dimension lookups for fact generation

**Files:**
- Create: `src/techmart/facts/__init__.py`
- Create: `src/techmart/facts/lookups.py`
- Test: `tests/test_lookups.py`

**Interfaces:**
- Consumes: `techmart.spark.session` (via the `spark` fixture in tests). Reads Polars dims produced by the existing `techmart.dimensions.*` builders.
- Produces:
  - `polars_to_spark(spark, pl_df) -> DataFrame` — converts a Polars DataFrame to Spark via pandas.
  - `product_economics(spark, dim_product_pl) -> DataFrame` — Spark DF with columns `product_sk` (long), `list_price` (double), `standard_cost` (double), `msrp` (double), one row per SKU.
  - `date_seasonality_weights(dim_date) -> tuple[list[int], list[int]]` — takes a **Spark** `dim_date` DataFrame (columns `date_sk`, `is_weekend`, `selling_season`, `holiday_name`, `year`), returns `(date_sks, weights)` parallel lists of ints, ordered by `date_sk`, every weight `>= 1`.

- [ ] **Step 1: Write the failing lookups test**

Create `tests/test_lookups.py`:

```python
from datetime import date

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_date import build_dim_date
from techmart.dimensions.dim_product import build_dim_product
from techmart.facts.lookups import (
    date_seasonality_weights,
    polars_to_spark,
    product_economics,
)

_PROFILE = ScaleProfile(
    name="test",
    num_stores=10,
    num_skus=40,
    history_years=1,
    sales_lines_target=2000,
    num_customers=200,
    num_vendors=20,
)
_CONFIG = TechmartConfig(
    scale_profile=_PROFILE,
    seed=42,
    output_dir=__import__("pathlib").Path("data"),
    catalog="techmart",
    schema_prefix="techmart_",
    end_date=date(2026, 1, 31),
)


def test_product_economics_one_row_per_sku(spark):
    dim = build_dim_product(_CONFIG)
    econ = product_economics(spark, dim)
    assert econ.count() == _PROFILE.num_skus
    assert set(econ.columns) == {"product_sk", "list_price", "standard_cost", "msrp"}
    from pyspark.sql import functions as F

    row = econ.agg(
        F.min("product_sk").alias("lo"),
        F.max("product_sk").alias("hi"),
        F.min("list_price").alias("minprice"),
    ).collect()[0]
    assert row["lo"] == 1 and row["hi"] == _PROFILE.num_skus
    assert row["minprice"] > 0


def test_date_weights_cover_calendar_and_are_positive(spark):
    dim = build_dim_date(_CONFIG.start_date, _CONFIG.end_date)
    dim_spark = polars_to_spark(
        spark,
        dim.select(["date_sk", "is_weekend", "selling_season", "holiday_name", "year"]),
    )
    date_sks, weights = date_seasonality_weights(dim_spark)
    assert len(date_sks) == dim.height
    assert len(weights) == dim.height
    assert min(weights) >= 1
    assert date_sks == sorted(date_sks)
    # Holiday-season days should on average outweigh a flat baseline of 100.
    assert max(weights) > 100
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_lookups.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.facts'`.

- [ ] **Step 3: Create the facts package init**

Create `src/techmart/facts/__init__.py`:

```python
```

(Empty file — package marker.)

- [ ] **Step 4: Implement the lookups**

Create `src/techmart/facts/lookups.py`:

```python
from __future__ import annotations

import polars as pl
from pyspark.sql import DataFrame, SparkSession, functions as F


def polars_to_spark(spark: SparkSession, pl_df: pl.DataFrame) -> DataFrame:
    """Convert a Polars DataFrame to a Spark DataFrame (via pandas)."""
    return spark.createDataFrame(pl_df.to_pandas())


def product_economics(spark: SparkSession, dim_product_pl: pl.DataFrame) -> DataFrame:
    """Per-SKU price/cost lookup for deriving realistic fact measures."""
    econ = dim_product_pl.select(["product_sk", "list_price", "standard_cost", "msrp"])
    return polars_to_spark(spark, econ)


def date_seasonality_weights(dim_date: DataFrame) -> tuple[list[int], list[int]]:
    """Integer sampling weights per ``date_sk`` from seasonality signals.

    Baseline 100, lifted by weekends, the Holiday and Back-to-School selling
    seasons, Black Friday, and a mild year-over-year growth trend. Returned as
    two parallel lists (ordered by ``date_sk``) suitable for dbldatagen
    ``values=`` / ``weights=``.
    """
    min_year = dim_date.agg(F.min("year")).collect()[0][0]
    weighted = (
        dim_date.select(
            "date_sk",
            (
                F.lit(100.0)
                * F.when(F.col("is_weekend"), 1.5).otherwise(1.0)
                * F.when(F.col("selling_season") == "Holiday", 2.5)
                .when(F.col("selling_season") == "Back-to-School", 1.8)
                .otherwise(1.0)
                * F.when(F.col("holiday_name") == "Black Friday", 3.0).otherwise(1.0)
                * (1.0 + 0.08 * (F.col("year") - F.lit(min_year)))
            ).alias("w"),
        )
        .withColumn("w", F.greatest(F.round("w").cast("int"), F.lit(1)))
        .orderBy("date_sk")
        .collect()
    )
    date_sks = [r["date_sk"] for r in weighted]
    weights = [r["w"] for r in weighted]
    return date_sks, weights
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `python -m pytest tests/test_lookups.py -q`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add src/techmart/facts/__init__.py src/techmart/facts/lookups.py tests/test_lookups.py
git commit -m "feat: add dimension lookups (product economics, seasonality weights)"
```

---

### Task 4: `fact_sales_line` spec and generator

**Files:**
- Create: `src/techmart/facts/fact_sales_line.py`
- Test: `tests/test_fact_sales_line.py`

**Interfaces:**
- Consumes: `techmart.spark.framework.FactColumn/FactSpec`, `techmart.config.TechmartConfig`, and lookups from Task 3 (`product_econ` Spark DF, `date_weights` tuple).
- Produces:
  - `FACT_SALES_LINE_SPEC: FactSpec` (schema `"core"`, name `"fact_sales_line"`).
  - `build_fact_sales_line(spark, config, *, product_econ, date_weights, rows=None, seed=None, promo_fraction=0.22) -> DataFrame` — returns a DataFrame whose columns equal `FACT_SALES_LINE_SPEC.column_names`, with all FKs in valid dimension ranges and the derived measure chain.

**Notes for the implementer:**
- Channel weights map to `channel_sk` `1..5` in dim_channel order (In-Store, Web, Mobile-App, Marketplace, Call-Center). `is_marketplace` is true iff `channel_sk == 4`.
- `product_sk` uses a right-skewed `Gamma(1.0, 2.0)` distribution for long-tail SKU popularity; all other surrogate FKs are uniform over `1..N`.
- If the determinism test ever fails, confirm `randomSeedMethod="fixed"` is set on the `DataGenerator` (it is, below).

- [ ] **Step 1: Write the failing generator test**

Create `tests/test_fact_sales_line.py`:

```python
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_date import build_dim_date
from techmart.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line
from techmart.facts.lookups import (
    date_seasonality_weights,
    polars_to_spark,
    product_economics,
)

_PROFILE = ScaleProfile(
    name="test",
    num_stores=10,
    num_skus=40,
    history_years=1,
    sales_lines_target=3000,
    num_customers=200,
    num_vendors=20,
)
_CONFIG = TechmartConfig(
    scale_profile=_PROFILE,
    seed=42,
    output_dir=Path("data"),
    catalog="techmart",
    schema_prefix="techmart_",
    end_date=date(2026, 1, 31),
)


def _lookups(spark):
    dim_product = build_dim_product(_CONFIG)
    dim_date = build_dim_date(_CONFIG.start_date, _CONFIG.end_date)
    econ = product_economics(spark, dim_product)
    dd = polars_to_spark(
        spark,
        dim_date.select(
            ["date_sk", "is_weekend", "selling_season", "holiday_name", "year"]
        ),
    )
    return econ, date_seasonality_weights(dd)


def test_schema_and_rowcount(spark):
    econ, dw = _lookups(spark)
    df = build_fact_sales_line(spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000)
    assert df.columns == FACT_SALES_LINE_SPEC.column_names
    assert df.count() == 3000


def test_referential_integrity(spark):
    econ, dw = _lookups(spark)
    df = build_fact_sales_line(spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000)
    r = df.agg(
        F.min("product_sk").alias("p_lo"),
        F.max("product_sk").alias("p_hi"),
        F.min("store_sk").alias("s_lo"),
        F.max("store_sk").alias("s_hi"),
        F.max("channel_sk").alias("c_hi"),
        F.count(F.when(F.col("unit_price").isNull(), 1)).alias("null_price"),
    ).collect()[0]
    assert r["p_lo"] >= 1 and r["p_hi"] <= _PROFILE.num_skus
    assert r["s_lo"] >= 1 and r["s_hi"] <= _PROFILE.num_stores
    assert r["c_hi"] <= 5
    assert r["null_price"] == 0  # every product_sk joined to economics


def test_measure_invariants(spark):
    econ, dw = _lookups(spark)
    df = build_fact_sales_line(spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000)
    bad = df.filter(
        (F.col("quantity") < 1)
        | (F.abs(F.col("net_sales_amount") - (F.col("gross_sales_amount") - F.col("discount_amount"))) > 0.01)
        | (F.abs(F.col("gross_margin_amount") - (F.col("net_sales_amount") - F.col("cogs_amount"))) > 0.01)
        | (F.col("discount_amount") < 0)
    ).count()
    assert bad == 0
    # Discount only when a promotion is present.
    assert df.filter(
        (F.col("promotion_sk").isNull()) & (F.col("discount_amount") > 0)
    ).count() == 0
    # is_marketplace iff channel_sk == 4.
    assert df.filter(
        (F.col("channel_sk") == 4) != F.col("is_marketplace")
    ).count() == 0


def test_deterministic(spark):
    econ, dw = _lookups(spark)
    agg = lambda: build_fact_sales_line(
        spark, _CONFIG, product_econ=econ, date_weights=dw, rows=3000
    ).agg(
        F.round(F.sum("net_sales_amount"), 2).alias("net"),
        F.sum("quantity").alias("qty"),
        F.count(F.when(F.col("promotion_sk").isNull(), 1)).alias("no_promo"),
    ).collect()[0]
    assert agg() == agg()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_fact_sales_line.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.facts.fact_sales_line'`.

- [ ] **Step 3: Implement the spec and generator**

Create `src/techmart/facts/fact_sales_line.py`:

```python
from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import FactColumn, FactSpec

FACT_SALES_LINE_SPEC = FactSpec(
    schema="core",
    name="fact_sales_line",
    grain="one row per sales transaction line",
    columns=[
        FactColumn("transaction_id", "long", "Degenerate transaction id", nullable=False),
        FactColumn("line_number", "int", "Line number within the transaction", nullable=False),
        FactColumn("receipt_id", "string", "Degenerate receipt id"),
        FactColumn("date_sk", "long", "Date FK (dim_date, yyyymmdd)", is_key=True, nullable=False),
        FactColumn("product_sk", "long", "Product FK (dim_product)", is_key=True, nullable=False),
        FactColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        FactColumn("customer_sk", "long", "Customer FK (dim_customer)", is_key=True, nullable=False),
        FactColumn("employee_sk", "long", "Selling associate FK (dim_employee)", is_key=True, nullable=False),
        FactColumn("promotion_sk", "long", "Promotion FK (dim_promotion); null if unpromoted", is_key=True),
        FactColumn("channel_sk", "long", "Channel FK (dim_channel)", is_key=True, nullable=False),
        FactColumn("quantity", "int", "Units sold on the line", nullable=False),
        FactColumn("unit_price", "double", "Selling price per unit"),
        FactColumn("unit_cost", "double", "Standard cost per unit"),
        FactColumn("gross_sales_amount", "double", "quantity * unit_price"),
        FactColumn("discount_amount", "double", "Promotional discount applied"),
        FactColumn("net_sales_amount", "double", "gross_sales_amount - discount_amount"),
        FactColumn("tax_amount", "double", "Sales tax on net sales"),
        FactColumn("cogs_amount", "double", "quantity * unit_cost"),
        FactColumn("gross_margin_amount", "double", "net_sales_amount - cogs_amount"),
        FactColumn("loyalty_points_earned", "long", "Loyalty points earned (floor of net sales)"),
        FactColumn("is_return", "boolean", "Always false in the sales fact", nullable=False),
        FactColumn("is_marketplace", "boolean", "Sold via the marketplace channel", nullable=False),
        FactColumn("tender_type", "string", "Payment tender type"),
    ],
)

# dim_channel surrogate order: In-Store, Web, Mobile-App, Marketplace, Call-Center.
_CHANNEL_SKS = [1, 2, 3, 4, 5]
_CHANNEL_WEIGHTS = [50, 28, 15, 5, 2]
_TENDERS = ["Card", "Card", "Card", "Cash", "Gift Card", "Mobile Pay"]

_DISCOUNT_RATE = 0.12
_TAX_RATE = 0.07


def build_fact_sales_line(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    product_econ: DataFrame,
    date_weights: tuple[list[int], list[int]],
    rows: int | None = None,
    seed: int | None = None,
    promo_fraction: float = 0.22,
) -> DataFrame:
    sp = config.scale_profile
    rows = rows if rows is not None else sp.sales_lines_target
    seed = seed if seed is not None else config.seed
    date_sks, weights = date_weights
    partitions = max(1, min(256, rows // 1_000_000))

    gen = (
        dg.DataGenerator(
            spark,
            name="fact_sales_line",
            rows=rows,
            partitions=partitions,
            randomSeed=seed,
            randomSeedMethod="fixed",
        )
        .withColumn("transaction_id", "long", minValue=1, maxValue=max(rows // 3, 1), random=True)
        .withColumn("line_number", "int", minValue=1, maxValue=8, random=True)
        .withColumn("date_sk", "long", values=date_sks, weights=weights, random=True)
        .withColumn(
            "product_sk", "long",
            minValue=1, maxValue=sp.num_skus,
            distribution=dg.distributions.Gamma(1.0, 2.0), random=True,
        )
        .withColumn("store_sk", "long", minValue=1, maxValue=sp.num_stores, random=True)
        .withColumn("customer_sk", "long", minValue=1, maxValue=sp.num_customers, random=True)
        .withColumn("employee_sk", "long", minValue=1, maxValue=sp.num_employees, random=True)
        .withColumn("channel_sk", "long", values=_CHANNEL_SKS, weights=_CHANNEL_WEIGHTS, random=True)
        .withColumn(
            "promotion_sk", "long",
            minValue=1, maxValue=sp.num_promotions,
            random=True, percentNulls=1.0 - promo_fraction,
        )
        .withColumn("quantity", "int", minValue=1, maxValue=5, random=True)
        .withColumn("tender_type", "string", values=_TENDERS, random=True)
    )
    base = gen.build()

    econ = product_econ.select(
        F.col("product_sk").alias("_econ_sk"),
        F.col("list_price"),
        F.col("standard_cost"),
    )
    joined = base.join(econ, base["product_sk"] == econ["_econ_sk"], "left").drop("_econ_sk")

    df = (
        joined
        .withColumn("unit_price", F.round(F.col("list_price"), 2))
        .withColumn("unit_cost", F.round(F.col("standard_cost"), 2))
        .withColumn("receipt_id", F.concat(F.lit("RCPT-"), F.col("transaction_id").cast("string")))
        .withColumn("gross_sales_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn(
            "discount_amount",
            F.when(
                F.col("promotion_sk").isNotNull(),
                F.round(F.col("gross_sales_amount") * F.lit(_DISCOUNT_RATE), 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("net_sales_amount", F.round(F.col("gross_sales_amount") - F.col("discount_amount"), 2))
        .withColumn("tax_amount", F.round(F.col("net_sales_amount") * F.lit(_TAX_RATE), 2))
        .withColumn("cogs_amount", F.round(F.col("quantity") * F.col("unit_cost"), 2))
        .withColumn("gross_margin_amount", F.round(F.col("net_sales_amount") - F.col("cogs_amount"), 2))
        .withColumn("loyalty_points_earned", F.floor(F.col("net_sales_amount")).cast("long"))
        .withColumn("is_return", F.lit(False))
        .withColumn("is_marketplace", F.col("channel_sk") == F.lit(4))
    )
    return df.select(FACT_SALES_LINE_SPEC.column_names)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_fact_sales_line.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/facts/fact_sales_line.py tests/test_fact_sales_line.py
git commit -m "feat: add fact_sales_line generator (dbldatagen, RI + measure chain)"
```

---

### Task 5: Fact registry and generation job assembly

**Files:**
- Create: `src/techmart/facts/registry.py`
- Create: `src/techmart/jobs/__init__.py`
- Create: `src/techmart/jobs/generate_facts.py`
- Test: `tests/test_generate_facts.py`

**Interfaces:**
- Consumes: Task 4's `FACT_SALES_LINE_SPEC` / `build_fact_sales_line`, Task 3's lookups, Task 2's `validate_fact_schema`, existing dim builders.
- Produces:
  - `techmart.facts.registry.FACT_SPECS: dict[str, FactSpec]`.
  - `generate_sales_line_local(spark, config, dim_product_pl, dim_date_pl, *, rows=None) -> DataFrame` — assembles lookups from Polars dims and returns the validated fact DataFrame (used by tests and the local CLI path).
  - `main(argv=None) -> int` — serverless DAB entrypoint: reads dims from Unity Catalog, generates `fact_sales_line`, writes it back with overwrite semantics.

- [ ] **Step 1: Write the failing assembly test**

Create `tests/test_generate_facts.py`:

```python
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_date import build_dim_date
from techmart.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC
from techmart.facts.registry import FACT_SPECS
from techmart.jobs.generate_facts import generate_sales_line_local

_PROFILE = ScaleProfile(
    name="test", num_stores=10, num_skus=40, history_years=1,
    sales_lines_target=2500, num_customers=200, num_vendors=20,
)
_CONFIG = TechmartConfig(
    scale_profile=_PROFILE, seed=42, output_dir=Path("data"),
    catalog="techmart", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_registry_contains_sales_line():
    assert FACT_SPECS["fact_sales_line"] is FACT_SALES_LINE_SPEC


def test_generate_sales_line_local_end_to_end(spark):
    dim_product = build_dim_product(_CONFIG)
    dim_date = build_dim_date(_CONFIG.start_date, _CONFIG.end_date)
    df = generate_sales_line_local(spark, _CONFIG, dim_product, dim_date, rows=2500)
    assert df.columns == FACT_SALES_LINE_SPEC.column_names
    assert df.count() == 2500
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_generate_facts.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.facts.registry'`.

- [ ] **Step 3: Implement the registry**

Create `src/techmart/facts/registry.py`:

```python
from __future__ import annotations

from ..spark.framework import FactSpec
from .fact_sales_line import FACT_SALES_LINE_SPEC

FACT_SPECS: dict[str, FactSpec] = {
    FACT_SALES_LINE_SPEC.name: FACT_SALES_LINE_SPEC,
}
```

- [ ] **Step 4: Create the jobs package init**

Create `src/techmart/jobs/__init__.py`:

```python
```

(Empty file — package marker.)

- [ ] **Step 5: Implement the job assembly and entrypoint**

Create `src/techmart/jobs/generate_facts.py`:

```python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl
from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig, load_config
from ..dimensions.dim_date import build_dim_date
from ..dimensions.dim_product import build_dim_product
from ..facts.fact_sales_line import FACT_SALES_LINE_SPEC, build_fact_sales_line
from ..facts.lookups import date_seasonality_weights, polars_to_spark, product_economics
from ..spark.framework import validate_fact_schema
from ..spark.session import get_spark

_DATE_WEIGHT_COLS = ["date_sk", "is_weekend", "selling_season", "holiday_name", "year"]


def generate_sales_line_local(
    spark: SparkSession,
    config: TechmartConfig,
    dim_product_pl: pl.DataFrame,
    dim_date_pl: pl.DataFrame,
    *,
    rows: int | None = None,
) -> DataFrame:
    """Assemble lookups from in-memory Polars dims and build the sales fact.

    Used by tests and local runs. On serverless, ``main`` reads the dims from
    Unity Catalog instead (see below).
    """
    econ = product_economics(spark, dim_product_pl)
    dd = polars_to_spark(spark, dim_date_pl.select(_DATE_WEIGHT_COLS))
    weights = date_seasonality_weights(dd)
    df = build_fact_sales_line(spark, config, product_econ=econ, date_weights=weights, rows=rows)
    validate_fact_schema(df, FACT_SALES_LINE_SPEC)
    return df


def main(argv: list[str] | None = None) -> int:
    """Serverless DAB entrypoint: read dims from UC, write fact_sales_line to UC."""
    parser = argparse.ArgumentParser(description="Generate Techmart fact_sales_line.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", default="techmart_")
    parser.add_argument("--profile", default=None, help="Scale profile name.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--profiles-path", default="config/scale_profiles.yaml")
    args = parser.parse_args(argv)

    config = load_config(
        Path(args.profiles_path), args.profile,
        seed=args.seed, catalog=args.catalog, schema_prefix=args.schema_prefix,
    )
    spark = get_spark("techmart-generate-facts")
    core = f"{args.catalog}.{args.schema_prefix}core"

    dim_product = spark.read.table(f"{core}.dim_product")
    dim_date = spark.read.table(f"{core}.dim_date")

    econ = dim_product.select("product_sk", "list_price", "standard_cost", "msrp")
    weights = date_seasonality_weights(dim_date.select(*_DATE_WEIGHT_COLS))
    df = build_fact_sales_line(spark, config, product_econ=econ, date_weights=weights)
    validate_fact_schema(df, FACT_SALES_LINE_SPEC)

    target = f"{core}.{FACT_SALES_LINE_SPEC.name}"
    df.write.mode("overwrite").saveAsTable(target)
    print(f"wrote {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `python -m pytest tests/test_generate_facts.py -q`
Expected: PASS (2 passed).

- [ ] **Step 7: Run the full suite to confirm no regressions**

Run: `python -m pytest -q`
Expected: PASS (all prior dim tests + the new Spark tests).

- [ ] **Step 8: Commit**

```bash
git add src/techmart/facts/registry.py src/techmart/jobs/ tests/test_generate_facts.py
git commit -m "feat: add fact registry and generate_facts job entrypoint"
```

---

### Task 6: Databricks Asset Bundle (DAB) scaffold

**Files:**
- Create: `databricks.yml`
- Create: `resources/generate_facts_job.yml`
- Modify: `README.md`
- Test: `tests/test_dab_bundle.py`

**Interfaces:**
- Consumes: `techmart.jobs.generate_facts:main` as the job entrypoint.
- Produces: a validatable DAB with `catalog` / `schema_prefix` / `scale_profile` variables and a serverless `generate_facts` job.

**Notes for the implementer:**
- The test parses YAML structurally (no workspace/auth needed). The live `databricks bundle validate` is a documented manual step, not part of the test.
- The job uses a serverless `environment` (no `job_clusters`), honoring the serverless-only constraint. The bundle host is intentionally **not** committed — it is supplied per-deploy via the `-p <profile>` CLI flag or a `DATABRICKS_HOST`/target override.

- [ ] **Step 1: Write the failing bundle-structure test**

Create `tests/test_dab_bundle.py`:

```python
from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[1]


def test_databricks_yml_structure():
    bundle = yaml.safe_load((_ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "techmart"
    # Parameterized, secret-free: catalog/schema/scale are variables.
    assert set(bundle["variables"]) >= {"catalog", "schema_prefix", "scale_profile"}
    # No committed workspace host anywhere in the bundle root.
    assert "host" not in yaml.safe_dump(bundle) or "${" in yaml.safe_dump(bundle)
    assert "dev" in bundle["targets"]


def test_generate_facts_job_is_serverless():
    job_doc = yaml.safe_load((_ROOT / "resources" / "generate_facts_job.yml").read_text())
    job = job_doc["resources"]["jobs"]["generate_facts"]
    task = job["tasks"][0]
    # Serverless: no classic job_clusters; an environment or serverless task.
    assert "job_clusters" not in job
    assert "python_wheel_task" in task or "spark_python_task" in task
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_dab_bundle.py -q`
Expected: FAIL — `FileNotFoundError` for `databricks.yml`.

- [ ] **Step 3: Create the bundle definition**

Create `databricks.yml`:

```yaml
bundle:
  name: techmart

include:
  - resources/*.yml

variables:
  catalog:
    description: Unity Catalog to hold techmart_* schemas.
    default: stable_classic_ppke9o
  schema_prefix:
    description: Schema name prefix (schemas are <prefix>core, <prefix>finance, ...).
    default: techmart_
  scale_profile:
    description: Scale profile (demo_lean | showcase | stress).
    default: showcase

targets:
  dev:
    mode: development
    default: true
    # Host is supplied per-deploy (CLI --profile / DATABRICKS_HOST); never committed.
```

- [ ] **Step 4: Create the job resource**

Create `resources/generate_facts_job.yml`:

```yaml
resources:
  jobs:
    generate_facts:
      name: techmart-generate-facts
      tasks:
        - task_key: generate_sales_line
          environment_key: default
          spark_python_task:
            python_file: ../src/techmart/jobs/generate_facts.py
            parameters:
              - "--catalog"
              - "${var.catalog}"
              - "--schema-prefix"
              - "${var.schema_prefix}"
              - "--profile"
              - "${var.scale_profile}"
      environments:
        - environment_key: default
          spec:
            client: "3"
            dependencies:
              - dbldatagen>=0.4
              - polars>=1.0.0
              - pyyaml>=6.0
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `python -m pytest tests/test_dab_bundle.py -q`
Expected: PASS (2 passed).

- [ ] **Step 6: Document deployment in the README**

Add this section to `README.md` (create the file if it does not exist; otherwise append):

```markdown
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

3. Deploy and run the fact-generation job (override variables as needed):

   ```bash
   databricks bundle deploy -p <profile> \
     --var="catalog=<catalog>,schema_prefix=techmart_,scale_profile=demo_lean"
   databricks bundle run generate_facts -p <profile>
   ```

The job reads the merged Delta dimensions from `<catalog>.techmart_core` and
writes `fact_sales_line` back to the same schema. Dimensions are generated by
the Polars CLI (`python -m techmart.cli --tables ...`) and loaded first.
```

- [ ] **Step 7: Run the full suite**

Run: `python -m pytest -q`
Expected: PASS (all tests).

- [ ] **Step 8: Commit**

```bash
git add databricks.yml resources/ README.md tests/test_dab_bundle.py
git commit -m "feat: add DAB scaffold for serverless fact generation"
```

---

## Self-Review

- **Spec coverage:** `fact_sales_line` (spec §Core facts) — all degenerate keys, 7 FKs, and the full measure list (`quantity`, `unit_price`, `unit_cost`, `gross_sales_amount`, `discount_amount`, `net_sales_amount`, `tax_amount`, `cogs_amount`, `gross_margin_amount`, `loyalty_points_earned`) plus flags (`is_return`, `is_marketplace`, `tender_type`) — Task 4. Spark/`dbldatagen` path on serverless — Tasks 1–5. DAB (`databricks.yml`) — Task 6. Conformed-dimension RI, seasonality, deterministic/idempotent generation, parameterization by profile — Tasks 3–6. Deferred to later phases (noted below): the remaining six facts, finance/AI/ops tables, semantic metric views, injected anomalies, and the full reconciliation deltas.
- **Placeholder scan:** none — every code step is complete.
- **Type consistency:** `FactSpec.column_names` order in Task 4 matches the `df.select(...)` in `build_fact_sales_line`; `build_fact_sales_line`'s keyword signature matches every call site (Tasks 4, 5); `date_seasonality_weights` takes a Spark DF in Tasks 3, 5; `product_economics` returns the four columns consumed by the econ join.

## Next plan

**Phase 4 — remaining core facts:** `fact_inventory_snapshot` (the big store×SKU×day fact and primary perf-tuning target), `fact_inventory_movement`, `fact_fulfillment`, `fact_returns`, `fact_web_events`, `fact_loyalty_activity` — reusing this phase's Spark framework, lookups, and DAB. `fact_returns` begins the gross↔net reconciliation story; `fact_inventory_snapshot` bridges to sales via `Date + Product + Store`. Later phases cover finance (`techmart_finance`), AI (`techmart_ai`), ops write-back (`techmart_ops`), and the semantic metric views (`techmart_semantic`).

**Carry-forward cosmetic minors (still open, safe to sweep in a small standalone PR):** `dim_store.region_id` zero-pad; `support.py` docstrings; a `dim_customer` enroll-null test; `dim_product.spec_attributes` via `pl.struct([...]).struct.json_encode()`; differentiate `manufacturer` from `brand_name`; fix the `brand_name` "hierarchy level 5" comment; clamp `discontinue_date <= end_date`.
```
