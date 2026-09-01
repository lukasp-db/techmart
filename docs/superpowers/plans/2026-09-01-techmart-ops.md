# Techmart Ops (`techmart_ops`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `techmart_ops` operational write-back layer — two Lakebase (Postgres) writable tables (`replenishment_order`, `forecast_override`) seeded deterministically from real lakehouse rows, plus a Delta→Postgres synced table and the DAB wiring to build them.

**Architecture:** Follows the proven serverless-native model. `build_*` functions produce deterministic Spark DataFrames (locally testable, like every prior fact). A new `pg_write.py` provides a pure DDL emitter (locally testable) plus a workspace-only psycopg write path. A `generate_ops` serverless notebook reads persisted core/AI tables, builds the DataFrames, and writes them into a bundle-provisioned Lakebase instance. The actual Postgres write / synced table / federation catalog is workspace-only behind a proven-green gate (exactly like the AI phase's `ai_query`).

**Tech Stack:** PySpark, dbldatagen (indirectly, via the source tables), psycopg (workspace-only, lazy import), Databricks SDK, Databricks Asset Bundles, pytest.

**Spec:** `docs/superpowers/specs/2026-09-01-techmart-ops-design.md`

## Global Constraints

- **Determinism discipline:** all row structure uses hash-keyed helpers from `src/techmart/facts/gen.py` (`uniform_hash`, `bounded_int`) or `F.xxhash64` for ids. NEVER `rand()`, `monotonically_increasing_id()`, `current_timestamp()`, `now()`, or `uuid()`. Timestamps derive from a source `date_sk` → `dim_date.date`.
- **Every table + column carries a `COMMENT`** (Genie/lineage). Table-level comment names the grain.
- **No Postgres `FOREIGN KEY` constraints:** the referenced dims live in Delta/UC, not Postgres, so FKs are advisory (documented in column comments, RI-by-construction) — consistent with the rest of the project, where Delta tables also don't enforce FKs. Only real `PRIMARY KEY` constraints are emitted.
- **psycopg + databricks-sdk are imported lazily** inside the workspace-only functions so `techmart.ops.pg_write` imports cleanly in local tests without those packages.
- **Bounded corpora:** row counts are capped by absolute-count scale levers (`num_replen_orders`, `num_forecast_overrides`, `forecast_serving_rows`), never derived off sales-line volume.
- **Tests are local Spark** (mirror Phase 4/5/6 fact tests). The PG write / synced table / federation catalog are workspace-only (proven-green gate). All builders keep the house signature `build_*(spark, config, *, <source dfs>)` even where `spark` is unused (matches existing builders).
- **Python idiom:** `from __future__ import annotations`; match surrounding style.

---

### Task 1: Scale-profile levers

**Files:**
- Modify: `src/techmart/config.py` (ScaleProfile dataclass)
- Modify: `config/scale_profiles.yaml`
- Test: `tests/test_config_ops_levers.py`

**Interfaces:**
- Produces: `ScaleProfile.num_replen_orders: int`, `ScaleProfile.num_forecast_overrides: int`, `ScaleProfile.forecast_serving_rows: int` (all keyword fields with defaults).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config_ops_levers.py
from pathlib import Path

from techmart.config import load_profiles

_YAML = Path(__file__).resolve().parents[1] / "config" / "scale_profiles.yaml"


def test_ops_levers_present_and_positive():
    profiles = load_profiles(_YAML)
    for name in ["smoke", "demo_lean", "showcase", "stress"]:
        p = profiles[name]
        assert p.num_replen_orders > 0
        assert p.num_forecast_overrides > 0
        assert p.forecast_serving_rows > 0


def test_ops_levers_scale_up():
    profiles = load_profiles(_YAML)
    assert profiles["smoke"].num_replen_orders <= profiles["showcase"].num_replen_orders
    assert profiles["smoke"].forecast_serving_rows <= profiles["showcase"].forecast_serving_rows
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config_ops_levers.py -v`
Expected: FAIL (`TypeError: __init__() got an unexpected keyword argument` or missing attr).

- [ ] **Step 3: Add the dataclass fields**

In `src/techmart/config.py`, append to `ScaleProfile` immediately after `forecast_horizon_weeks: int = 26`:

```python
    # Ops write-back levers (Phase 5.3).
    num_replen_orders: int = 50
    num_forecast_overrides: int = 30
    forecast_serving_rows: int = 500
```

- [ ] **Step 4: Add per-profile values in `config/scale_profiles.yaml`**

Add these three keys to each profile block (values per profile):

```yaml
  # demo_lean:
    num_replen_orders: 5000
    num_forecast_overrides: 2000
    forecast_serving_rows: 50000
  # showcase:
    num_replen_orders: 50000
    num_forecast_overrides: 20000
    forecast_serving_rows: 500000
  # smoke:
    num_replen_orders: 50
    num_forecast_overrides: 30
    forecast_serving_rows: 500
  # stress:
    num_replen_orders: 200000
    num_forecast_overrides: 80000
    forecast_serving_rows: 2000000
```

Place each trio under its existing profile (after that profile's `forecast_horizon_weeks:` line), indented to match the profile's other keys (4 spaces under the profile name).

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_config_ops_levers.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/techmart/config.py config/scale_profiles.yaml tests/test_config_ops_levers.py
git commit -m "feat(ops): add techmart_ops scale-profile levers"
```

---

### Task 2: `pg_write` — Postgres spec, DDL emitter, workspace write path

**Files:**
- Create: `src/techmart/ops/__init__.py` (empty)
- Create: `src/techmart/ops/pg_write.py`
- Test: `tests/test_pg_write.py`

**Interfaces:**
- Produces:
  - `PgTableSpec(schema, name, grain, columns: list[SparkColumn], primary_key: tuple[str, ...])` with `.column_names` and `.select_ordered(df)`.
  - `pg_type(dtype: str) -> str`
  - `pg_ddl(spec: PgTableSpec, schema: str) -> list[str]` (pure)
  - `get_pg_connection(instance_name, database)` and `write_pg(df, spec, *, conn, schema) -> int` (workspace-only, lazy psycopg).
- Consumes: `SparkColumn` from `techmart.spark.framework`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_pg_write.py
from techmart.ops.pg_write import PgTableSpec, pg_ddl, pg_type
from techmart.spark.framework import SparkColumn

_SPEC = PgTableSpec(
    schema="ops",
    name="widget",
    grain="one row per widget",
    columns=[
        SparkColumn("widget_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("qty", "int", "Units", nullable=False),
        SparkColumn("note", "string", "Free text"),
        SparkColumn("created_at", "timestamp", "When", nullable=False),
    ],
    primary_key=("widget_id",),
)


def test_pg_type_mapping():
    assert pg_type("long") == "bigint"
    assert pg_type("int") == "integer"
    assert pg_type("double") == "double precision"
    assert pg_type("string") == "text"
    assert pg_type("boolean") == "boolean"
    assert pg_type("timestamp") == "timestamptz"
    assert pg_type("date") == "date"


def test_pg_ddl_create_pk_and_types():
    create = pg_ddl(_SPEC, "techmart_ops")[0]
    assert "CREATE TABLE IF NOT EXISTS techmart_ops.widget" in create
    assert "widget_id bigint NOT NULL" in create
    assert "qty integer NOT NULL" in create
    assert "created_at timestamptz NOT NULL" in create
    # nullable column has no NOT NULL
    assert "note text" in create and "note text NOT NULL" not in create
    assert "PRIMARY KEY (widget_id)" in create


def test_pg_ddl_comments():
    joined = "\n".join(pg_ddl(_SPEC, "techmart_ops"))
    assert "COMMENT ON TABLE techmart_ops.widget IS 'one row per widget';" in joined
    assert "COMMENT ON COLUMN techmart_ops.widget.widget_id IS 'Surrogate key';" in joined
    assert "COMMENT ON COLUMN techmart_ops.widget.note IS 'Free text';" in joined


def test_pg_ddl_escapes_single_quotes():
    spec = PgTableSpec(
        schema="ops", name="t", grain="grain's test",
        columns=[SparkColumn("id", "long", "it's a key", nullable=False)],
        primary_key=("id",),
    )
    joined = "\n".join(pg_ddl(spec, "s"))
    assert "IS 'grain''s test';" in joined
    assert "IS 'it''s a key';" in joined


def test_column_names():
    assert _SPEC.column_names == ["widget_id", "qty", "note", "created_at"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pg_write.py -v`
Expected: FAIL (`ModuleNotFoundError: techmart.ops.pg_write`).

- [ ] **Step 3: Create the package + module**

Create empty `src/techmart/ops/__init__.py`.

Create `src/techmart/ops/pg_write.py`:

```python
"""Postgres (Lakebase) table spec + DDL emitter + workspace write path.

`pg_type` / `pg_ddl` / `PgTableSpec` are pure and locally testable. `write_pg`
and `get_pg_connection` import psycopg + databricks-sdk lazily and run only on
the workspace against a live Lakebase instance (proven-green gate).
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..spark.framework import SparkColumn

_PG_TYPES: dict[str, str] = {
    "long": "bigint",
    "int": "integer",
    "double": "double precision",
    "string": "text",
    "boolean": "boolean",
    "timestamp": "timestamptz",
    "date": "date",
}


def pg_type(dtype: str) -> str:
    """Map a framework dtype to its Postgres column type."""
    return _PG_TYPES[dtype]


@dataclass(frozen=True)
class PgTableSpec:
    schema: str  # target schema group, e.g. "ops"
    name: str
    grain: str
    columns: list[SparkColumn]
    primary_key: tuple[str, ...]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def select_ordered(self, df: DataFrame) -> DataFrame:
        """Project df to the spec's columns, in order (comment as field metadata)."""
        return df.select(
            *[F.col(c.name).alias(c.name, metadata={"comment": c.comment}) for c in self.columns]
        )


def _qualified(schema: str, name: str) -> str:
    return f"{schema}.{name}"


def _sql_str(s: str) -> str:
    """Single-quote a SQL string literal, escaping embedded single quotes."""
    return "'" + s.replace("'", "''") + "'"


def pg_ddl(spec: PgTableSpec, schema: str) -> list[str]:
    """Emit CREATE TABLE (PK) + table/column COMMENT statements.

    FKs to lakehouse dims are advisory (documented in column comments), not PG
    constraints: the referenced dims live in Delta/UC, not Postgres.
    """
    table = _qualified(schema, spec.name)
    col_lines = [
        f"  {c.name} {pg_type(c.dtype)}{'' if c.nullable else ' NOT NULL'}"
        for c in spec.columns
    ]
    col_lines.append(f"  PRIMARY KEY ({', '.join(spec.primary_key)})")
    create = f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(col_lines) + "\n);"
    stmts = [create, f"COMMENT ON TABLE {table} IS {_sql_str(spec.grain)};"]
    stmts += [
        f"COMMENT ON COLUMN {table}.{c.name} IS {_sql_str(c.comment)};" for c in spec.columns
    ]
    return stmts


def get_pg_connection(instance_name: str, database: str):
    """Open a psycopg connection to a Lakebase instance via workspace OAuth.

    Workspace-only. Exact SDK credential idiom is validated on the workspace
    (proven-green gate); adjust here if the SDK surface differs.
    """
    import psycopg  # lazy: not a local test/runtime dependency
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    instance = w.database.get_database_instance(name=instance_name)
    cred = w.database.generate_database_credential(
        request_id=instance_name, instance_names=[instance_name]
    )
    return psycopg.connect(
        host=instance.read_write_dns,
        dbname=database,
        user=w.current_user.me().user_name,
        password=cred.token,
        sslmode="require",
    )


def write_pg(df: DataFrame, spec: PgTableSpec, *, conn, schema: str) -> int:
    """Create-if-needed + idempotently reseed a Postgres table from a Spark DF.

    Truncate + insert gives a deterministic baseline on every regeneration.
    Workspace-only. Returns the number of rows written.
    """
    rows = [tuple(r[c] for c in spec.column_names) for r in df.collect()]
    cols = ", ".join(spec.column_names)
    placeholders = ", ".join(["%s"] * len(spec.column_names))
    table = _qualified(schema, spec.name)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        for stmt in pg_ddl(spec, schema):
            cur.execute(stmt)
        cur.execute(f"TRUNCATE TABLE {table};")
        if rows:
            cur.executemany(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", rows)
    conn.commit()
    return len(rows)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_pg_write.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ops/__init__.py src/techmart/ops/pg_write.py tests/test_pg_write.py
git commit -m "feat(ops): add pg_write PgTableSpec + DDL emitter + workspace write path"
```

---

### Task 3: `replenishment_order` builder

**Files:**
- Create: `src/techmart/ops/replenishment_order.py`
- Test: `tests/test_replenishment_order.py`

**Interfaces:**
- Consumes: `PgTableSpec` (Task 2); `uniform_hash`, `bounded_int` from `techmart.facts.gen`; `SparkColumn`.
- Produces: `REPLENISHMENT_ORDER_SPEC: PgTableSpec`; `build_replenishment_order(spark, config, *, fact_inventory_snapshot, dim_date) -> DataFrame`. Reads snapshot columns `date_sk, store_sk, product_sk, available_qty, reorder_point, safety_stock_qty`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_replenishment_order.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.ops.replenishment_order import (
    REPLENISHMENT_ORDER_SPEC, build_replenishment_order,
)

_P = ScaleProfile("t", 5, 500, 1, 50000, 1000, 20, num_replen_orders=40)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def _snapshot(spark):
    d = 20260131  # exists in dim_date (contiguous up to end_date)
    rows = [
        (d, s, p, p % 10, 5, 3)  # available_qty = p%10, reorder_point 5, safety_stock 3
        for p in range(1, 61)
        for s in range(1, 6)
    ]
    return spark.createDataFrame(
        rows,
        "date_sk long, store_sk long, product_sk long, "
        "available_qty int, reorder_point int, safety_stock_qty int",
    )


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    snap = _snapshot(spark)
    return build_replenishment_order(spark, _CFG, fact_inventory_snapshot=snap, dim_date=dd), snap


def test_schema_and_grain(spark):
    df, _ = _build(spark)
    assert df.columns == REPLENISHMENT_ORDER_SPEC.column_names
    assert df.groupBy("replen_id").count().filter(F.col("count") > 1).count() == 0


def test_bounded_and_ri(spark):
    df, snap = _build(spark)
    assert 0 < df.count() <= _P.num_replen_orders
    src = snap.select("product_sk", "store_sk").distinct()
    assert df.select("product_sk", "store_sk").distinct() \
        .join(src, ["product_sk", "store_sk"], "left_anti").count() == 0


def test_invariants(spark):
    df, _ = _build(spark)
    assert df.filter(F.col("suggested_qty") < 0).count() == 0
    bad = df.filter(
        ((F.col("status") == "Suggested") &
         (F.col("approved_qty").isNotNull() | F.col("approved_by").isNotNull()))
        | ((F.col("status") != "Suggested") &
           (F.col("approved_qty").isNull() | F.col("approved_by").isNull()))
    )
    assert bad.count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*"), F.sum("suggested_qty")).first()
    b = _build(spark)[0].agg(F.count("*"), F.sum("suggested_qty")).first()
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_replenishment_order.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the builder**

Create `src/techmart/ops/replenishment_order.py`:

```python
"""replenishment_order: operational replenishment suggestions seeded from inventory snapshots."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn
from .pg_write import PgTableSpec

_PLANNERS = ("planner_amir", "planner_bianca", "planner_chen", "planner_dana")

REPLENISHMENT_ORDER_SPEC = PgTableSpec(
    schema="ops",
    name="replenishment_order",
    grain="one row per suggested replenishment (product x store, latest snapshot)",
    columns=[
        SparkColumn("replen_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", nullable=False),
        SparkColumn("suggested_qty", "int", "System-suggested reorder units", nullable=False),
        SparkColumn("approved_qty", "int", "Planner-approved units (null while Suggested)"),
        SparkColumn("status", "string", "Suggested/Approved/Rejected/Ordered", nullable=False),
        SparkColumn("reorder_point", "int", "Reorder-point threshold from the snapshot", nullable=False),
        SparkColumn("created_by", "string", "Creator (system)", nullable=False),
        SparkColumn("approved_by", "string", "Approving planner (null while Suggested)"),
        SparkColumn("created_at", "timestamp", "Row creation time (deterministic)", nullable=False),
        SparkColumn("updated_at", "timestamp", "Last update time (deterministic)", nullable=False),
    ],
    primary_key=("replen_id",),
)


def build_replenishment_order(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_inventory_snapshot: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    max_date_sk = fact_inventory_snapshot.agg(F.max("date_sk")).first()[0]
    snap = (
        fact_inventory_snapshot
        .filter(F.col("date_sk") == F.lit(max_date_sk))
        .filter(F.col("available_qty") <= F.col("reorder_point"))
        .select("date_sk", "store_sk", "product_sk",
                "available_qty", "reorder_point", "safety_stock_qty")
    )

    pick = uniform_hash(F.col("product_sk"), F.col("store_sk"), salt="replen_pick")
    cand = (
        snap.withColumn("_r", pick)
        .orderBy("_r", "product_sk", "store_sk")
        .limit(sp.num_replen_orders)
    )

    dd = dim_date.select("date_sk", "date")
    j = cand.join(dd, "date_sk")

    keys = (F.col("product_sk"), F.col("store_sk"))
    suggested = F.greatest(
        F.col("reorder_point") + F.col("safety_stock_qty") - F.col("available_qty"), F.lit(0)
    ).cast("int")
    u = uniform_hash(*keys, salt="status")
    status = (
        F.when(u < F.lit(0.6), F.lit("Suggested"))
        .when(u < F.lit(0.8), F.lit("Approved"))
        .when(u < F.lit(0.9), F.lit("Ordered"))
        .otherwise(F.lit("Rejected"))
    )
    planner_idx = bounded_int(*keys, salt="planner", lo=1, hi=len(_PLANNERS))
    planner = F.element_at(F.array(*[F.lit(p) for p in _PLANNERS]), planner_idx)
    approved = F.greatest(
        F.col("suggested_qty") + bounded_int(*keys, salt="appr", lo=-2, hi=2), F.lit(0)
    ).cast("int")

    df = (
        j
        .withColumn("suggested_qty", suggested)
        .withColumn("status", status)
        .withColumn("replen_id", F.xxhash64(F.col("product_sk"), F.col("store_sk"),
                                            F.col("date_sk"), F.lit("replen")))
        .withColumn("approved_qty",
                    F.when(F.col("status") == F.lit("Suggested"), F.lit(None).cast("int"))
                    .otherwise(approved))
        .withColumn("created_by", F.lit("system"))
        .withColumn("approved_by",
                    F.when(F.col("status") == F.lit("Suggested"), F.lit(None).cast("string"))
                    .otherwise(planner))
        .withColumn("created_at", F.col("date").cast("timestamp"))
        .withColumn("updated_at",
                    F.expr("CASE WHEN status = 'Suggested' THEN created_at "
                           "ELSE created_at + INTERVAL 2 DAYS END"))
    )
    return REPLENISHMENT_ORDER_SPEC.select_ordered(df)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_replenishment_order.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ops/replenishment_order.py tests/test_replenishment_order.py
git commit -m "feat(ops): add replenishment_order builder"
```

---

### Task 4: `forecast_override` builder

**Files:**
- Create: `src/techmart/ops/forecast_override.py`
- Test: `tests/test_forecast_override.py`

**Interfaces:**
- Consumes: `PgTableSpec` (Task 2); `uniform_hash`, `bounded_int`; `SparkColumn`.
- Produces: `FORECAST_OVERRIDE_SPEC: PgTableSpec`; `build_forecast_override(spark, config, *, fact_sales_forecast, dim_date) -> DataFrame`. Reads forecast columns `date_sk, product_sk, store_sk, forecast_version, fiscal_year, fiscal_week, forecast_qty`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_forecast_override.py
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.ops.forecast_override import (
    FORECAST_OVERRIDE_SPEC, build_forecast_override,
)

_P = ScaleProfile("t", 5, 500, 1, 50000, 1000, 20, num_forecast_overrides=30)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))


def _forecast(spark):
    d = 20260131
    rows = []
    for p in range(1, 41):
        for s in range(1, 6):
            rows.append((d, p, s, "improved", 2026, 5, 100.0 + p))
            rows.append((d, p, s, "baseline", 2026, 5, 90.0 + p))
    return spark.createDataFrame(
        rows,
        "date_sk long, product_sk long, store_sk long, forecast_version string, "
        "fiscal_year int, fiscal_week int, forecast_qty double",
    )


def _build(spark):
    dd = build_dim_date(spark, _CFG)
    fc = _forecast(spark)
    return build_forecast_override(spark, _CFG, fact_sales_forecast=fc, dim_date=dd), fc


def test_schema_and_grain(spark):
    df, _ = _build(spark)
    assert df.columns == FORECAST_OVERRIDE_SPEC.column_names
    assert df.groupBy("override_id").count().filter(F.col("count") > 1).count() == 0


def test_bounded_and_invariants(spark):
    df, _ = _build(spark)
    assert 0 < df.count() <= _P.num_forecast_overrides
    assert df.filter(F.col("override_qty") < 0).count() == 0
    assert df.filter((F.col("override_reason") == "") | F.col("override_reason").isNull()).count() == 0
    assert df.filter((F.col("planner_id") == "") | F.col("planner_id").isNull()).count() == 0


def test_ri_only_improved(spark):
    df, fc = _build(spark)
    imp = fc.filter(F.col("forecast_version") == "improved").select("product_sk", "store_sk").distinct()
    assert df.select("product_sk", "store_sk").distinct() \
        .join(imp, ["product_sk", "store_sk"], "left_anti").count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*"), F.round(F.sum("override_qty"), 2)).first()
    b = _build(spark)[0].agg(F.count("*"), F.round(F.sum("override_qty"), 2)).first()
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_forecast_override.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the builder**

Create `src/techmart/ops/forecast_override.py`:

```python
"""forecast_override: human-in-the-loop overrides seeded from fact_sales_forecast."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import TechmartConfig
from ..facts.gen import bounded_int, uniform_hash
from ..spark.framework import SparkColumn
from .pg_write import PgTableSpec

_REASONS = ("Local promotion", "Competitor closeout", "Weather event", "Known stockout recovery")
_PLANNERS = ("planner_amir", "planner_bianca", "planner_chen", "planner_dana")

FORECAST_OVERRIDE_SPEC = PgTableSpec(
    schema="ops",
    name="forecast_override",
    grain="one row per human override of a forecast cell",
    columns=[
        SparkColumn("override_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("product_sk", "long", "Product FK (dim_product)", nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year", nullable=False),
        SparkColumn("fiscal_week", "int", "Retail fiscal week", nullable=False),
        SparkColumn("ai_forecast_qty", "double", "AI forecast units being overridden", nullable=False),
        SparkColumn("override_qty", "double", "Planner-overridden units", nullable=False),
        SparkColumn("override_reason", "string", "Reason for the override", nullable=False),
        SparkColumn("planner_id", "string", "Planner who made the override", nullable=False),
        SparkColumn("created_at", "timestamp", "Row creation time (deterministic)", nullable=False),
        SparkColumn("updated_at", "timestamp", "Last update time (deterministic)", nullable=False),
    ],
    primary_key=("override_id",),
)


def build_forecast_override(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_forecast: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    sp = config.scale_profile

    pick = uniform_hash(F.col("product_sk"), F.col("store_sk"), F.col("date_sk"), salt="override_pick")
    cand = (
        fact_sales_forecast
        .filter(F.col("forecast_version") == F.lit("improved"))
        .select("date_sk", "product_sk", "store_sk", "fiscal_year", "fiscal_week", "forecast_qty")
        .withColumn("_r", pick)
        .orderBy("_r", "product_sk", "store_sk", "date_sk")
        .limit(sp.num_forecast_overrides)
    )

    dd = dim_date.select("date_sk", "date")
    j = cand.join(dd, "date_sk")

    keys = (F.col("product_sk"), F.col("store_sk"), F.col("date_sk"))
    delta = uniform_hash(*keys, salt="override_delta") * F.lit(0.6) - F.lit(0.3)  # ±30%
    reason_idx = bounded_int(*keys, salt="reason", lo=1, hi=len(_REASONS))
    planner_idx = bounded_int(*keys, salt="planner", lo=1, hi=len(_PLANNERS))

    df = (
        j
        .withColumn("override_id", F.xxhash64(F.col("product_sk"), F.col("store_sk"),
                                              F.col("date_sk"), F.lit("override")))
        .withColumn("ai_forecast_qty", F.col("forecast_qty"))
        .withColumn("override_qty",
                    F.greatest(F.round(F.col("forecast_qty") * (F.lit(1.0) + delta), 2), F.lit(0.0)))
        .withColumn("override_reason", F.element_at(F.array(*[F.lit(r) for r in _REASONS]), reason_idx))
        .withColumn("planner_id", F.element_at(F.array(*[F.lit(p) for p in _PLANNERS]), planner_idx))
        .withColumn("created_at", F.col("date").cast("timestamp"))
        .withColumn("updated_at", F.expr("created_at + INTERVAL 1 DAY"))
    )
    return FORECAST_OVERRIDE_SPEC.select_ordered(df)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_forecast_override.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ops/forecast_override.py tests/test_forecast_override.py
git commit -m "feat(ops): add forecast_override builder"
```

---

### Task 5: `techmart_ops` registry

**Files:**
- Create: `src/techmart/ops/registry.py`
- Test: `tests/test_ops_registry.py`

**Interfaces:**
- Consumes: `REPLENISHMENT_ORDER_SPEC`, `FORECAST_OVERRIDE_SPEC`.
- Produces: `OPS_SPECS: list[PgTableSpec]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ops_registry.py
from techmart.ops.registry import OPS_SPECS


def test_ops_specs_contents():
    names = [s.name for s in OPS_SPECS]
    assert names == ["replenishment_order", "forecast_override"]
    assert len(set(names)) == len(names)
    for s in OPS_SPECS:
        assert s.primary_key  # every ops table has a PK
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ops_registry.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the registry**

Create `src/techmart/ops/registry.py`:

```python
"""techmart_ops table registry."""
from __future__ import annotations

from .forecast_override import FORECAST_OVERRIDE_SPEC
from .replenishment_order import REPLENISHMENT_ORDER_SPEC

OPS_SPECS = [REPLENISHMENT_ORDER_SPEC, FORECAST_OVERRIDE_SPEC]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_ops_registry.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/techmart/ops/registry.py tests/test_ops_registry.py
git commit -m "feat(ops): add techmart_ops table registry"
```

---

### Task 6: `generate_ops` serverless notebook

**Files:**
- Create: `notebooks/generate_ops.py`
- Modify: `tests/test_notebooks.py` (add coverage test)

**Interfaces:**
- Consumes: `build_replenishment_order`, `build_forecast_override`, `get_pg_connection`, `write_pg`, specs.
- Produces: a Databricks source notebook that reads `dim_date`, `fact_inventory_snapshot` (core), `fact_sales_forecast` (ai); builds both DataFrames; writes them to Lakebase; documents the `forecast_serving` synced table.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_notebooks.py`:

```python
def test_generate_ops_notebook_covers_builders():
    text = _read("generate_ops.py")
    assert text.splitlines()[0] == "# Databricks notebook source"
    assert "dbutils.widgets" in text
    assert "write_pg" in text
    assert "synced" in text.lower()  # documents the Delta->PG synced table
    for b in ["build_replenishment_order", "build_forecast_override"]:
        assert b in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_notebooks.py::test_generate_ops_notebook_covers_builders -v`
Expected: FAIL (`FileNotFoundError`).

- [ ] **Step 3: Write the notebook**

Create `notebooks/generate_ops.py`:

```python
# Databricks notebook source
# MAGIC %pip install "psycopg[binary]"
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import sys
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("lakebase_instance", "")
dbutils.widgets.text("lakebase_database", "techmart")
# COMMAND ----------
from pathlib import Path
from techmart.config import load_config
from techmart.ops.replenishment_order import REPLENISHMENT_ORDER_SPEC, build_replenishment_order
from techmart.ops.forecast_override import FORECAST_OVERRIDE_SPEC, build_forecast_override
from techmart.ops.pg_write import get_pg_connection, write_pg

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
instance = dbutils.widgets.get("lakebase_instance")
database = dbutils.widgets.get("lakebase_database")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
ai = f"{catalog}.{schema_prefix}ai"
ops_schema = f"{schema_prefix}ops"

dim_date = spark.read.table(f"{core}.dim_date")
snapshot = spark.read.table(f"{core}.fact_inventory_snapshot")
forecast = spark.read.table(f"{ai}.fact_sales_forecast")

# --- build deterministic operational rows (locally tested) ---
repl = build_replenishment_order(spark, config, fact_inventory_snapshot=snapshot, dim_date=dim_date)
ovr = build_forecast_override(spark, config, fact_sales_forecast=forecast, dim_date=dim_date)

# --- write-back tables: native writable Postgres (workspace-only) ---
conn = get_pg_connection(instance, database)
try:
    print("replenishment_order rows:", write_pg(repl, REPLENISHMENT_ORDER_SPEC, conn=conn, schema=ops_schema))
    print("forecast_override rows:", write_pg(ovr, FORECAST_OVERRIDE_SPEC, conn=conn, schema=ops_schema))
finally:
    conn.close()

# COMMAND ----------
# --- serve-to-app: Delta -> Postgres synced table (bounded fact_sales_forecast slice) ---
# The synced table + UC Postgres federation catalog (write-back read path) are created
# via the Databricks SDK / workspace against the provisioned instance. Created here (after
# fact_sales_forecast exists) rather than as a deploy-time resource, so ordering is safe.
# Validated on the workspace (proven-green gate).
from databricks.sdk import WorkspaceClient  # noqa: E402

w = WorkspaceClient()
serving_rows = config.scale_profile.forecast_serving_rows
serving_source = f"{ai}.fact_sales_forecast"
serving_target = f"{ops_schema}.forecast_serving"
print("synced table:", serving_target, "<-", serving_source, "rows cap:", serving_rows)
# w.database.create_synced_database_table(...)  # see spec §Architecture; wired on the workspace
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_notebooks.py -v`
Expected: PASS (all notebook tests, including the new one)

- [ ] **Step 5: Commit**

```bash
git add notebooks/generate_ops.py tests/test_notebooks.py
git commit -m "feat(ops): add generate_ops serverless notebook"
```

---

### Task 7: DAB wiring — Lakebase instance + `generate_ops` task

**Files:**
- Modify: `databricks.yml` (new variables)
- Create: `resources/lakebase.yml` (database instance resource)
- Modify: `resources/generate_facts_job.yml` (generate_ops task)
- Modify: `tests/test_dab_bundle.py` (add wiring tests)

**Interfaces:**
- Consumes: `notebooks/generate_ops.py`.
- Produces: bundle variables `lakebase_instance` (no default), `lakebase_database` (default `techmart`), `lakebase_capacity` (default `CU_1`); a `database_instances` resource; a `generate_ops` notebook task `depends_on: [generate_facts, generate_ai]`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_dab_bundle.py`:

```python
def test_ops_bundle_variables_present():
    import yaml
    bundle = yaml.safe_load((_ROOT / "databricks.yml").read_text())
    assert {"lakebase_instance", "lakebase_database"} <= set(bundle["variables"])
    # lakebase_instance has NO committed default (supplied per-deploy, like host/warehouse_id)
    inst = bundle["variables"]["lakebase_instance"]
    assert "default" not in inst or inst.get("default") in (None, "")


def test_lakebase_instance_resource_present():
    import yaml
    found = False
    candidates = [_ROOT / "databricks.yml", *sorted((_ROOT / "resources").glob("*.yml"))]
    for path in candidates:
        doc = yaml.safe_load(path.read_text()) or {}
        if "database_instances" in doc.get("resources", {}):
            found = True
    assert found, "no database_instances resource declared in the bundle"


def test_ops_task_wired():
    import yaml
    job = yaml.safe_load((_ROOT / "resources" / "generate_facts_job.yml").read_text())["resources"]["jobs"]["generate_facts"]
    by_key = {t["task_key"]: t for t in job["tasks"]}
    assert "generate_ops" in by_key
    deps = {d["task_key"] for d in by_key["generate_ops"].get("depends_on", [])}
    assert deps == {"generate_facts", "generate_ai"}
    assert by_key["generate_ops"]["notebook_task"]["notebook_path"].endswith("generate_ops.py")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_dab_bundle.py -v`
Expected: FAIL (new tests fail; existing pass).

- [ ] **Step 3: Add bundle variables**

In `databricks.yml`, add under `variables:` (after `llm_endpoint`):

```yaml
  lakebase_instance:
    description: Lakebase (Postgres) database instance name (supplied per-deploy).
  lakebase_database:
    description: Lakebase database name for techmart_ops write-back tables.
    default: techmart
  lakebase_capacity:
    description: Lakebase instance capacity unit.
    default: CU_1
```

- [ ] **Step 4: Add the database instance resource**

Create `resources/lakebase.yml`:

```yaml
# Bundle-provisioned Lakebase (managed Postgres) instance for techmart_ops write-back.
# DAB database-instance resource support is validated on the workspace (proven-green gate).
resources:
  database_instances:
    techmart_lakebase:
      name: ${var.lakebase_instance}
      capacity: ${var.lakebase_capacity}
```

- [ ] **Step 5: Add the `generate_ops` task**

In `resources/generate_facts_job.yml`, append to `tasks:` (after `generate_ai_text`):

```yaml
        - task_key: generate_ops
          depends_on:
            - task_key: generate_facts
            - task_key: generate_ai
          notebook_task:
            notebook_path: ../notebooks/generate_ops.py
            base_parameters:
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              scale_profile: ${var.scale_profile}
              lakebase_instance: ${var.lakebase_instance}
              lakebase_database: ${var.lakebase_database}
```

- [ ] **Step 6: Run the full DAB + bundle test suite**

Run: `pytest tests/test_dab_bundle.py -v`
Expected: PASS (new + existing, including `test_ai_tasks_wired_with_fanout` and `test_generate_facts_job_is_serverless_notebooks`, which must remain green)

- [ ] **Step 7: Commit**

```bash
git add databricks.yml resources/lakebase.yml resources/generate_facts_job.yml tests/test_dab_bundle.py
git commit -m "feat(ops): provision Lakebase instance + wire generate_ops task"
```

---

## Final verification

- [ ] Run the whole suite: `pytest -q` — all prior tests plus the new ops tests green.
- [ ] Confirm no builder uses `rand()`/`monotonically_increasing_id()`/`current_timestamp()`/`uuid()`.
- [ ] Confirm `import techmart.ops.pg_write` works without psycopg installed (lazy import).

## Next plan (Phase 5.4 — out of scope here)

`techmart_semantic` metric views over the gold star schema (net sales, gross margin %, sell-through, weeks-of-supply, GMROI, forecast accuracy/MAPE, budget attainment; persona groupings; a few materialized). Its own spec→plan→PR cycle. Also standing: workspace-validate this ops phase (psycopg write, synced table, federation) at smoke on field-eng-east, and the deferred `randomSeedMethod="fixed"` builder-correlation sweep.
```
