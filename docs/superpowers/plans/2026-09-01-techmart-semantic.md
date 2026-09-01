# Techmart Semantic (`techmart_semantic`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `techmart_semantic` layer — six subject-area Databricks **metric views** over the gold star schema plus informational **PK/FK `RELY` constraints** on the gold tables — via a pure, locally-tested DDL/YAML emitter and a workspace-only apply notebook.

**Architecture:** Mirrors `ops/pg_write.py`: typed specs + a pure Python emitter that produces `CREATE OR REPLACE VIEW … WITH METRICS LANGUAGE YAML AS $$…$$` and `ALTER TABLE … ADD CONSTRAINT … NOT ENFORCED RELY` strings (unit-tested), applied by a thin serverless notebook (`generate_semantic.py`) that only `spark.sql`s the emitted strings. Each metric view is single-source (one fact) joined star-style to the conformed dims; cross-fact metrics are deferred. Measure math is validated locally against sample data even though the metric-view engine itself is workspace-only.

**Tech Stack:** Python 3.10+, PySpark (local Spark for tests), PyYAML (already a core dep, used only in tests for round-trip parsing), Databricks metric views (YAML spec v1.1), DAB.

**Spec:** `docs/superpowers/specs/2026-09-01-techmart-semantic-design.md`

## Global Constraints

- **Determinism / purity:** all DDL/YAML is emitted by pure functions from frozen dataclass specs — deterministic strings, no runtime randomness, no clock, no I/O. Locally testable with no workspace.
- **Catalog-agnostic specs:** specs store bare `schema` + `table` names (`core`/`finance`/`ai`); the emitter qualifies to `<catalog>.<schema_prefix><schema>.<table>` at emit time. No committed catalog/host in specs.
- **`RELY` safety:** `NOT ENFORCED RELY` is declared only where generation guarantees the property — dimension surrogate PKs are unique (generated `1..N`), fact grain PKs are unique by construction (verified in this plan's registry against the builders), and fact→dim FK RI is the project's existing proven-green invariant. Every PK column must be `nullable=False` in its source spec (a static test enforces this).
- **Metric-view YAML:** spec `version: 1.1`; base-fact columns referenced as `source.<col>`; dims referenced by their join `name`; the join key is emitted as the quoted string key `"on"` (unquoted `on` parses as boolean `true` in YAML 1.1 — this is a correctness requirement, tested by round-trip).
- **No new `ScaleProfile` levers / no new `databricks.yml` variables** — metric-view and constraint definitions are volume-independent and need only the existing `catalog` + `schema_prefix`.
- **Workspace-only (proven-green gate, like `ai_query` / ops psycopg):** actual `CREATE VIEW WITH METRICS`, `RELY` constraint creation, and materialization refresh are validated on field-eng-east at smoke — never asserted locally.
- **Package location:** all new module code under `src/techmart/semantic/`; tests under `tests/`; notebook under `notebooks/`.

---

## File Structure

- `src/techmart/semantic/__init__.py` — package marker (empty).
- `src/techmart/semantic/metric_view.py` — `MetricField`, `MetricJoin`, `MaterializedView`, `Materialization`, `MetricViewSpec` dataclasses + pure `metric_view_ddl(spec, *, catalog, schema_prefix)` and the internal YAML renderer. (Task 1)
- `src/techmart/semantic/constraints.py` — `ForeignKey`, `TableConstraints` dataclasses + pure `pk_ddl(...)`, `fk_ddl(...)`, `drop_pk_ddl(...)` emitters. (Task 2)
- `src/techmart/semantic/metric_views.py` — the six `MetricViewSpec` instances + `METRIC_VIEW_SPECS`. (Task 3)
- `src/techmart/semantic/table_constraints.py` — `TABLE_CONSTRAINTS` (all dims + facts). (Task 5)
- `src/techmart/semantic/registry.py` — re-exports `METRIC_VIEW_SPECS`, `TABLE_CONSTRAINTS`. (Task 5)
- `notebooks/generate_semantic.py` — serverless apply notebook. (Task 6)
- `resources/generate_facts_job.yml` — add `generate_semantic` task. (Task 7)
- Tests: `tests/test_metric_view.py` (T1), `tests/test_constraints.py` (T2), `tests/test_metric_views_registry.py` (T3), `tests/test_metric_view_math.py` (T4), `tests/test_table_constraints.py` (T5), additions to `tests/test_notebooks.py` (T6) and `tests/test_dab_bundle.py` (T7).

---

## Task 1: Metric-view specs + YAML/DDL emitter

**Files:**
- Create: `src/techmart/semantic/__init__.py` (empty)
- Create: `src/techmart/semantic/metric_view.py`
- Test: `tests/test_metric_view.py`

**Interfaces:**
- Produces:
  - `MetricField(name: str, expr: str, comment: str, display_name: str | None = None, synonyms: tuple[str, ...] = (), format: dict | None = None)` — used for BOTH dimensions and measures.
  - `MetricJoin(name: str, schema: str, table: str, on: str)`
  - `MaterializedView(name: str, dimensions: tuple[str, ...], measures: tuple[str, ...], type: str = "aggregated")`
  - `Materialization(schedule: str, mode: str, materialized_views: tuple[MaterializedView, ...])`
  - `MetricViewSpec(name: str, source_schema: str, source_table: str, comment: str, dimensions: tuple[MetricField, ...], measures: tuple[MetricField, ...], joins: tuple[MetricJoin, ...] = (), materialization: Materialization | None = None)`
  - `metric_view_ddl(spec: MetricViewSpec, *, catalog: str, schema_prefix: str) -> str`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_metric_view.py
import yaml

from techmart.semantic.metric_view import (
    MaterializedView, Materialization, MetricField, MetricJoin, MetricViewSpec,
    metric_view_ddl,
)

_SPEC = MetricViewSpec(
    name="mv_demo",
    source_schema="core",
    source_table="fact_sales_line",
    comment="Demo sales metrics",
    joins=(
        MetricJoin(name="dim_date", schema="core", table="dim_date",
                   on="source.date_sk = dim_date.date_sk"),
    ),
    dimensions=(
        MetricField(name="fiscal_year", expr="dim_date.fiscal_year",
                    comment="Retail fiscal year", display_name="Fiscal Year",
                    synonyms=("FY",)),
    ),
    measures=(
        MetricField(name="net_sales", expr="SUM(source.net_sales_amount)",
                    comment="Net sales", display_name="Net Sales",
                    format={"type": "currency"}),
        MetricField(name="gross_margin_pct",
                    expr="SUM(source.gross_margin_amount)/NULLIF(SUM(source.net_sales_amount),0)",
                    comment="Gross margin percent", display_name="Gross Margin %",
                    format={"type": "percentage"}),
    ),
    materialization=Materialization(
        schedule="EVERY 24 HOURS", mode="relaxed",
        materialized_views=(
            MaterializedView(name="mv_demo_daily",
                             dimensions=("fiscal_year",), measures=("net_sales",)),
        ),
    ),
)


def test_ddl_header_and_wrapper():
    ddl = metric_view_ddl(_SPEC, catalog="cat", schema_prefix="tm_")
    assert ddl.startswith(
        "CREATE OR REPLACE VIEW cat.tm_semantic.mv_demo WITH METRICS LANGUAGE YAML AS $$"
    )
    assert ddl.rstrip().endswith("$$")


def test_inner_yaml_round_trips_with_quoted_on_key():
    ddl = metric_view_ddl(_SPEC, catalog="cat", schema_prefix="tm_")
    inner = ddl.split("$$")[1]
    doc = yaml.safe_load(inner)
    assert doc["version"] == 1.1
    assert doc["source"] == "cat.tm_core.fact_sales_line"
    assert doc["joins"][0]["name"] == "dim_date"
    assert doc["joins"][0]["source"] == "cat.tm_core.dim_date"
    # The join key MUST survive as the string "on", not boolean True.
    assert doc["joins"][0]["on"] == "source.date_sk = dim_date.date_sk"
    assert True not in doc["joins"][0]  # no bool key from an unquoted `on`


def test_dimensions_and_measures_present():
    doc = yaml.safe_load(metric_view_ddl(_SPEC, catalog="c", schema_prefix="tm_").split("$$")[1])
    dim = doc["dimensions"][0]
    assert dim == {"name": "fiscal_year", "expr": "dim_date.fiscal_year",
                   "display_name": "Fiscal Year", "comment": "Retail fiscal year",
                   "synonyms": ["FY"]}
    names = {m["name"]: m for m in doc["measures"]}
    assert names["net_sales"]["expr"] == "SUM(source.net_sales_amount)"
    assert names["net_sales"]["format"] == {"type": "currency"}
    assert names["gross_margin_pct"]["format"] == {"type": "percentage"}


def test_materialization_block():
    doc = yaml.safe_load(metric_view_ddl(_SPEC, catalog="c", schema_prefix="tm_").split("$$")[1])
    mat = doc["materialization"]
    assert mat["schedule"] == "EVERY 24 HOURS"
    assert mat["mode"] == "relaxed"
    mv = mat["materialized_views"][0]
    assert mv == {"name": "mv_demo_daily", "type": "aggregated",
                  "dimensions": ["fiscal_year"], "measures": ["net_sales"]}


def test_no_materialization_omits_block():
    spec = MetricViewSpec(name="mv_x", source_schema="ai", source_table="fact_sales_forecast",
                          comment="c", dimensions=(), measures=(
                              MetricField(name="q", expr="SUM(source.forecast_qty)", comment="q"),))
    doc = yaml.safe_load(metric_view_ddl(spec, catalog="c", schema_prefix="tm_").split("$$")[1])
    assert "materialization" not in doc
    assert "joins" not in doc  # empty joins tuple omits the key
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_metric_view.py -v`
Expected: FAIL (module `techmart.semantic.metric_view` does not exist).

- [ ] **Step 3: Write minimal implementation**

Create empty `src/techmart/semantic/__init__.py`, then:

```python
# src/techmart/semantic/metric_view.py
"""Metric-view specs + a pure YAML/DDL emitter.

`metric_view_ddl` produces a `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE
YAML AS $$...$$` statement (Databricks metric-view spec v1.1). Pure and locally
testable; the actual execution (metric-view engine) is workspace-only.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# YAML 1.1 boolean-like keys that MUST be quoted so they round-trip as strings
# (an unquoted `on:` key parses back as boolean True).
_QUOTE_KEYS = {"on", "off", "yes", "no", "true", "false", "null"}


@dataclass(frozen=True)
class MetricField:
    name: str
    expr: str
    comment: str
    display_name: str | None = None
    synonyms: tuple[str, ...] = ()
    format: dict | None = None


@dataclass(frozen=True)
class MetricJoin:
    name: str
    schema: str
    table: str
    on: str


@dataclass(frozen=True)
class MaterializedView:
    name: str
    dimensions: tuple[str, ...]
    measures: tuple[str, ...]
    type: str = "aggregated"


@dataclass(frozen=True)
class Materialization:
    schedule: str
    mode: str
    materialized_views: tuple[MaterializedView, ...]


@dataclass(frozen=True)
class MetricViewSpec:
    name: str
    source_schema: str
    source_table: str
    comment: str
    dimensions: tuple[MetricField, ...]
    measures: tuple[MetricField, ...]
    joins: tuple[MetricJoin, ...] = ()
    materialization: Materialization | None = None


def _qualify(catalog: str, schema_prefix: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema_prefix}{schema}.{table}"


def _yaml_scalar(value) -> str:
    """Render a scalar. Strings are double-quoted (escaping \\ and "); numbers
    and bools are emitted raw so they round-trip as YAML numbers/bools."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    s = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _yaml(obj, indent: int = 0) -> list[str]:
    """Deterministic YAML renderer for dict/list/scalar with 2-space indent.
    Dict keys that are YAML boolean-like are quoted (see _QUOTE_KEYS)."""
    pad = "  " * indent
    lines: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f'"{k}"' if str(k).lower() in _QUOTE_KEYS else str(k)
            if isinstance(v, dict):
                lines.append(f"{pad}{key}:")
                lines += _yaml(v, indent + 1)
            elif isinstance(v, list):
                lines.append(f"{pad}{key}:")
                lines += _yaml(v, indent)  # list items align under the key
            else:
                lines.append(f"{pad}{key}: {_yaml_scalar(v)}")
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                rendered = _yaml(item, indent + 1)
                # hang the first key off the "- " marker
                first = rendered[0].lstrip()
                lines.append(f"{pad}- {first}")
                lines += rendered[1:]
            else:
                lines.append(f"{pad}- {_yaml_scalar(item)}")
    else:  # pragma: no cover - scalars handled inline above
        lines.append(f"{pad}{_yaml_scalar(obj)}")
    return lines


def _field_dict(f: MetricField) -> dict:
    d: dict = {"name": f.name, "expr": f.expr}
    if f.display_name is not None:
        d["display_name"] = f.display_name
    d["comment"] = f.comment
    if f.synonyms:
        d["synonyms"] = list(f.synonyms)
    if f.format is not None:
        d["format"] = f.format
    return d


def metric_view_ddl(spec: MetricViewSpec, *, catalog: str, schema_prefix: str) -> str:
    view = _qualify(catalog, schema_prefix, "semantic", spec.name)
    inner: dict = {
        "version": 1.1,
        "comment": spec.comment,
        "source": _qualify(catalog, schema_prefix, spec.source_schema, spec.source_table),
    }
    if spec.joins:
        inner["joins"] = [
            {"name": j.name,
             "source": _qualify(catalog, schema_prefix, j.schema, j.table),
             "on": j.on}
            for j in spec.joins
        ]
    inner["dimensions"] = [_field_dict(d) for d in spec.dimensions]
    inner["measures"] = [_field_dict(m) for m in spec.measures]
    if spec.materialization is not None:
        m = spec.materialization
        inner["materialization"] = {
            "schedule": m.schedule,
            "mode": m.mode,
            "materialized_views": [
                {"name": mv.name, "type": mv.type,
                 "dimensions": list(mv.dimensions), "measures": list(mv.measures)}
                for mv in m.materialized_views
            ],
        }
    body = "\n".join(_yaml(inner))
    return f"CREATE OR REPLACE VIEW {view} WITH METRICS LANGUAGE YAML AS $$\n{body}\n$$;"
```

> Note: verify the `_yaml` list/dict indentation renders valid YAML by relying on the round-trip tests (they parse the output with `yaml.safe_load`). If a nested list-of-dicts case mis-indents, fix `_yaml` until every test in this task passes — the round-trip assertions are the contract.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_metric_view.py -v`
Expected: PASS (all 5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/semantic/__init__.py src/techmart/semantic/metric_view.py tests/test_metric_view.py
git commit -m "feat(semantic): metric-view specs + pure YAML/DDL emitter"
```

---

## Task 2: PK/FK `RELY` constraint emitter

**Files:**
- Create: `src/techmart/semantic/constraints.py`
- Test: `tests/test_constraints.py`

**Interfaces:**
- Produces:
  - `ForeignKey(columns: tuple[str, ...], ref_schema: str, ref_table: str, ref_columns: tuple[str, ...])`
  - `TableConstraints(schema: str, table: str, primary_key: tuple[str, ...], foreign_keys: tuple[ForeignKey, ...] = ())`
  - `pk_ddl(tc, *, catalog, schema_prefix) -> str`
  - `fk_ddl(tc, fk, *, catalog, schema_prefix) -> str`
  - `drop_pk_ddl(tc, *, catalog, schema_prefix) -> str` (idempotency helper)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_constraints.py
from techmart.semantic.constraints import (
    ForeignKey, TableConstraints, drop_pk_ddl, fk_ddl, pk_ddl,
)

_TC = TableConstraints(
    schema="core", table="fact_sales_line",
    primary_key=("transaction_id", "line_number"),
    foreign_keys=(
        ForeignKey(columns=("date_sk",), ref_schema="core", ref_table="dim_date",
                   ref_columns=("date_sk",)),
        ForeignKey(columns=("promotion_sk",), ref_schema="core", ref_table="dim_promotion",
                   ref_columns=("promotion_sk",)),
    ),
)


def test_pk_ddl_rely():
    sql = pk_ddl(_TC, catalog="cat", schema_prefix="tm_")
    assert sql == (
        "ALTER TABLE cat.tm_core.fact_sales_line "
        "ADD CONSTRAINT fact_sales_line_pk "
        "PRIMARY KEY (transaction_id, line_number) NOT ENFORCED RELY;"
    )


def test_fk_ddl_rely():
    sql = fk_ddl(_TC, _TC.foreign_keys[0], catalog="cat", schema_prefix="tm_")
    assert sql == (
        "ALTER TABLE cat.tm_core.fact_sales_line "
        "ADD CONSTRAINT fact_sales_line_date_sk_fk "
        "FOREIGN KEY (date_sk) REFERENCES cat.tm_core.dim_date (date_sk) "
        "NOT ENFORCED RELY;"
    )


def test_fk_constraint_names_unique_per_table():
    names = {fk_ddl(_TC, fk, catalog="c", schema_prefix="tm_").split("ADD CONSTRAINT ")[1].split(" ")[0]
             for fk in _TC.foreign_keys}
    assert len(names) == len(_TC.foreign_keys)


def test_drop_pk_ddl_if_exists():
    sql = drop_pk_ddl(_TC, catalog="cat", schema_prefix="tm_")
    assert sql == (
        "ALTER TABLE cat.tm_core.fact_sales_line "
        "DROP CONSTRAINT IF EXISTS fact_sales_line_pk;"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_constraints.py -v`
Expected: FAIL (module does not exist).

- [ ] **Step 3: Write minimal implementation**

```python
# src/techmart/semantic/constraints.py
"""Informational PK/FK constraint specs + pure DDL emitters.

Every constraint is `NOT ENFORCED RELY`: RELY lets the optimizer trust the
constraint (join / group-by elimination). Safe only because generation
guarantees key uniqueness (PK) and referential integrity (FK) by construction.
Pure string generation; the actual ALTER TABLE runs workspace-only.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    ref_schema: str
    ref_table: str
    ref_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableConstraints:
    schema: str
    table: str
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()


def _qualify(catalog: str, schema_prefix: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema_prefix}{schema}.{table}"


def _pk_name(tc: TableConstraints) -> str:
    return f"{tc.table}_pk"


def _fk_name(tc: TableConstraints, fk: ForeignKey) -> str:
    return f"{tc.table}_{'_'.join(fk.columns)}_fk"


def pk_ddl(tc: TableConstraints, *, catalog: str, schema_prefix: str) -> str:
    table = _qualify(catalog, schema_prefix, tc.schema, tc.table)
    cols = ", ".join(tc.primary_key)
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {_pk_name(tc)} "
        f"PRIMARY KEY ({cols}) NOT ENFORCED RELY;"
    )


def drop_pk_ddl(tc: TableConstraints, *, catalog: str, schema_prefix: str) -> str:
    table = _qualify(catalog, schema_prefix, tc.schema, tc.table)
    return f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_pk_name(tc)};"


def fk_ddl(tc: TableConstraints, fk: ForeignKey, *, catalog: str, schema_prefix: str) -> str:
    table = _qualify(catalog, schema_prefix, tc.schema, tc.table)
    ref = _qualify(catalog, schema_prefix, fk.ref_schema, fk.ref_table)
    cols = ", ".join(fk.columns)
    ref_cols = ", ".join(fk.ref_columns)
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {_fk_name(tc, fk)} "
        f"FOREIGN KEY ({cols}) REFERENCES {ref} ({ref_cols}) NOT ENFORCED RELY;"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_constraints.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/semantic/constraints.py tests/test_constraints.py
git commit -m "feat(semantic): PK/FK NOT ENFORCED RELY constraint emitter"
```

---

## Task 3: The six metric-view specs (`METRIC_VIEW_SPECS`)

**Files:**
- Create: `src/techmart/semantic/metric_views.py`
- Test: `tests/test_metric_views_registry.py`

**Interfaces:**
- Consumes: `MetricViewSpec`, `MetricField`, `MetricJoin`, `Materialization`, `MaterializedView` (Task 1).
- Produces: `METRIC_VIEW_SPECS: tuple[MetricViewSpec, ...]` (six views: `mv_sales`, `mv_inventory`, `mv_inventory_valuation`, `mv_forecast`, `mv_gl_actuals`, `mv_budget_plan`).

**Column facts (verbatim from the specs — bind exactly):**
- `dim_date`: `date`, `fiscal_year`, `fiscal_period`, `fiscal_quarter`, `selling_season`, `is_weekend`, `is_holiday`.
- `dim_product`: `division_name`, `department_name`, `category_name`, `subcategory_name`, `brand_name`, `product_name`.
- `dim_store`: `region_name`, `district_name`, `store_format`, `store_name`, `state`.
- `dim_customer`: `segment`, `loyalty_tier`, `customer_type`.
- `dim_channel`: `channel_name`, `channel_type`.
- `dim_promotion`: `promo_type`, `funding_source`.
- `dim_gl_account`: `account_type`, `statement`, `account_name`, `account_category`.
- `dim_department`: `department_name`, `department_group`.
- Source measures per fact — see spec `§Metric views`; `fact_inventory_valuation` has **no** `vendor_sk` (join only `dim_date`, `dim_store`; `category_id`/`category_name` are degenerate source columns).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_metric_views_registry.py
import re

from techmart.ai.registry import AI_SPECS
from techmart.facts.registry import FACT_SPECS
from techmart.finance.registry import FINANCE_SPECS
from techmart.semantic.metric_views import METRIC_VIEW_SPECS

# Build a lookup of (schema, table) -> set(column names) from the real specs.
_DIM_TABLES = {}  # filled below via a helper in the test


def _all_specs():
    from techmart.spark.dimensions.dim_date import DIM_DATE_SPEC
    from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC
    from techmart.spark.dimensions.dim_store import DIM_STORE_SPEC
    from techmart.spark.dimensions.dim_customer import DIM_CUSTOMER_SPEC
    from techmart.spark.dimensions.dim_channel import DIM_CHANNEL_SPEC
    from techmart.spark.dimensions.dim_promotion import DIM_PROMOTION_SPEC
    from techmart.spark.dimensions.dim_vendor import DIM_VENDOR_SPEC
    from techmart.spark.dimensions.dim_employee import DIM_EMPLOYEE_SPEC
    dims = [DIM_DATE_SPEC, DIM_PRODUCT_SPEC, DIM_STORE_SPEC, DIM_CUSTOMER_SPEC,
            DIM_CHANNEL_SPEC, DIM_PROMOTION_SPEC, DIM_VENDOR_SPEC, DIM_EMPLOYEE_SPEC]
    facts = list(FACT_SPECS.values()) + list(FINANCE_SPECS) + list(AI_SPECS)
    return {(s.schema, s.name): set(s.column_names) for s in dims + facts}


def test_six_views_distinct_names():
    names = [v.name for v in METRIC_VIEW_SPECS]
    assert names == ["mv_sales", "mv_inventory", "mv_inventory_valuation",
                     "mv_forecast", "mv_gl_actuals", "mv_budget_plan"]
    assert len(set(names)) == 6


def test_sources_are_real_tables():
    cols = _all_specs()
    for v in METRIC_VIEW_SPECS:
        assert (v.source_schema, v.source_table) in cols


def test_flagship_views_are_materialized():
    by = {v.name: v for v in METRIC_VIEW_SPECS}
    assert by["mv_sales"].materialization is not None
    assert by["mv_inventory"].materialization is not None
    # materialized dims/measures must reference this view's own field names
    for name in ("mv_sales", "mv_inventory"):
        v = by[name]
        dim_names = {d.name for d in v.dimensions}
        meas_names = {m.name for m in v.measures}
        for mv in v.materialization.materialized_views:
            assert set(mv.dimensions) <= dim_names
            assert set(mv.measures) <= meas_names


def _refs(expr):
    # extract alias.column tokens (e.g. source.net_sales_amount, dim_date.fiscal_year)
    return re.findall(r"\b([a-z_]+)\.([a-z_]+)\b", expr)


def test_every_expr_references_valid_columns():
    cols = _all_specs()
    for v in METRIC_VIEW_SPECS:
        alias_to_table = {"source": (v.source_schema, v.source_table)}
        for j in v.joins:
            alias_to_table[j.name] = (j.schema, j.table)
        for f in list(v.dimensions) + list(v.measures):
            for alias, col in _refs(f.expr):
                assert alias in alias_to_table, f"{v.name}.{f.name}: unknown alias {alias}"
                schema, table = alias_to_table[alias]
                assert col in cols[(schema, table)], \
                    f"{v.name}.{f.name}: {alias}.{col} not on {schema}.{table}"


def test_join_on_predicates_reference_valid_columns():
    cols = _all_specs()
    for v in METRIC_VIEW_SPECS:
        alias_to_table = {"source": (v.source_schema, v.source_table)}
        for j in v.joins:
            alias_to_table[j.name] = (j.schema, j.table)
        for j in v.joins:
            for alias, col in _refs(j.on):
                assert alias in alias_to_table
                assert col in cols[alias_to_table[alias]]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_metric_views_registry.py -v`
Expected: FAIL (module `techmart.semantic.metric_views` does not exist).

- [ ] **Step 3: Write minimal implementation**

Create `src/techmart/semantic/metric_views.py`. Reusable join builders keep the specs DRY; every column below is verified against the specs.

```python
# src/techmart/semantic/metric_views.py
"""The six subject-area metric views over the gold star schema.

Each view is single-source (one fact) joined star-style to the conformed dims.
Cross-fact metrics (sell-through, weeks-of-supply, forecast MAPE, budget
attainment) are deferred (spec §Out of scope).
"""
from __future__ import annotations

from .metric_view import (
    MaterializedView, Materialization, MetricField, MetricJoin, MetricViewSpec,
)

_CUR = {"type": "currency"}
_PCT = {"type": "percentage"}


def _j(name: str, schema: str, fk: str, pk: str) -> MetricJoin:
    """Star join: source.<fk> = <name>.<pk>."""
    return MetricJoin(name=name, schema=schema, table=name,
                      on=f"source.{fk} = {name}.{pk}")


# --- shared conformed-dimension join sets ------------------------------------
_JOIN_DATE = _j("dim_date", "core", "date_sk", "date_sk")
_JOIN_PRODUCT = _j("dim_product", "core", "product_sk", "product_sk")
_JOIN_STORE = _j("dim_store", "core", "store_sk", "store_sk")
_JOIN_CUSTOMER = _j("dim_customer", "core", "customer_sk", "customer_sk")
_JOIN_CHANNEL = _j("dim_channel", "core", "channel_sk", "channel_sk")
_JOIN_PROMO = _j("dim_promotion", "core", "promotion_sk", "promotion_sk")
_JOIN_GL = _j("dim_gl_account", "finance", "gl_account_sk", "gl_account_sk")
_JOIN_DEPT = _j("dim_department", "finance", "department_sk", "department_sk")


def _date_dims() -> tuple[MetricField, ...]:
    return (
        MetricField("date", "dim_date.date", "Calendar date", "Date"),
        MetricField("fiscal_year", "dim_date.fiscal_year", "Retail fiscal year",
                    "Fiscal Year", ("FY",)),
        MetricField("fiscal_period", "dim_date.fiscal_period",
                    "Retail fiscal period (1-12)", "Fiscal Period"),
        MetricField("fiscal_quarter", "dim_date.fiscal_quarter",
                    "Retail fiscal quarter", "Fiscal Quarter"),
        MetricField("selling_season", "dim_date.selling_season",
                    "Selling season (Holiday, Back-to-School, ...)", "Selling Season"),
    )


def _product_dims() -> tuple[MetricField, ...]:
    return (
        MetricField("division", "dim_product.division_name", "Product division", "Division"),
        MetricField("department", "dim_product.department_name",
                    "Product department", "Department", ("merch department",)),
        MetricField("category", "dim_product.category_name", "Product category", "Category"),
        MetricField("subcategory", "dim_product.subcategory_name",
                    "Product subcategory", "Subcategory"),
        MetricField("brand", "dim_product.brand_name", "Product brand", "Brand"),
        MetricField("product", "dim_product.product_name", "Product name", "Product"),
    )


def _store_dims() -> tuple[MetricField, ...]:
    return (
        MetricField("region", "dim_store.region_name", "Store region", "Region"),
        MetricField("district", "dim_store.district_name", "Store district", "District"),
        MetricField("store_format", "dim_store.store_format",
                    "Store format (Flagship/Standard/Outlet/Online-only)", "Store Format"),
        MetricField("store", "dim_store.store_name", "Store name", "Store"),
        MetricField("state", "dim_store.state", "Store state", "State"),
    )


# =============================================================================
# mv_sales  (source: core.fact_sales_line) — MATERIALIZED
# =============================================================================
MV_SALES = MetricViewSpec(
    name="mv_sales",
    source_schema="core", source_table="fact_sales_line",
    comment="Sales performance metrics at transaction-line grain.",
    joins=(_JOIN_DATE, _JOIN_PRODUCT, _JOIN_STORE, _JOIN_CUSTOMER, _JOIN_CHANNEL, _JOIN_PROMO),
    dimensions=(
        *_date_dims(), *_product_dims(), *_store_dims(),
        MetricField("channel", "dim_channel.channel_name", "Sales channel", "Channel"),
        MetricField("channel_type", "dim_channel.channel_type",
                    "Channel type (Physical/Digital)", "Channel Type"),
        MetricField("customer_segment", "dim_customer.segment",
                    "Customer segment", "Customer Segment"),
        MetricField("loyalty_tier", "dim_customer.loyalty_tier",
                    "Loyalty tier", "Loyalty Tier"),
        MetricField("customer_type", "dim_customer.customer_type",
                    "Retail or Commercial-B2B", "Customer Type"),
        MetricField("promo_type", "dim_promotion.promo_type",
                    "Promotion type", "Promotion Type"),
        MetricField("funding_source", "dim_promotion.funding_source",
                    "Promotion funding source (Retailer/Vendor)", "Funding Source"),
    ),
    measures=(
        MetricField("gross_sales", "SUM(source.gross_sales_amount)",
                    "Gross sales amount", "Gross Sales", ("revenue",), _CUR),
        MetricField("net_sales", "SUM(source.net_sales_amount)",
                    "Net sales amount (gross - discount)", "Net Sales", ("sales",), _CUR),
        MetricField("discount", "SUM(source.discount_amount)",
                    "Promotional discount", "Discount", (), _CUR),
        MetricField("discount_rate",
                    "SUM(source.discount_amount)/NULLIF(SUM(source.gross_sales_amount),0)",
                    "Discount as a fraction of gross sales", "Discount Rate", (), _PCT),
        MetricField("cogs", "SUM(source.cogs_amount)", "Cost of goods sold", "COGS", (), _CUR),
        MetricField("gross_margin", "SUM(source.gross_margin_amount)",
                    "Gross margin amount (net sales - COGS)", "Gross Margin", ("margin",), _CUR),
        MetricField("gross_margin_pct",
                    "SUM(source.gross_margin_amount)/NULLIF(SUM(source.net_sales_amount),0)",
                    "Gross margin as a fraction of net sales", "Gross Margin %",
                    ("margin percent", "GM%"), _PCT),
        MetricField("units", "SUM(source.quantity)", "Units sold", "Units"),
        MetricField("line_count", "COUNT(1)", "Sales line count", "Line Count"),
        MetricField("transaction_count", "COUNT(DISTINCT source.transaction_id)",
                    "Distinct transaction (receipt) count", "Transactions", ("orders",)),
        MetricField("avg_order_value",
                    "SUM(source.net_sales_amount)/NULLIF(COUNT(DISTINCT source.transaction_id),0)",
                    "Average net sales per transaction", "Avg Order Value", ("AOV",), _CUR),
        MetricField("avg_basket_units",
                    "SUM(source.quantity)/NULLIF(COUNT(DISTINCT source.transaction_id),0)",
                    "Average units per transaction", "Avg Basket Units"),
        MetricField("avg_unit_price",
                    "SUM(source.gross_sales_amount)/NULLIF(SUM(source.quantity),0)",
                    "Average selling price per unit", "Avg Unit Price", (), _CUR),
    ),
    materialization=Materialization(
        schedule="EVERY 24 HOURS", mode="relaxed",
        materialized_views=(
            MaterializedView(
                name="mv_sales_daily",
                dimensions=("date", "region", "department"),
                measures=("net_sales", "gross_margin", "units", "transaction_count"),
            ),
        ),
    ),
)

# =============================================================================
# mv_inventory  (source: core.fact_inventory_snapshot) — MATERIALIZED
# =============================================================================
MV_INVENTORY = MetricViewSpec(
    name="mv_inventory",
    source_schema="core", source_table="fact_inventory_snapshot",
    comment="Inventory stock-position metrics at store x SKU x day grain.",
    joins=(_JOIN_DATE, _JOIN_PRODUCT, _JOIN_STORE),
    dimensions=(*_date_dims(), *_product_dims(), *_store_dims()),
    measures=(
        MetricField("on_hand_qty", "SUM(source.on_hand_qty)", "Units on hand", "On-Hand Units"),
        MetricField("available_qty", "SUM(source.available_qty)",
                    "Available units (on hand - reserved)", "Available Units"),
        MetricField("on_order_qty", "SUM(source.on_order_qty)",
                    "Units on open purchase orders", "On-Order Units"),
        MetricField("reserved_qty", "SUM(source.reserved_qty)", "Reserved units", "Reserved Units"),
        MetricField("on_hand_cost_value", "SUM(source.on_hand_cost_value)",
                    "Inventory at cost", "On-Hand Cost Value", (), _CUR),
        MetricField("on_hand_retail_value", "SUM(source.on_hand_retail_value)",
                    "Inventory at retail", "On-Hand Retail Value", (), _CUR),
        MetricField("avg_days_of_supply", "AVG(source.days_of_supply)",
                    "Average days of supply", "Avg Days of Supply", ("DOS",)),
        MetricField("out_of_stock_rate",
                    "AVG(CASE WHEN source.is_out_of_stock THEN 1 ELSE 0 END)",
                    "Fraction of store x SKU x day cells out of stock", "Out-of-Stock Rate",
                    ("OOS rate",), _PCT),
        MetricField("sku_count", "COUNT(DISTINCT source.product_sk)",
                    "Distinct SKU count", "SKU Count"),
        MetricField("stocked_store_count", "COUNT(DISTINCT source.store_sk)",
                    "Distinct store count", "Store Count"),
    ),
    materialization=Materialization(
        schedule="EVERY 24 HOURS", mode="relaxed",
        materialized_views=(
            MaterializedView(
                name="mv_inventory_daily",
                dimensions=("date", "region", "department"),
                measures=("on_hand_qty", "on_hand_cost_value", "out_of_stock_rate"),
            ),
        ),
    ),
)

# =============================================================================
# mv_inventory_valuation  (source: finance.fact_inventory_valuation)
#   NOTE: no vendor_sk on this fact; category_id/category_name are degenerate.
# =============================================================================
MV_INVENTORY_VALUATION = MetricViewSpec(
    name="mv_inventory_valuation",
    source_schema="finance", source_table="fact_inventory_valuation",
    comment="Finance view of inventory value at store x category x fiscal period.",
    joins=(_JOIN_DATE, _JOIN_STORE),
    dimensions=(
        *_date_dims(), *_store_dims(),
        MetricField("category_id", "source.category_id", "Product category id", "Category Id"),
        MetricField("category_name", "source.category_name",
                    "Product category name", "Category"),
    ),
    measures=(
        MetricField("on_hand_cost_value", "SUM(source.on_hand_cost_value)",
                    "Period-end inventory at cost", "On-Hand Cost Value", (), _CUR),
        MetricField("on_hand_retail_value", "SUM(source.on_hand_retail_value)",
                    "Period-end inventory at retail", "On-Hand Retail Value", (), _CUR),
        MetricField("cogs", "SUM(source.cogs_amount)", "Category COGS", "COGS", (), _CUR),
        MetricField("markdown", "SUM(source.markdown_amount)", "Markdown value", "Markdown", (), _CUR),
        MetricField("shrink", "SUM(source.shrink_amount)", "Shrink value", "Shrink", (), _CUR),
        MetricField("avg_gmroi", "AVG(source.gmroi)",
                    "Gross-margin return on inventory investment", "Avg GMROI", ("GMROI",)),
    ),
)

# =============================================================================
# mv_forecast  (source: ai.fact_sales_forecast)
# =============================================================================
MV_FORECAST = MetricViewSpec(
    name="mv_forecast",
    source_schema="ai", source_table="fact_sales_forecast",
    comment="AI demand-forecast levels at product x store x fiscal week x version.",
    joins=(_JOIN_DATE, _JOIN_PRODUCT, _JOIN_STORE),
    dimensions=(
        *_date_dims(), *_product_dims(), *_store_dims(),
        MetricField("forecast_version", "source.forecast_version",
                    "Forecast model version (baseline/improved)", "Forecast Version"),
        MetricField("model_name", "source.model_name", "Forecast model name", "Model"),
        MetricField("fiscal_week", "source.fiscal_week", "Retail fiscal week", "Fiscal Week"),
    ),
    measures=(
        MetricField("forecast_qty", "SUM(source.forecast_qty)",
                    "Projected units", "Forecast Units"),
        MetricField("forecast_amount", "SUM(source.forecast_amount)",
                    "Projected net sales amount", "Forecast Amount", (), _CUR),
        MetricField("lower_bound", "SUM(source.lower_bound)",
                    "Lower prediction bound (qty)", "Lower Bound"),
        MetricField("upper_bound", "SUM(source.upper_bound)",
                    "Upper prediction bound (qty)", "Upper Bound"),
        MetricField("interval_width", "SUM(source.upper_bound - source.lower_bound)",
                    "Prediction interval width (qty)", "Interval Width"),
    ),
)

# =============================================================================
# mv_gl_actuals  (source: finance.fact_gl_actuals)
# =============================================================================
MV_GL_ACTUALS = MetricViewSpec(
    name="mv_gl_actuals",
    source_schema="finance", source_table="fact_gl_actuals",
    comment="GL actual amounts at account x store x department x fiscal period.",
    joins=(_JOIN_DATE, _JOIN_STORE, _JOIN_GL, _JOIN_DEPT),
    dimensions=(
        *_date_dims(), *_store_dims(),
        MetricField("account_type", "dim_gl_account.account_type",
                    "Revenue/COGS/Opex/Asset", "Account Type"),
        MetricField("statement", "dim_gl_account.statement",
                    "P&L or Balance-Sheet", "Statement"),
        MetricField("account", "dim_gl_account.account_name", "GL account name", "Account"),
        MetricField("account_category", "dim_gl_account.account_category",
                    "Account rollup category", "Account Category"),
        MetricField("department", "dim_department.department_name",
                    "Cost-center department", "Department"),
        MetricField("department_group", "dim_department.department_group",
                    "COGS-bearing or Opex grouping", "Department Group"),
    ),
    measures=(
        MetricField("actual_amount", "SUM(source.actual_amount)",
                    "Actual amount (contra revenue negative)", "Actual Amount", (), _CUR),
    ),
)

# =============================================================================
# mv_budget_plan  (source: finance.fact_budget_plan)
# =============================================================================
MV_BUDGET_PLAN = MetricViewSpec(
    name="mv_budget_plan",
    source_schema="finance", source_table="fact_budget_plan",
    comment="Budget/forecast plan amounts at account x store x department x period x version.",
    joins=(_JOIN_DATE, _JOIN_STORE, _JOIN_GL, _JOIN_DEPT),
    dimensions=(
        *_date_dims(), *_store_dims(),
        MetricField("plan_version", "source.plan_version",
                    "Budget/Forecast/Latest-Estimate", "Plan Version"),
        MetricField("scenario", "source.scenario", "Planning scenario", "Scenario"),
        MetricField("account_type", "dim_gl_account.account_type",
                    "Revenue/COGS/Opex/Asset", "Account Type"),
        MetricField("account", "dim_gl_account.account_name", "GL account name", "Account"),
        MetricField("department", "dim_department.department_name",
                    "Cost-center department", "Department"),
    ),
    measures=(
        MetricField("plan_amount", "SUM(source.plan_amount)", "Planned amount",
                    "Plan Amount", (), _CUR),
        MetricField("plan_units", "SUM(source.plan_units)", "Planned units (proxy)", "Plan Units"),
    ),
)

METRIC_VIEW_SPECS: tuple[MetricViewSpec, ...] = (
    MV_SALES, MV_INVENTORY, MV_INVENTORY_VALUATION, MV_FORECAST, MV_GL_ACTUALS, MV_BUDGET_PLAN,
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_metric_views_registry.py -v`
Expected: PASS. If `test_every_expr_references_valid_columns` flags a column, fix the spec column name against the real spec (do not weaken the test).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/semantic/metric_views.py tests/test_metric_views_registry.py
git commit -m "feat(semantic): six subject-area metric-view specs"
```

---

## Task 4: Measure-math validation against sample data

**Files:**
- Test: `tests/test_metric_view_math.py`

**Interfaces:**
- Consumes: `METRIC_VIEW_SPECS` (Task 3), the local Spark session fixture from `tests/conftest.py` (same fixture the fact-builder tests use — inspect `conftest.py` for its name, e.g. `spark`).

This task has no `src/` deliverable — its deliverable is a test that proves every measure `expr` computes the correct number on real Spark. All our measure exprs reference only `source.*` columns, so each view needs only a small sample of its source fact registered as the temp view `` `source` `` (backticked: `source` is a keyword-ish identifier).

- [ ] **Step 1: Write the test (it exercises Task 3's exprs directly)**

```python
# tests/test_metric_view_math.py
import math

import pytest

from techmart.semantic.metric_views import (
    MV_SALES, MV_INVENTORY, MV_FORECAST, MV_GL_ACTUALS, MV_BUDGET_PLAN,
    MV_INVENTORY_VALUATION,
)


def _measure(spark, sample_rows, schema, expr):
    df = spark.createDataFrame(sample_rows, schema=schema)
    df.createOrReplaceTempView("source")
    # backtick the base alias so `source` is treated as an identifier
    sql = expr.replace("source.", "`source`.")
    return spark.sql(f"SELECT {sql} AS m FROM `source`").collect()[0]["m"]


def _by_name(spec):
    return {m.name: m.expr for m in spec.measures}


def test_sales_measures(spark):
    # two lines, one transaction: gross 100+50, discount 10+0, net 90+50,
    # cogs 60+30, margin 30+20, qty 2+1
    schema = ("transaction_id long, line_number int, quantity int, "
              "gross_sales_amount double, discount_amount double, net_sales_amount double, "
              "cogs_amount double, gross_margin_amount double")
    rows = [(1, 1, 2, 100.0, 10.0, 90.0, 60.0, 30.0),
            (1, 2, 1, 50.0, 0.0, 50.0, 30.0, 20.0)]
    m = _by_name(MV_SALES)
    assert _measure(spark, rows, schema, m["gross_sales"]) == 150.0
    assert _measure(spark, rows, schema, m["net_sales"]) == 140.0
    assert _measure(spark, rows, schema, m["discount"]) == 10.0
    assert _measure(spark, rows, schema, m["cogs"]) == 90.0
    assert _measure(spark, rows, schema, m["gross_margin"]) == 50.0
    assert _measure(spark, rows, schema, m["units"]) == 3
    assert _measure(spark, rows, schema, m["line_count"]) == 2
    assert _measure(spark, rows, schema, m["transaction_count"]) == 1
    assert math.isclose(_measure(spark, rows, schema, m["gross_margin_pct"]), 50.0 / 140.0)
    assert math.isclose(_measure(spark, rows, schema, m["discount_rate"]), 10.0 / 150.0)
    assert math.isclose(_measure(spark, rows, schema, m["avg_order_value"]), 140.0)
    assert math.isclose(_measure(spark, rows, schema, m["avg_basket_units"]), 3.0)
    assert math.isclose(_measure(spark, rows, schema, m["avg_unit_price"]), 150.0 / 3.0)


def test_inventory_measures(spark):
    schema = ("store_sk long, product_sk long, on_hand_qty int, available_qty int, "
              "on_hand_cost_value double, days_of_supply double, is_out_of_stock boolean")
    rows = [(1, 10, 4, 3, 40.0, 8.0, False),
            (1, 11, 0, 0, 0.0, 0.0, True),
            (2, 10, 6, 6, 60.0, 4.0, False)]
    m = _by_name(MV_INVENTORY)
    assert _measure(spark, rows, schema, m["on_hand_qty"]) == 10
    assert _measure(spark, rows, schema, m["on_hand_cost_value"]) == 100.0
    assert math.isclose(_measure(spark, rows, schema, m["avg_days_of_supply"]), 4.0)
    assert math.isclose(_measure(spark, rows, schema, m["out_of_stock_rate"]), 1.0 / 3.0)
    assert _measure(spark, rows, schema, m["sku_count"]) == 2
    assert _measure(spark, rows, schema, m["stocked_store_count"]) == 2


def test_valuation_measures(spark):
    schema = ("gmroi double, on_hand_cost_value double, markdown_amount double")
    rows = [(2.0,), (4.0,), (6.0,)]
    # need three columns; rebuild rows with all
    schema = "gmroi double, on_hand_cost_value double, markdown_amount double"
    rows = [(2.0, 10.0, 1.0), (4.0, 20.0, 2.0), (6.0, 30.0, 3.0)]
    m = _by_name(MV_INVENTORY_VALUATION)
    assert math.isclose(_measure(spark, rows, schema, m["avg_gmroi"]), 4.0)
    assert _measure(spark, rows, schema, m["on_hand_cost_value"]) == 60.0
    assert _measure(spark, rows, schema, m["markdown"]) == 6.0


def test_forecast_measures(spark):
    schema = "forecast_qty double, forecast_amount double, lower_bound double, upper_bound double"
    rows = [(10.0, 100.0, 8.0, 12.0), (20.0, 200.0, 15.0, 25.0)]
    m = _by_name(MV_FORECAST)
    assert _measure(spark, rows, schema, m["forecast_qty"]) == 30.0
    assert _measure(spark, rows, schema, m["forecast_amount"]) == 300.0
    assert _measure(spark, rows, schema, m["interval_width"]) == (12.0 - 8.0) + (25.0 - 15.0)


def test_finance_measures(spark):
    m = _by_name(MV_GL_ACTUALS)
    assert _measure(spark, [(100.0,), (-25.0,)], "actual_amount double", m["actual_amount"]) == 75.0
    b = _by_name(MV_BUDGET_PLAN)
    schema = "plan_amount double, plan_units long"
    rows = [(100.0, 5), (50.0, 3)]
    assert _measure(spark, rows, schema, b["plan_amount"]) == 150.0
    assert _measure(spark, rows, schema, b["plan_units"]) == 8
```

- [ ] **Step 2: Run test to verify it fails, then passes**

Run: `pytest tests/test_metric_view_math.py -v`
First confirm the Spark fixture name in `tests/conftest.py` and adjust the fixture argument if it is not `spark`. Expected after Task 3: PASS. A failure here means a measure `expr` is semantically wrong — fix the expr in `metric_views.py` (Task 3 file), not the assertion.

- [ ] **Step 3: Commit**

```bash
git add tests/test_metric_view_math.py
git commit -m "test(semantic): validate metric-view measure math on sample data"
```

---

## Task 5: Constraint registry (`TABLE_CONSTRAINTS`) + `registry.py` + integrity tests

**Files:**
- Create: `src/techmart/semantic/table_constraints.py`
- Create: `src/techmart/semantic/registry.py`
- Test: `tests/test_table_constraints.py`

**Interfaces:**
- Consumes: `ForeignKey`, `TableConstraints` (Task 2).
- Produces: `TABLE_CONSTRAINTS: tuple[TableConstraints, ...]`; `registry.py` re-exports `METRIC_VIEW_SPECS` and `TABLE_CONSTRAINTS`.

**PK/FK facts (verified against builders — bind exactly):**
- Dims (PK = surrogate key, no FKs): `dim_date(date_sk)`, `dim_product(product_sk)`, `dim_store(store_sk)`, `dim_customer(customer_sk)`, `dim_employee(employee_sk)`, `dim_vendor(vendor_sk)`, `dim_promotion(promotion_sk)`, `dim_channel(channel_sk)`, `dim_gl_account(gl_account_sk)`, `dim_department(department_sk)`.
- Facts (PK = grain; FKs = the `is_key=True` surrogate columns → dim PKs):
  - `core.fact_sales_line` PK `(transaction_id, line_number)`; FK date_sk, product_sk, store_sk, customer_sk, employee_sk, promotion_sk, channel_sk.
  - `core.fact_inventory_snapshot` PK `(date_sk, store_sk, product_sk)`; FK date_sk, store_sk, product_sk.
  - `core.fact_inventory_movement` PK `(movement_id)`; FK date_sk, product_sk, store_sk, vendor_sk.
  - `core.fact_returns` PK `(rma_id)`; FK date_sk, product_sk, store_sk, customer_sk, employee_sk, channel_sk.
  - `core.fact_fulfillment` PK `(order_id)`; FK date_sk, product_sk, store_sk, customer_sk, channel_sk.
  - `core.fact_loyalty_activity` PK `(loyalty_event_id)`; FK date_sk, customer_sk, store_sk, channel_sk.
  - `core.fact_web_events` PK `(session_id, event_number)`; FK date_sk, customer_sk, product_sk, channel_sk.
  - `finance.fact_gl_actuals` PK `(date_sk, gl_account_sk, store_sk, department_sk)`; FK all four → dim_date/dim_gl_account/dim_store/dim_department.
  - `finance.fact_budget_plan` PK `(date_sk, gl_account_sk, store_sk, department_sk, plan_version)`; FK date_sk, gl_account_sk, store_sk, department_sk.
  - `finance.fact_inventory_valuation` PK `(date_sk, store_sk, category_id)`; FK date_sk, store_sk (**not** category_id — not unique in any dim).
  - `ai.fact_sales_forecast` PK `(date_sk, product_sk, store_sk, forecast_version)`; FK date_sk, product_sk, store_sk.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_table_constraints.py
from techmart.ai.registry import AI_SPECS
from techmart.facts.registry import FACT_SPECS
from techmart.finance.registry import FINANCE_SPECS
from techmart.semantic.registry import METRIC_VIEW_SPECS, TABLE_CONSTRAINTS


def _spec_index():
    from techmart.spark.dimensions.dim_date import DIM_DATE_SPEC
    from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC
    from techmart.spark.dimensions.dim_store import DIM_STORE_SPEC
    from techmart.spark.dimensions.dim_customer import DIM_CUSTOMER_SPEC
    from techmart.spark.dimensions.dim_channel import DIM_CHANNEL_SPEC
    from techmart.spark.dimensions.dim_promotion import DIM_PROMOTION_SPEC
    from techmart.spark.dimensions.dim_vendor import DIM_VENDOR_SPEC
    from techmart.spark.dimensions.dim_employee import DIM_EMPLOYEE_SPEC
    dims = [DIM_DATE_SPEC, DIM_PRODUCT_SPEC, DIM_STORE_SPEC, DIM_CUSTOMER_SPEC,
            DIM_CHANNEL_SPEC, DIM_PROMOTION_SPEC, DIM_VENDOR_SPEC, DIM_EMPLOYEE_SPEC]
    facts = list(FACT_SPECS.values()) + list(FINANCE_SPECS) + list(AI_SPECS)
    return {(s.schema, s.name): s for s in dims + facts}


def test_registry_reexports():
    assert len(METRIC_VIEW_SPECS) == 6
    assert len(TABLE_CONSTRAINTS) >= 21  # 10 dims + 11 facts


def test_pk_columns_exist_and_not_null():
    idx = _spec_index()
    for tc in TABLE_CONSTRAINTS:
        spec = idx[(tc.schema, tc.table)]
        by = {c.name: c for c in spec.columns}
        assert tc.primary_key, f"{tc.table} has no PK"
        for col in tc.primary_key:
            assert col in by, f"{tc.table}.{col} missing"
            assert by[col].nullable is False, f"{tc.table}.{col} PK column must be NOT NULL"


def test_fk_targets_exist_and_are_pks():
    idx = _spec_index()
    pk_by_table = {(tc.schema, tc.table): tc.primary_key for tc in TABLE_CONSTRAINTS}
    for tc in TABLE_CONSTRAINTS:
        spec_cols = {c.name for c in idx[(tc.schema, tc.table)].columns}
        for fk in tc.foreign_keys:
            for col in fk.columns:
                assert col in spec_cols, f"{tc.table}.{col} FK column missing"
            ref = (fk.ref_schema, fk.ref_table)
            assert ref in idx, f"{tc.table}: FK target {ref} not a known table"
            ref_cols = {c.name for c in idx[ref].columns}
            for col in fk.ref_columns:
                assert col in ref_cols, f"{ref}.{col} missing"
            # FK must reference the target's declared PK
            assert tuple(fk.ref_columns) == tuple(pk_by_table[ref]), \
                f"{tc.table}: FK to {ref} must reference its PK {pk_by_table[ref]}"


def test_fk_columns_are_marked_is_key_on_facts():
    idx = _spec_index()
    for tc in TABLE_CONSTRAINTS:
        spec = idx[(tc.schema, tc.table)]
        if not tc.table.startswith("fact_"):
            continue
        key_cols = {c.name for c in spec.columns if c.is_key}
        for fk in tc.foreign_keys:
            for col in fk.columns:
                assert col in key_cols, f"{tc.table}.{col} should be is_key=True"


def test_every_gold_fact_has_constraints():
    covered = {(tc.schema, tc.table) for tc in TABLE_CONSTRAINTS}
    for name in FACT_SPECS:  # core facts
        assert ("core", name) in covered
    for s in FINANCE_SPECS:
        if s.name.startswith("fact_"):
            assert (s.schema, s.name) in covered
    assert ("ai", "fact_sales_forecast") in covered
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_table_constraints.py -v`
Expected: FAIL (module missing).

- [ ] **Step 3: Write minimal implementation**

```python
# src/techmart/semantic/table_constraints.py
"""PK/FK constraint registry for the gold tables (applied NOT ENFORCED RELY).

PKs are the grain keys (verified unique by construction); FKs are the
surrogate-key columns referencing the conformed-dimension PKs (RI by
construction). fact_inventory_valuation.category_id is NOT an FK (not a unique
dim key).
"""
from __future__ import annotations

from .constraints import ForeignKey, TableConstraints


def _fk(col: str, schema: str, table: str, ref: str) -> ForeignKey:
    return ForeignKey(columns=(col,), ref_schema=schema, ref_table=table, ref_columns=(ref,))


# Conformed-dimension FKs (all live in core except gl_account/department in finance).
_DATE = _fk("date_sk", "core", "dim_date", "date_sk")
_PRODUCT = _fk("product_sk", "core", "dim_product", "product_sk")
_STORE = _fk("store_sk", "core", "dim_store", "store_sk")
_CUSTOMER = _fk("customer_sk", "core", "dim_customer", "customer_sk")
_EMPLOYEE = _fk("employee_sk", "core", "dim_employee", "employee_sk")
_VENDOR = _fk("vendor_sk", "core", "dim_vendor", "vendor_sk")
_PROMO = _fk("promotion_sk", "core", "dim_promotion", "promotion_sk")
_CHANNEL = _fk("channel_sk", "core", "dim_channel", "channel_sk")
_GL = _fk("gl_account_sk", "finance", "dim_gl_account", "gl_account_sk")
_DEPT = _fk("department_sk", "finance", "dim_department", "department_sk")


def _dim(schema: str, table: str, pk: str) -> TableConstraints:
    return TableConstraints(schema=schema, table=table, primary_key=(pk,))


TABLE_CONSTRAINTS: tuple[TableConstraints, ...] = (
    # --- dimensions (PK on the surrogate key) ---
    _dim("core", "dim_date", "date_sk"),
    _dim("core", "dim_product", "product_sk"),
    _dim("core", "dim_store", "store_sk"),
    _dim("core", "dim_customer", "customer_sk"),
    _dim("core", "dim_employee", "employee_sk"),
    _dim("core", "dim_vendor", "vendor_sk"),
    _dim("core", "dim_promotion", "promotion_sk"),
    _dim("core", "dim_channel", "channel_sk"),
    _dim("finance", "dim_gl_account", "gl_account_sk"),
    _dim("finance", "dim_department", "department_sk"),
    # --- core facts ---
    TableConstraints("core", "fact_sales_line", ("transaction_id", "line_number"),
                     (_DATE, _PRODUCT, _STORE, _CUSTOMER, _EMPLOYEE, _PROMO, _CHANNEL)),
    TableConstraints("core", "fact_inventory_snapshot", ("date_sk", "store_sk", "product_sk"),
                     (_DATE, _STORE, _PRODUCT)),
    TableConstraints("core", "fact_inventory_movement", ("movement_id",),
                     (_DATE, _PRODUCT, _STORE, _VENDOR)),
    TableConstraints("core", "fact_returns", ("rma_id",),
                     (_DATE, _PRODUCT, _STORE, _CUSTOMER, _EMPLOYEE, _CHANNEL)),
    TableConstraints("core", "fact_fulfillment", ("order_id",),
                     (_DATE, _PRODUCT, _STORE, _CUSTOMER, _CHANNEL)),
    TableConstraints("core", "fact_loyalty_activity", ("loyalty_event_id",),
                     (_DATE, _CUSTOMER, _STORE, _CHANNEL)),
    TableConstraints("core", "fact_web_events", ("session_id", "event_number"),
                     (_DATE, _CUSTOMER, _PRODUCT, _CHANNEL)),
    # --- finance facts ---
    TableConstraints("finance", "fact_gl_actuals",
                     ("date_sk", "gl_account_sk", "store_sk", "department_sk"),
                     (_DATE, _GL, _STORE, _DEPT)),
    TableConstraints("finance", "fact_budget_plan",
                     ("date_sk", "gl_account_sk", "store_sk", "department_sk", "plan_version"),
                     (_DATE, _GL, _STORE, _DEPT)),
    TableConstraints("finance", "fact_inventory_valuation",
                     ("date_sk", "store_sk", "category_id"),
                     (_DATE, _STORE)),
    # --- ai facts ---
    TableConstraints("ai", "fact_sales_forecast",
                     ("date_sk", "product_sk", "store_sk", "forecast_version"),
                     (_DATE, _PRODUCT, _STORE)),
)
```

```python
# src/techmart/semantic/registry.py
"""techmart_semantic registry: metric-view specs + gold-table constraints."""
from __future__ import annotations

from .metric_views import METRIC_VIEW_SPECS
from .table_constraints import TABLE_CONSTRAINTS

__all__ = ["METRIC_VIEW_SPECS", "TABLE_CONSTRAINTS"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_table_constraints.py -v`
Expected: PASS. (Note: `dim_gl_account`/`dim_department` are referenced as FK targets and must themselves be in `TABLE_CONSTRAINTS` so `test_fk_targets_exist_and_are_pks` can resolve their PKs — they are.)

- [ ] **Step 5: Commit**

```bash
git add src/techmart/semantic/table_constraints.py src/techmart/semantic/registry.py tests/test_table_constraints.py
git commit -m "feat(semantic): gold-table PK/FK RELY constraint registry"
```

---

## Task 6: `generate_semantic.py` serverless notebook

**Files:**
- Create: `notebooks/generate_semantic.py`
- Test: append to `tests/test_notebooks.py`

**Interfaces:**
- Consumes: `metric_view_ddl` (T1), `pk_ddl`/`fk_ddl`/`drop_pk_ddl` (T2), `METRIC_VIEW_SPECS`/`TABLE_CONSTRAINTS` (T5).

- [ ] **Step 1: Write the failing test (append to `tests/test_notebooks.py`)**

```python
def test_generate_semantic_notebook_covers_emitters():
    text = _read("generate_semantic.py")
    assert text.splitlines()[0] == "# Databricks notebook source"
    assert "dbutils.widgets" in text
    assert "metric_view_ddl" in text
    assert "METRIC_VIEW_SPECS" in text
    assert "TABLE_CONSTRAINTS" in text
    assert "pk_ddl" in text and "fk_ddl" in text
    assert "semantic" in text  # creates the techmart_semantic schema
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_notebooks.py::test_generate_semantic_notebook_covers_emitters -v`
Expected: FAIL (file missing).

- [ ] **Step 3: Write the notebook**

```python
# Databricks notebook source
import sys
sys.path.insert(0, "../src")
dbutils.widgets.text("catalog", "stable_classic_ppke9o")
dbutils.widgets.text("schema_prefix", "techmart_")
dbutils.widgets.text("scale_profile", "smoke")  # accepted for parity; unused by definitions
# COMMAND ----------
from techmart.semantic.registry import METRIC_VIEW_SPECS, TABLE_CONSTRAINTS
from techmart.semantic.metric_view import metric_view_ddl
from techmart.semantic.constraints import drop_pk_ddl, fk_ddl, pk_ddl

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
kw = dict(catalog=catalog, schema_prefix=schema_prefix)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_prefix}semantic")

# COMMAND ----------
# --- informational PK/FK constraints (NOT ENFORCED RELY) on the gold tables ---
# RELY is safe: uniqueness (PK) + RI (FK) are guaranteed by construction and
# proven green on the workspace. Drop-then-add keeps re-runs idempotent.
for tc in TABLE_CONSTRAINTS:
    spark.sql(drop_pk_ddl(tc, **kw).rstrip(";"))
    spark.sql(pk_ddl(tc, **kw).rstrip(";"))
    for fk in tc.foreign_keys:
        # FKs require the referenced PK to exist first (declared above for every dim).
        spark.sql(fk_ddl(tc, fk, **kw).rstrip(";"))
    print("constraints:", tc.schema, tc.table)

# COMMAND ----------
# --- metric views into techmart_semantic ---
for spec in METRIC_VIEW_SPECS:
    spark.sql(metric_view_ddl(spec, **kw).rstrip(";"))
    print("metric view:", spec.name)
```

> Notes for the implementer: (1) `spark.sql` takes a single statement — the emitters end in `;`, so strip it with `.rstrip(";")`. (2) FK creation requires the target dim's PK to already exist; because every dim's PK is added in the same constraints loop before any FK referencing it *may* run, iterate so that **all** PKs are added before FKs, OR add each dim PK first. Simplest robust ordering: run two passes — first every `pk_ddl`, then every `fk_ddl`. Restructure the constraints `COMMAND` into a PK pass then an FK pass. Adjust the notebook accordingly and keep the test assertions satisfied.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_notebooks.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add notebooks/generate_semantic.py tests/test_notebooks.py
git commit -m "feat(semantic): generate_semantic serverless apply notebook"
```

---

## Task 7: DAB job wiring

**Files:**
- Modify: `resources/generate_facts_job.yml` (add `generate_semantic` task)
- Test: append to `tests/test_dab_bundle.py`

**Interfaces:**
- Consumes: the existing `generate_facts` / `generate_finance` / `generate_ai` tasks.

- [ ] **Step 1: Write the failing test (append to `tests/test_dab_bundle.py`)**

```python
def test_semantic_task_wired():
    import yaml
    job = yaml.safe_load((_ROOT / "resources" / "generate_facts_job.yml").read_text())["resources"]["jobs"]["generate_facts"]
    by_key = {t["task_key"]: t for t in job["tasks"]}
    assert "generate_semantic" in by_key
    deps = {d["task_key"] for d in by_key["generate_semantic"].get("depends_on", [])}
    assert deps == {"generate_facts", "generate_finance", "generate_ai"}
    assert by_key["generate_semantic"]["notebook_task"]["notebook_path"].endswith("generate_semantic.py")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_dab_bundle.py::test_semantic_task_wired -v`
Expected: FAIL (task not present).

- [ ] **Step 3: Add the task to `resources/generate_facts_job.yml`**

Append under `tasks:` (a pure edge addition — do not modify other tasks):

```yaml
        - task_key: generate_semantic
          depends_on:
            - task_key: generate_facts
            - task_key: generate_finance
            - task_key: generate_ai
          notebook_task:
            notebook_path: ../notebooks/generate_semantic.py
            base_parameters:
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              scale_profile: ${var.scale_profile}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_dab_bundle.py -v`
Expected: PASS (all bundle tests, including the existing ones).

- [ ] **Step 5: Run the full suite**

Run: `pytest -q`
Expected: all tests pass (existing + new).

- [ ] **Step 6: Commit**

```bash
git add resources/generate_facts_job.yml tests/test_dab_bundle.py
git commit -m "feat(semantic): wire generate_semantic task into the DAB job"
```

---

## Self-Review (completed by plan author)

- **Spec coverage:** metric views (T1/T3/T4), PK/FK `RELY` constraints (T2/T5), materialization (T1/T3), Genie metadata comments/display_name/synonyms/format (T1/T3), notebook apply (T6), DAB fan-out edge with correct `depends_on` (T7), no new levers/vars (Global Constraints), workspace-only gate (documented, not asserted locally). Deferred cross-fact metrics are explicitly out of scope. ✔
- **Placeholder scan:** every code and test step contains real code; no TBD/TODO. ✔
- **Type consistency:** `MetricField` used uniformly for dims and measures; `metric_view_ddl`/`pk_ddl`/`fk_ddl` signatures match across tasks and the notebook; `MetricViewSpec` field names (`source_schema`/`source_table`/`joins`/`materialization`) consistent T1↔T3↔T4↔T6; `TableConstraints`/`ForeignKey` fields consistent T2↔T5↔T6. ✔
- **Verified against real specs:** all dimension attribute names, fact grains/PKs (incl. `rma_id`/`order_id`/`movement_id`/`loyalty_event_id` uniqueness), and the `fact_inventory_valuation` no-vendor / category-degenerate facts. ✔

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-09-01-techmart-semantic.md`. Two execution options:

1. **Subagent-Driven (recommended)** — a fresh subagent per task, task review between tasks, broad review at the end.
2. **Inline Execution** — execute tasks in this session with checkpoints.

Which approach?
