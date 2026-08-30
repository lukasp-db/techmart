# Techmart Phase 2A — Foundation Hardening & Reference Data — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the Techmart generation framework and add the shared machinery + reference data that the Phase 2B dimension builders depend on — before any second builder copies Phase 1's patterns.

**Architecture:** Extends the existing `techmart` package. Adds (1) dtype-aware schema resolution/validation on `TableSpec`, (2) a reusable SCD2 scaffolding helper, (3) a registry that decouples table dispatch from the CLI, (4) entity-count sizing in the scale profiles, and (5) a curated electronics product taxonomy as package reference data. No new external dependencies.

**Tech Stack:** Python ≥ 3.10, Polars, NumPy, PyYAML, pytest (all already in use).

## Global Constraints

_Every task's requirements implicitly include this section._

- **Language/floor:** Python ≥ 3.10.
- **Naming:** `snake_case`, aligned to the Databricks retail industry model v2. Surrogate keys `*_sk` (Int64); business keys `*_id`.
- **Comments:** every table and every column carries a human-readable comment on its `Column`/`TableSpec`.
- **Determinism:** all generation is seeded and reproducible; the same config + seed produces identical output. Reference data (taxonomy) is deterministic (fixed authored content, ids assigned by stable enumeration order).
- **Config-driven scale:** `scale_profile` ∈ {`demo_lean`, `showcase` (default), `stress`}.
- **SCD2 policy (Phase 2):** dimensions emit **current rows only** with SCD2 scaffolding columns present (`is_current=true`, `effective_start_ts` at history start, `effective_end_ts=null`, `version=1`). Real historical versioning is deferred to a later phase.
- **Secret-free:** no workspace URLs, tokens, or account identifiers in code or committed files.
- **Serverless-compatible:** pure Python + Polars; no classic-cluster-only features.
- **Schemas:** internal schema names are unprefixed (`core`, `finance`, …); the `techmart_` prefix is applied only at deploy time (not this phase).

---

### Task 1: Dtype-aware schema resolution & validation

**Files:**
- Modify: `src/techmart/framework/schema.py` (add `polars_schema` method to `TableSpec`)
- Modify: `src/techmart/framework/writer.py` (extend `validate_schema` to check dtypes)
- Modify: `src/techmart/dimensions/dim_date.py` (use `DIM_DATE_SPEC.polars_schema()`)
- Test: `tests/test_writer.py` (extend)

**Interfaces:**
- Consumes: existing `Column`, `TableSpec`, `write_table`.
- Produces:
  - `TableSpec.polars_schema() -> dict[str, PolarsDataType]` — maps each column name to its Polars dtype via `getattr(polars, column.dtype)`. Uses a lazy `import polars` inside the method so `schema.py` keeps no module-level Polars import.
  - `validate_schema(df, spec)` now raises `SchemaMismatchError` on **either** a column-name mismatch (as before) **or** a dtype mismatch.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_writer.py`)

```python
def test_polars_schema_maps_declared_dtypes():
    schema = SPEC.polars_schema()
    assert schema == {"demo_sk": pl.Int64, "label": pl.Utf8}


def test_validate_schema_rejects_wrong_dtype():
    # Right column names, wrong dtype for demo_sk (Utf8 instead of Int64).
    df = pl.DataFrame({"demo_sk": ["1", "2"], "label": ["a", "b"]})
    with pytest.raises(SchemaMismatchError):
        validate_schema(df, SPEC)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_writer.py -v`
Expected: `test_polars_schema_maps_declared_dtypes` FAILS (`AttributeError: 'TableSpec' object has no attribute 'polars_schema'`); `test_validate_schema_rejects_wrong_dtype` FAILS (no error raised — old validate_schema checks names only).

- [ ] **Step 3: Add `polars_schema` to `TableSpec`** (`src/techmart/framework/schema.py`)

Add this method inside the `TableSpec` class (after the `column_names` property):

```python
    def polars_schema(self) -> dict:
        """Map each column name to its Polars dtype (resolved from the dtype name)."""
        import polars as pl

        return {c.name: getattr(pl, c.dtype) for c in self.columns}
```

- [ ] **Step 4: Extend `validate_schema`** (`src/techmart/framework/writer.py`)

Replace the body of `validate_schema` with:

```python
def validate_schema(df: pl.DataFrame, spec: TableSpec) -> None:
    if df.columns != spec.column_names:
        raise SchemaMismatchError(
            f"{spec.name}: expected columns {spec.column_names}, got {df.columns}"
        )
    expected = spec.polars_schema()
    mismatches = [
        (name, str(df.schema[name]), str(dtype))
        for name, dtype in expected.items()
        if df.schema[name] != dtype
    ]
    if mismatches:
        raise SchemaMismatchError(f"{spec.name}: dtype mismatch (name, actual, expected): {mismatches}")
```

- [ ] **Step 5: Refactor `build_dim_date` to use `polars_schema()`** (`src/techmart/dimensions/dim_date.py`)

In `build_dim_date`, replace the inline `getattr` schema construction (the `schema = {col.name: getattr(pl, col.dtype) for col in DIM_DATE_SPEC.columns}` line) with:

```python
    return pl.DataFrame(rows, schema=DIM_DATE_SPEC.polars_schema()).select(
        DIM_DATE_SPEC.column_names
    )
```

Ensure no other reference to a local `schema` variable remains in the function.

- [ ] **Step 6: Run the affected tests to verify they pass**

Run: `python -m pytest tests/test_writer.py tests/test_dim_date.py tests/test_cli.py -v`
Expected: PASS (existing writer tests + 2 new + all dim_date + all cli tests). The existing `test_write_table_roundtrips` still passes because its DataFrame dtypes (Int64, Utf8) match `SPEC`.

- [ ] **Step 7: Commit**

```bash
git add src/techmart/framework/schema.py src/techmart/framework/writer.py src/techmart/dimensions/dim_date.py tests/test_writer.py
git commit -m "feat: dtype-aware schema resolution and validation in the framework"
```

---

### Task 2: SCD2 scaffolding helper

**Files:**
- Create: `src/techmart/framework/scd2.py`
- Test: `tests/test_scd2.py`

**Interfaces:**
- Consumes: `Column` from `techmart.framework.schema`.
- Produces:
  - `scd2_columns() -> list[Column]` — the four SCD2 columns in this exact order: `effective_start_ts` (Datetime, not null), `effective_end_ts` (Datetime, nullable), `is_current` (Boolean, not null), `version` (Int64, not null). Dimension specs append these to their own columns.
  - `with_scd2_current(df: polars.DataFrame, start: datetime.date) -> polars.DataFrame` — appends the four columns for **current** rows: `effective_start_ts` = midnight of `start`, `effective_end_ts` = null, `is_current` = True, `version` = 1.

- [ ] **Step 1: Write the failing tests**

`tests/test_scd2.py`:
```python
from datetime import date, datetime

import polars as pl

from techmart.framework.scd2 import scd2_columns, with_scd2_current


def test_scd2_columns_names_and_order():
    assert [c.name for c in scd2_columns()] == [
        "effective_start_ts",
        "effective_end_ts",
        "is_current",
        "version",
    ]


def test_scd2_columns_carry_comments():
    assert all(c.comment for c in scd2_columns())


def test_with_scd2_current_appends_current_version():
    df = pl.DataFrame({"product_sk": [1, 2, 3]})
    out = with_scd2_current(df, date(2023, 1, 31))
    assert out.columns == [
        "product_sk",
        "effective_start_ts",
        "effective_end_ts",
        "is_current",
        "version",
    ]
    assert out["is_current"].to_list() == [True, True, True]
    assert out["version"].to_list() == [1, 1, 1]
    assert out["effective_end_ts"].null_count() == 3
    assert out["effective_start_ts"].to_list() == [datetime(2023, 1, 31)] * 3


def test_with_scd2_current_dtypes():
    out = with_scd2_current(pl.DataFrame({"x": [1]}), date(2023, 1, 31))
    assert out.schema["effective_start_ts"] == pl.Datetime
    assert out.schema["effective_end_ts"] == pl.Datetime
    assert out.schema["is_current"] == pl.Boolean
    assert out.schema["version"] == pl.Int64
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_scd2.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.framework.scd2'`.

- [ ] **Step 3: Write the implementation**

`src/techmart/framework/scd2.py`:
```python
from __future__ import annotations

from datetime import date, datetime

import polars as pl

from .schema import Column


def scd2_columns() -> list[Column]:
    """The four SCD Type 2 control columns appended to every SCD2 dimension."""
    return [
        Column("effective_start_ts", "Datetime", "SCD2 effective start timestamp", nullable=False),
        Column("effective_end_ts", "Datetime", "SCD2 effective end timestamp; null when current"),
        Column("is_current", "Boolean", "True for the current version of the row", nullable=False),
        Column("version", "Int64", "SCD2 version number (1-based)", nullable=False),
    ]


def with_scd2_current(df: pl.DataFrame, start: date) -> pl.DataFrame:
    """Append SCD2 columns marking every row as the current (version 1) record."""
    start_ts = datetime(start.year, start.month, start.day)
    return df.with_columns(
        pl.lit(start_ts, dtype=pl.Datetime).alias("effective_start_ts"),
        pl.lit(None, dtype=pl.Datetime).alias("effective_end_ts"),
        pl.lit(True, dtype=pl.Boolean).alias("is_current"),
        pl.lit(1, dtype=pl.Int64).alias("version"),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_scd2.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/framework/scd2.py tests/test_scd2.py
git commit -m "feat: add SCD2 current-version scaffolding helper"
```

---

### Task 3: Registry-based table dispatch

**Files:**
- Create: `src/techmart/registry.py`
- Modify: `src/techmart/cli.py` (dispatch via the registry)
- Test: `tests/test_registry.py`

**Interfaces:**
- Consumes: `TableSpec` from `techmart.framework.schema`; `TechmartConfig` from `techmart.config`; `DIM_DATE_SPEC`, `build_dim_date` from `techmart.dimensions.dim_date`.
- Produces:
  - `TableBuilder` (frozen dataclass): `spec: TableSpec`, `build: Callable[[TechmartConfig], polars.DataFrame]`.
  - `REGISTRY: dict[str, TableBuilder]` keyed by table name; contains `dim_date` this phase. (Phase 2B adds entries here as each dimension lands.)
- Modifies: `generate(config, tables)` now dispatches through `REGISTRY`; unknown table names raise `ValueError`. `cli.py` no longer imports `dim_date` directly.

- [ ] **Step 1: Write the failing tests**

`tests/test_registry.py`:
```python
from datetime import date
from pathlib import Path

from techmart.config import load_config
from techmart.dimensions.dim_date import DIM_DATE_SPEC
from techmart.framework.writer import validate_schema
from techmart.registry import REGISTRY, TableBuilder

PROFILES = Path("config/scale_profiles.yaml")


def test_registry_contains_dim_date():
    assert "dim_date" in REGISTRY
    assert isinstance(REGISTRY["dim_date"], TableBuilder)
    assert REGISTRY["dim_date"].spec is DIM_DATE_SPEC


def test_registry_builder_produces_conforming_dataframe():
    cfg = load_config(PROFILES, "demo_lean", end_date=date(2026, 1, 31))
    df = REGISTRY["dim_date"].build(cfg)
    validate_schema(df, DIM_DATE_SPEC)  # no raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.registry'`.

- [ ] **Step 3: Write the registry**

`src/techmart/registry.py`:
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from .config import TechmartConfig
from .dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from .framework.schema import TableSpec


@dataclass(frozen=True)
class TableBuilder:
    spec: TableSpec
    build: Callable[[TechmartConfig], pl.DataFrame]


def _build_dim_date(config: TechmartConfig) -> pl.DataFrame:
    return build_dim_date(config.start_date, config.end_date)


REGISTRY: dict[str, TableBuilder] = {
    DIM_DATE_SPEC.name: TableBuilder(spec=DIM_DATE_SPEC, build=_build_dim_date),
}
```

- [ ] **Step 4: Refactor the CLI to dispatch via the registry** (`src/techmart/cli.py`)

Replace the current imports and `generate` function so `generate` uses `REGISTRY` and `cli.py` no longer imports `dim_date`. The imports at the top of `cli.py` become:

```python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import TechmartConfig, load_config
from .framework.writer import write_table
from .registry import REGISTRY
```

And the `generate` function becomes:

```python
def generate(config: TechmartConfig, tables: list[str]) -> list[Path]:
    written: list[Path] = []
    for table in tables:
        try:
            builder = REGISTRY[table]
        except KeyError:
            raise ValueError(f"Unknown table: {table!r}")
        df = builder.build(config)
        written.append(write_table(df, builder.spec, config.output_dir))
    return written
```

Leave `main()` unchanged.

- [ ] **Step 5: Run the affected tests to verify they pass**

Run: `python -m pytest tests/test_registry.py tests/test_cli.py -v`
Expected: PASS. The existing CLI tests still pass — `generate` still writes `dim_date` and still raises `ValueError` for unknown tables (now via the registry `KeyError` → `ValueError`).

- [ ] **Step 6: Commit**

```bash
git add src/techmart/registry.py src/techmart/cli.py tests/test_registry.py
git commit -m "feat: registry-based table dispatch, decoupled from the CLI"
```

---

### Task 4: Scale-profile entity sizing

**Files:**
- Modify: `config/scale_profiles.yaml` (add `num_customers`, `num_vendors`)
- Modify: `src/techmart/config.py` (add fields + derived counts)
- Test: `tests/test_config.py` (extend)

**Interfaces:**
- Consumes: existing `ScaleProfile`, `load_profiles`, `load_config`.
- Produces:
  - `ScaleProfile` gains fields `num_customers: int`, `num_vendors: int` (after the existing four).
  - Module constants `ASSOCIATES_PER_STORE = 40`, `CAMPAIGNS_PER_YEAR = 60`.
  - `ScaleProfile.num_employees -> int` property = `ASSOCIATES_PER_STORE * num_stores`.
  - `ScaleProfile.num_promotions -> int` property = `CAMPAIGNS_PER_YEAR * history_years`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_config.py`)

```python
def test_profiles_carry_customer_and_vendor_counts():
    cfg = load_config(PROFILES, "showcase")
    assert cfg.scale_profile.num_customers == 5_000_000
    assert cfg.scale_profile.num_vendors == 5_000


def test_derived_employee_and_promotion_counts():
    cfg = load_config(PROFILES, "showcase")  # 1000 stores, 3 years history
    assert cfg.scale_profile.num_employees == 40 * 1000
    assert cfg.scale_profile.num_promotions == 60 * 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL — `TypeError` on `ScaleProfile(... unexpected keyword 'num_customers')` (YAML now carries keys the dataclass lacks) and `AttributeError` for `num_employees`.

- [ ] **Step 3: Add the counts to the profiles** (`config/scale_profiles.yaml`)

Add `num_customers` and `num_vendors` to each profile so the file reads:

```yaml
# Techmart scale profiles. `showcase` is the demo default.
profiles:
  demo_lean:
    num_stores: 100
    num_skus: 20000
    history_years: 2
    sales_lines_target: 75000000
    num_customers: 500000
    num_vendors: 1000
  showcase:
    num_stores: 1000
    num_skus: 200000
    history_years: 3
    sales_lines_target: 750000000
    num_customers: 5000000
    num_vendors: 5000
  stress:
    num_stores: 2000
    num_skus: 500000
    history_years: 5
    sales_lines_target: 3000000000
    num_customers: 20000000
    num_vendors: 10000
```

- [ ] **Step 4: Extend `ScaleProfile`** (`src/techmart/config.py`)

Add the two module constants near the top (below `DEFAULT_PROFILE`):

```python
ASSOCIATES_PER_STORE = 40
CAMPAIGNS_PER_YEAR = 60
```

Extend the `ScaleProfile` dataclass to add the two fields and the two derived properties:

```python
@dataclass(frozen=True)
class ScaleProfile:
    name: str
    num_stores: int
    num_skus: int
    history_years: int
    sales_lines_target: int
    num_customers: int
    num_vendors: int

    @property
    def num_employees(self) -> int:
        """Derived associate headcount across all stores."""
        return ASSOCIATES_PER_STORE * self.num_stores

    @property
    def num_promotions(self) -> int:
        """Derived promotion/campaign count across the history window."""
        return CAMPAIGNS_PER_YEAR * self.history_years
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS (existing config tests + 2 new). `load_profiles`' `ScaleProfile(name=name, **cfg)` now consumes all six YAML keys.

- [ ] **Step 6: Commit**

```bash
git add config/scale_profiles.yaml src/techmart/config.py tests/test_config.py
git commit -m "feat: add customer/vendor counts and derived employee/promotion sizing"
```

---

### Task 5: Curated electronics product taxonomy

**Files:**
- Create: `src/techmart/reference/__init__.py`
- Create: `src/techmart/reference/taxonomy.py`
- Test: `tests/test_taxonomy.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (pure reference data).
- Produces:
  - Frozen dataclasses `Subcategory(id, name)`, `Category(id, name, subcategories: tuple[Subcategory, ...], brands: tuple[str, ...])`, `Department(id, name, categories: tuple[Category, ...])`, `Division(id, name, departments: tuple[Department, ...])`.
  - `TAXONOMY: tuple[Division, ...]` — the built, id-assigned taxonomy tree.
  - `subcategory_paths() -> list[tuple[Division, Department, Category, Subcategory]]` — every root-to-leaf path (used by Phase 2B's `dim_product` to assign SKUs).
  - Ids are assigned deterministically by enumeration order: `DIV{d:02d}`, `DEP{d:02d}{p:02d}`, `CAT{d:02d}{p:02d}{c:02d}`, `SUB{d:02d}{p:02d}{c:02d}{s:02d}` (all 1-based).

- [ ] **Step 1: Write the failing tests**

`tests/test_taxonomy.py`:
```python
from techmart.reference.taxonomy import (
    TAXONOMY,
    Category,
    Division,
    subcategory_paths,
)


def _all_ids():
    ids = []
    for div in TAXONOMY:
        ids.append(div.id)
        for dep in div.departments:
            ids.append(dep.id)
            for cat in dep.categories:
                ids.append(cat.id)
                for sub in cat.subcategories:
                    ids.append(sub.id)
    return ids


def test_taxonomy_is_nonempty_and_typed():
    assert len(TAXONOMY) >= 5
    assert all(isinstance(d, Division) for d in TAXONOMY)


def test_all_ids_unique():
    ids = _all_ids()
    assert len(ids) == len(set(ids))


def test_id_prefixes_by_level():
    div = TAXONOMY[0]
    assert div.id.startswith("DIV")
    assert div.departments[0].id.startswith("DEP")
    assert div.departments[0].categories[0].id.startswith("CAT")
    assert div.departments[0].categories[0].subcategories[0].id.startswith("SUB")


def test_every_category_has_subcategories_and_brands():
    for div in TAXONOMY:
        for dep in div.departments:
            for cat in dep.categories:
                assert isinstance(cat, Category)
                assert len(cat.subcategories) >= 1
                assert len(cat.brands) >= 1


def test_expected_divisions_present():
    names = {d.name for d in TAXONOMY}
    assert {"Computing", "Consumer Electronics", "Appliances", "Networking & DIY"} <= names


def test_subcategory_paths_cover_every_leaf():
    paths = subcategory_paths()
    leaf_count = sum(
        len(cat.subcategories)
        for div in TAXONOMY
        for dep in div.departments
        for cat in dep.categories
    )
    assert len(paths) == leaf_count
    div, dep, cat, sub = paths[0]
    assert isinstance(div, Division) and sub in cat.subcategories
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_taxonomy.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.reference'`.

- [ ] **Step 3: Write the reference package**

`src/techmart/reference/__init__.py`:
```python
"""Curated reference data for Techmart (product taxonomy, etc.)."""
```

`src/techmart/reference/taxonomy.py`:
```python
"""Curated electronics merchandise taxonomy for Techmart.

Authored as raw nested content (names only); ids are assigned deterministically
by enumeration order at import time. Brands are defined per department and shared
by that department's categories.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Subcategory:
    id: str
    name: str


@dataclass(frozen=True)
class Category:
    id: str
    name: str
    subcategories: tuple[Subcategory, ...]
    brands: tuple[str, ...]


@dataclass(frozen=True)
class Department:
    id: str
    name: str
    categories: tuple[Category, ...]


@dataclass(frozen=True)
class Division:
    id: str
    name: str
    departments: tuple[Department, ...]


# Raw authored content. Structure:
#   division -> department -> {"brands": [...], "categories": {category: [subcategories]}}
_RAW: dict[str, dict[str, dict]] = {
    "Computing": {
        "Laptops": {
            "brands": ["Dell", "ASUS", "Lenovo", "HP", "Acer", "Apple"],
            "categories": {
                "Gaming Laptops": ["15\" Gaming Laptops", "17\" Gaming Laptops"],
                "Ultrabooks": ["13\" Ultrabooks", "14\" Ultrabooks"],
                "Business Laptops": ["Standard Business Laptops", "Convertible Business Laptops"],
            },
        },
        "Desktops": {
            "brands": ["Dell", "HP", "Lenovo", "CyberPowerPC", "Apple"],
            "categories": {
                "Gaming Desktops": ["Mid-Tower Gaming Desktops", "Compact Gaming Desktops"],
                "All-in-Ones": ["24\" All-in-Ones", "27\" All-in-Ones"],
            },
        },
        "PC Components": {
            "brands": ["NVIDIA", "AMD", "Intel", "Corsair", "Samsung", "Western Digital"],
            "categories": {
                "Graphics Cards": ["NVIDIA GPUs", "AMD GPUs"],
                "Storage Drives": ["NVMe SSDs", "SATA SSDs", "Hard Disk Drives"],
                "Memory": ["DDR4 Memory", "DDR5 Memory"],
            },
        },
    },
    "Consumer Electronics": {
        "Cameras": {
            "brands": ["Canon", "Nikon", "Sony", "Fujifilm"],
            "categories": {
                "Mirrorless Cameras": ["Full-Frame Mirrorless", "APS-C Mirrorless"],
                "Action Cameras": ["Standard Action Cameras", "360 Action Cameras"],
            },
        },
        "Mobile": {
            "brands": ["Apple", "Samsung", "Google", "Motorola"],
            "categories": {
                "Smartphones": ["Flagship Smartphones", "Mid-Range Smartphones"],
                "Tablets": ["Standard Tablets", "Pro Tablets"],
            },
        },
        "Printers": {
            "brands": ["HP", "Canon", "Epson", "Brother"],
            "categories": {
                "Inkjet Printers": ["All-in-One Inkjet", "Photo Inkjet"],
                "Laser Printers": ["Monochrome Laser", "Color Laser"],
            },
        },
    },
    "Appliances": {
        "Major Appliances": {
            "brands": ["Whirlpool", "LG", "Samsung", "GE"],
            "categories": {
                "Refrigerators": ["French-Door Refrigerators", "Top-Freezer Refrigerators"],
                "Laundry": ["Front-Load Washers", "Electric Dryers"],
            },
        },
        "Small Appliances": {
            "brands": ["Ninja", "Cuisinart", "Keurig", "Dyson"],
            "categories": {
                "Kitchen": ["Blenders", "Coffee Makers"],
                "Home": ["Vacuum Cleaners", "Air Purifiers"],
            },
        },
    },
    "Networking & DIY": {
        "Networking": {
            "brands": ["Ubiquiti", "Netgear", "TP-Link", "ASUS"],
            "categories": {
                "Routers": ["Wi-Fi 6 Routers", "Mesh Routers"],
                "Switches": ["Unmanaged Switches", "Managed Switches"],
            },
        },
        "Cabling & Parts": {
            "brands": ["Monoprice", "Cable Matters", "StarTech", "Belkin"],
            "categories": {
                "Ethernet Cabling": ["Cat6 Ethernet Cable", "Cat6a Ethernet Cable"],
                "Connectors & Tools": ["RJ45 Connectors", "Crimping Tools"],
            },
        },
    },
    "Services": {
        "Support Services": {
            "brands": ["Techmart Care"],
            "categories": {
                "Protection Plans": ["Laptop Protection Plans", "Appliance Protection Plans"],
                "Installation": ["Home Networking Installation", "Appliance Installation"],
            },
        },
    },
}


def _build() -> tuple[Division, ...]:
    divisions: list[Division] = []
    for d_idx, (div_name, deps) in enumerate(_RAW.items(), start=1):
        departments: list[Department] = []
        for p_idx, (dep_name, dep_body) in enumerate(deps.items(), start=1):
            brands = tuple(dep_body["brands"])
            categories: list[Category] = []
            for c_idx, (cat_name, subs) in enumerate(dep_body["categories"].items(), start=1):
                subcategories = tuple(
                    Subcategory(
                        id=f"SUB{d_idx:02d}{p_idx:02d}{c_idx:02d}{s_idx:02d}",
                        name=sub_name,
                    )
                    for s_idx, sub_name in enumerate(subs, start=1)
                )
                categories.append(
                    Category(
                        id=f"CAT{d_idx:02d}{p_idx:02d}{c_idx:02d}",
                        name=cat_name,
                        subcategories=subcategories,
                        brands=brands,
                    )
                )
            departments.append(
                Department(id=f"DEP{d_idx:02d}{p_idx:02d}", name=dep_name, categories=tuple(categories))
            )
        divisions.append(Division(id=f"DIV{d_idx:02d}", name=div_name, departments=tuple(departments)))
    return tuple(divisions)


TAXONOMY: tuple[Division, ...] = _build()


def subcategory_paths() -> list[tuple[Division, Department, Category, Subcategory]]:
    """Every root-to-leaf path through the taxonomy."""
    return [
        (div, dep, cat, sub)
        for div in TAXONOMY
        for dep in div.departments
        for cat in dep.categories
        for sub in cat.subcategories
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_taxonomy.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Run the full suite and commit**

Run: `python -m pytest -q`
Expected: all tests pass (Phase 1 suite + all Phase 2A additions).

```bash
git add src/techmart/reference tests/test_taxonomy.py
git commit -m "feat: add curated electronics product taxonomy reference data"
```

---

## Self-Review

**1. Spec coverage (Phase 2A slice):**
- Final-review Important #1 (framework dtype validation) → Task 1. ✅
- SCD2 scaffolding reused by all Phase 2B dims → Task 2. ✅
- Final-review Important #2 (registry dispatch) → Task 3. ✅
- Entity sizing (num_customers/num_vendors + derived employees/promotions) → Task 4. ✅
- Curated electronics taxonomy reference data → Task 5. ✅
- Deferred to Phase 2B: the seven dimension builders that consume this machinery.

**2. Placeholder scan:** No TBD/TODO; every code and test step contains complete, runnable content. ✅

**3. Type consistency:** `TableSpec.polars_schema()` (Task 1) is consumed by `validate_schema` (Task 1) and available to Phase 2B builders; `scd2_columns()`/`with_scd2_current()` names match across Task 2 and its tests; `TableBuilder(spec, build)` and `REGISTRY` (Task 3) match the CLI's `generate` usage; `ScaleProfile` new fields/properties (Task 4) match the YAML keys and tests; taxonomy dataclass fields and `subcategory_paths()` (Task 5) match the tests. ✅

---

## Next plan

**Phase 2B — Dimension builders:** `dim_channel`, `dim_store`, `dim_vendor`, `dim_employee`, `dim_customer`, `dim_promotion`, `dim_product` — each built on this phase's `polars_schema`, `scd2` helper, registry, sizing, and taxonomy; each registered in `REGISTRY` and generating a conforming Parquet table.
