# Techmart Foundation (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the project scaffolding, configuration/scale-profile system, deterministic generation framework, and the first conformed dimension (`dim_date`) for the Techmart synthetic retail dataset.

**Architecture:** A Python package (`techmart`) generates synthetic retail data locally with Polars. Generation is driven by declarative `TableSpec` objects (schema + comments + grain) and a deterministic seeded-RNG helper, so every run is reproducible and idempotent. A tiny writer validates that a generated DataFrame matches its spec and writes Parquet under an output directory grouped by target schema (`core`, `finance`, …). Large fact generation via Spark/`dbldatagen` is deliberately deferred to Phase 3; local Polars is sufficient for the framework and all dimensions.

**Tech Stack:** Python 3.10+, Polars (dataframes + Parquet), NumPy (seeded RNG), PyYAML (config), pytest (tests).

## Global Constraints

_Every task's requirements implicitly include this section._

- **Language/floor:** Python ≥ 3.10.
- **Naming:** `snake_case`, aligned to the Databricks retail industry model v2 entity names. Surrogate keys are `*_sk` (Int64/BIGINT); business/natural keys are `*_id`.
- **Comments:** every table and every column carries a human-readable comment (stored on the `TableSpec`/`Column` so a later deploy phase can emit Delta `COMMENT`s that fuel Genie).
- **Determinism:** all generation is seeded and reproducible; the same config + seed produces byte-identical output. Generation is idempotent (re-running overwrites cleanly).
- **Config-driven scale:** `scale_profile` ∈ {`demo_lean`, `showcase` (default), `stress`} controls stores/SKUs/history/volume.
- **Secret-free:** no workspace URLs, tokens, or account identifiers in code or committed files (public repo).
- **Schemas:** target schema names are unprefixed internally (`core`, `finance`, `ai`, `ops`, `semantic`); the `techmart_` prefix is applied only at deploy time (not this phase).

---

### Task 1: Project scaffolding & installable package

**Files:**
- Create: `pyproject.toml`
- Create: `src/techmart/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/test_package.py`
- Create: `README.md`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: importable package `techmart` exposing `techmart.__version__: str`.

- [ ] **Step 1: Write the failing test**

`tests/test_package.py`:
```python
import techmart


def test_package_exposes_version():
    assert isinstance(techmart.__version__, str)
    assert techmart.__version__.count(".") >= 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_package.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart'`.

- [ ] **Step 3: Create the package and build config**

`pyproject.toml`:
```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "techmart"
version = "0.1.0"
description = "Synthetic retail data foundation for the Techmart BI blog series"
requires-python = ">=3.10"
dependencies = [
    "polars>=1.0.0",
    "numpy>=1.26.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0.0"]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

`src/techmart/__init__.py`:
```python
"""Techmart synthetic retail data foundation."""

__version__ = "0.1.0"
```

`tests/__init__.py`: (empty file)

`README.md`:
```markdown
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
```

- [ ] **Step 4: Install the package and run the test**

Run: `pip install -e ".[dev]" && python -m pytest tests/test_package.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/techmart/__init__.py tests/__init__.py tests/test_package.py README.md
git commit -m "feat: scaffold techmart package"
```

---

### Task 2: Scale profiles & configuration

**Files:**
- Create: `config/scale_profiles.yaml`
- Create: `src/techmart/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `ScaleProfile` (frozen dataclass): `name: str`, `num_stores: int`, `num_skus: int`, `history_years: int`, `sales_lines_target: int`.
  - `TechmartConfig` (frozen dataclass): `scale_profile: ScaleProfile`, `seed: int`, `output_dir: pathlib.Path`, `catalog: str`, `schema_prefix: str`, `end_date: datetime.date`; property `start_date: datetime.date`.
  - `load_profiles(path: Path) -> dict[str, ScaleProfile]`.
  - `load_config(profiles_path: Path, profile_name: str | None = None, *, seed: int = 42, output_dir: Path = Path("data"), catalog: str = "techmart", schema_prefix: str = "techmart_", end_date: date = date(2026, 1, 31)) -> TechmartConfig`.
  - `DEFAULT_PROFILE: str = "showcase"`.

- [ ] **Step 1: Write the failing tests**

`tests/test_config.py`:
```python
from datetime import date
from pathlib import Path

import pytest

from techmart.config import (
    DEFAULT_PROFILE,
    ScaleProfile,
    load_config,
    load_profiles,
)

PROFILES = Path("config/scale_profiles.yaml")


def test_loads_all_three_profiles():
    profiles = load_profiles(PROFILES)
    assert set(profiles) == {"demo_lean", "showcase", "stress"}
    assert all(isinstance(p, ScaleProfile) for p in profiles.values())


def test_default_profile_is_showcase():
    cfg = load_config(PROFILES)
    assert cfg.scale_profile.name == DEFAULT_PROFILE == "showcase"


def test_start_date_derived_from_history_years():
    cfg = load_config(PROFILES, "showcase", end_date=date(2026, 1, 31))
    assert cfg.scale_profile.history_years == 3
    assert cfg.start_date == date(2023, 1, 31)


def test_unknown_profile_raises():
    with pytest.raises(KeyError):
        load_config(PROFILES, "does_not_exist")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.config'`.

- [ ] **Step 3: Write the config file and module**

`config/scale_profiles.yaml`:
```yaml
# Techmart scale profiles. `showcase` is the demo default.
profiles:
  demo_lean:
    num_stores: 100
    num_skus: 20000
    history_years: 2
    sales_lines_target: 75000000
  showcase:
    num_stores: 1000
    num_skus: 200000
    history_years: 3
    sales_lines_target: 750000000
  stress:
    num_stores: 2000
    num_skus: 500000
    history_years: 5
    sales_lines_target: 3000000000
```

`src/techmart/config.py`:
```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

DEFAULT_PROFILE = "showcase"


@dataclass(frozen=True)
class ScaleProfile:
    name: str
    num_stores: int
    num_skus: int
    history_years: int
    sales_lines_target: int


@dataclass(frozen=True)
class TechmartConfig:
    scale_profile: ScaleProfile
    seed: int
    output_dir: Path
    catalog: str
    schema_prefix: str
    end_date: date

    @property
    def start_date(self) -> date:
        """First calendar day of the generated history window."""
        target_year = self.end_date.year - self.scale_profile.history_years
        try:
            return self.end_date.replace(year=target_year)
        except ValueError:
            # Handle Feb 29 end dates on non-leap target years.
            return self.end_date.replace(year=target_year, day=28)


def load_profiles(path: Path) -> dict[str, ScaleProfile]:
    raw = yaml.safe_load(Path(path).read_text())
    return {
        name: ScaleProfile(name=name, **cfg)
        for name, cfg in raw["profiles"].items()
    }


def load_config(
    profiles_path: Path,
    profile_name: str | None = None,
    *,
    seed: int = 42,
    output_dir: Path = Path("data"),
    catalog: str = "techmart",
    schema_prefix: str = "techmart_",
    end_date: date = date(2026, 1, 31),
) -> TechmartConfig:
    profiles = load_profiles(profiles_path)
    name = profile_name or DEFAULT_PROFILE
    return TechmartConfig(
        scale_profile=profiles[name],
        seed=seed,
        output_dir=Path(output_dir),
        catalog=catalog,
        schema_prefix=schema_prefix,
        end_date=end_date,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add config/scale_profiles.yaml src/techmart/config.py tests/test_config.py
git commit -m "feat: add scale profiles and configuration loader"
```

---

### Task 3: Deterministic seeded RNG

**Files:**
- Create: `src/techmart/rng.py`
- Test: `tests/test_rng.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `SeededRng` class with `__init__(self, base_seed: int)` and `stream(self, name: str) -> numpy.random.Generator`. Same `(base_seed, name)` always yields the same generator sequence; different names yield independent sequences.

- [ ] **Step 1: Write the failing tests**

`tests/test_rng.py`:
```python
import numpy as np

from techmart.rng import SeededRng


def test_same_seed_and_name_is_reproducible():
    a = SeededRng(42).stream("dim_date").integers(0, 1_000_000, size=50)
    b = SeededRng(42).stream("dim_date").integers(0, 1_000_000, size=50)
    assert np.array_equal(a, b)


def test_different_names_are_independent():
    a = SeededRng(42).stream("dim_store").integers(0, 1_000_000, size=50)
    b = SeededRng(42).stream("dim_product").integers(0, 1_000_000, size=50)
    assert not np.array_equal(a, b)


def test_different_base_seeds_diverge():
    a = SeededRng(1).stream("dim_date").integers(0, 1_000_000, size=50)
    b = SeededRng(2).stream("dim_date").integers(0, 1_000_000, size=50)
    assert not np.array_equal(a, b)


def test_stream_returns_numpy_generator():
    assert isinstance(SeededRng(7).stream("x"), np.random.Generator)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_rng.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.rng'`.

- [ ] **Step 3: Write the implementation**

`src/techmart/rng.py`:
```python
from __future__ import annotations

import hashlib

import numpy as np


class SeededRng:
    """Factory for independent, reproducible RNG substreams.

    Each named stream is derived deterministically from the base seed, so
    generators for different tables/columns are reproducible run-to-run yet
    statistically independent of one another.
    """

    def __init__(self, base_seed: int) -> None:
        self.base_seed = base_seed

    def stream(self, name: str) -> np.random.Generator:
        digest = hashlib.sha256(f"{self.base_seed}:{name}".encode()).digest()
        seed = int.from_bytes(digest[:8], "big")
        return np.random.default_rng(seed)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_rng.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/rng.py tests/test_rng.py
git commit -m "feat: add deterministic seeded RNG"
```

---

### Task 4: TableSpec schema & Parquet writer

**Files:**
- Create: `src/techmart/framework/__init__.py`
- Create: `src/techmart/framework/schema.py`
- Create: `src/techmart/framework/writer.py`
- Test: `tests/test_writer.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `Column` (frozen dataclass): `name: str`, `dtype: str`, `comment: str`, `is_key: bool = False`, `nullable: bool = True`.
  - `TableSpec` (frozen dataclass): `schema: str`, `name: str`, `grain: str`, `columns: list[Column]`; property `column_names -> list[str]`.
  - `SchemaMismatchError(ValueError)`.
  - `validate_schema(df: polars.DataFrame, spec: TableSpec) -> None` — raises `SchemaMismatchError` if `df.columns != spec.column_names`.
  - `write_table(df: polars.DataFrame, spec: TableSpec, output_dir: pathlib.Path) -> pathlib.Path` — validates, writes `<output_dir>/<spec.schema>/<spec.name>.parquet`, returns the path.

- [ ] **Step 1: Write the failing tests**

`tests/test_writer.py`:
```python
from pathlib import Path

import polars as pl
import pytest

from techmart.framework.schema import Column, TableSpec
from techmart.framework.writer import (
    SchemaMismatchError,
    validate_schema,
    write_table,
)

SPEC = TableSpec(
    schema="core",
    name="dim_demo",
    grain="one row per demo id",
    columns=[
        Column("demo_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
        Column("label", "Utf8", "Human label"),
    ],
)


def test_column_names_preserves_order():
    assert SPEC.column_names == ["demo_sk", "label"]


def test_validate_schema_passes_on_match():
    df = pl.DataFrame({"demo_sk": [1, 2], "label": ["a", "b"]})
    validate_schema(df, SPEC)  # no raise


def test_validate_schema_rejects_wrong_columns():
    df = pl.DataFrame({"demo_sk": [1], "wrong": ["a"]})
    with pytest.raises(SchemaMismatchError):
        validate_schema(df, SPEC)


def test_write_table_roundtrips(tmp_path: Path):
    df = pl.DataFrame({"demo_sk": [1, 2], "label": ["a", "b"]})
    dest = write_table(df, SPEC, tmp_path)
    assert dest == tmp_path / "core" / "dim_demo.parquet"
    assert dest.exists()
    back = pl.read_parquet(dest)
    assert back.columns == SPEC.column_names
    assert back.height == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_writer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.framework'`.

- [ ] **Step 3: Write the framework modules**

`src/techmart/framework/__init__.py`:
```python
"""Declarative table-spec generation framework."""
```

`src/techmart/framework/schema.py`:
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str  # Polars dtype name, e.g. "Int64", "Utf8", "Date", "Boolean"
    comment: str
    is_key: bool = False
    nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    schema: str  # target schema group: "core", "finance", "ai", "ops", "semantic"
    name: str  # table name, e.g. "dim_date"
    grain: str  # one-line description of the row grain
    columns: list[Column]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]
```

`src/techmart/framework/writer.py`:
```python
from __future__ import annotations

from pathlib import Path

import polars as pl

from .schema import TableSpec


class SchemaMismatchError(ValueError):
    """Raised when a DataFrame's columns do not match its TableSpec."""


def validate_schema(df: pl.DataFrame, spec: TableSpec) -> None:
    if df.columns != spec.column_names:
        raise SchemaMismatchError(
            f"{spec.name}: expected columns {spec.column_names}, got {df.columns}"
        )


def write_table(df: pl.DataFrame, spec: TableSpec, output_dir: Path) -> Path:
    validate_schema(df, spec)
    dest_dir = Path(output_dir) / spec.schema
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{spec.name}.parquet"
    df.write_parquet(dest)
    return dest
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_writer.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/framework tests/test_writer.py
git commit -m "feat: add table spec schema and parquet writer"
```

---

### Task 5: `dim_date` builder (retail 4-5-4 calendar)

**Files:**
- Create: `src/techmart/dimensions/__init__.py`
- Create: `src/techmart/dimensions/dim_date.py`
- Test: `tests/test_dim_date.py`

**Interfaces:**
- Consumes: `Column`, `TableSpec` from `techmart.framework.schema`.
- Produces:
  - `DIM_DATE_SPEC: TableSpec` (schema `"core"`, name `"dim_date"`).
  - `fiscal_attrs(d: datetime.date) -> tuple[int, int, int, int]` returning `(fiscal_year, fiscal_week, fiscal_period, fiscal_quarter)`.
  - `holiday_name(d: datetime.date) -> str | None`.
  - `build_dim_date(start: datetime.date, end: datetime.date) -> polars.DataFrame` conforming to `DIM_DATE_SPEC` (inclusive of both ends), ordered by date.

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_date.py`:
```python
from datetime import date

from techmart.dimensions.dim_date import (
    DIM_DATE_SPEC,
    build_dim_date,
    fiscal_attrs,
    holiday_name,
)
from techmart.framework.writer import validate_schema


def test_row_count_is_inclusive_day_count():
    df = build_dim_date(date(2024, 1, 1), date(2024, 12, 31))
    assert df.height == 366  # 2024 is a leap year


def test_conforms_to_spec():
    df = build_dim_date(date(2024, 1, 1), date(2024, 1, 31))
    validate_schema(df, DIM_DATE_SPEC)  # no raise


def test_date_sk_is_yyyymmdd_and_unique():
    df = build_dim_date(date(2024, 1, 1), date(2024, 1, 3))
    assert df["date_sk"].to_list() == [20240101, 20240102, 20240103]
    assert df["date_sk"].n_unique() == df.height


def test_weekend_flag():
    df = build_dim_date(date(2024, 1, 6), date(2024, 1, 8))  # Sat, Sun, Mon
    assert df["is_weekend"].to_list() == [True, True, False]


def test_known_holidays():
    df = build_dim_date(date(2024, 12, 24), date(2024, 12, 26))
    names = dict(zip(df["date_sk"].to_list(), df["holiday_name"].to_list()))
    assert names[20241225] == "Christmas Day"
    assert names[20241224] is None
    # Thanksgiving 2024 = Nov 28; Black Friday = Nov 29.
    assert holiday_name(date(2024, 11, 28)) == "Thanksgiving"
    assert holiday_name(date(2024, 11, 29)) == "Black Friday"


def test_fiscal_week_one_starts_first_sunday_of_february():
    # First Sunday of Feb 2024 is Feb 4 -> fiscal week 1, period 1, quarter 1.
    fy, fw, fp, fq = fiscal_attrs(date(2024, 2, 4))
    assert (fy, fw, fp, fq) == (2024, 1, 1, 1)


def test_fiscal_period_pattern_454():
    # Week 5 falls in period 2 (4-5-4: period 1 = weeks 1-4, period 2 = weeks 5-9).
    _, _, period_wk5, _ = fiscal_attrs(date(2024, 3, 3))  # 4 weeks after Feb 4
    assert period_wk5 == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_date.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.dimensions'`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/__init__.py`:
```python
"""Conformed dimension builders."""
```

`src/techmart/dimensions/dim_date.py`:
```python
from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from ..framework.schema import Column, TableSpec

DIM_DATE_SPEC = TableSpec(
    schema="core",
    name="dim_date",
    grain="one row per calendar day",
    columns=[
        Column("date_sk", "Int64", "Surrogate key in yyyymmdd form", is_key=True, nullable=False),
        Column("date", "Date", "Calendar date", nullable=False),
        Column("day_of_week", "Int64", "ISO day of week (1=Mon..7=Sun)"),
        Column("day_name", "Utf8", "Day name"),
        Column("week", "Int64", "ISO week number"),
        Column("month", "Int64", "Calendar month (1-12)"),
        Column("month_name", "Utf8", "Month name"),
        Column("quarter", "Int64", "Calendar quarter (1-4)"),
        Column("year", "Int64", "Calendar year"),
        Column("fiscal_year", "Int64", "Retail 4-5-4 fiscal year"),
        Column("fiscal_week", "Int64", "Retail fiscal week (1-53)"),
        Column("fiscal_period", "Int64", "Retail fiscal period (1-12)"),
        Column("fiscal_quarter", "Int64", "Retail fiscal quarter (1-4)"),
        Column("is_weekend", "Boolean", "True on Saturday or Sunday"),
        Column("is_holiday", "Boolean", "True on a recognized US holiday"),
        Column("holiday_name", "Utf8", "Holiday name, else null"),
        Column("selling_season", "Utf8", "Retail selling-season label"),
    ],
)

_DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
_MONTH_SEASON = {
    1: "Post-Holiday", 2: "Post-Holiday", 3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Back-to-School", 8: "Back-to-School", 9: "Back-to-School",
    10: "Fall", 11: "Holiday", 12: "Holiday",
}
# 4-5-4 weeks-per-period pattern (12 periods, 4 quarters of 4-5-4).
_PERIOD_WEEKS = [4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5, 4]


def _first_sunday_of_february(year: int) -> date:
    d = date(year, 2, 1)
    offset = (6 - d.weekday()) % 7  # weekday(): Mon=0..Sun=6
    return d + timedelta(days=offset)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """nth (1-based) `weekday` (Mon=0..Sun=6) of the month."""
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    last = nxt - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def fiscal_attrs(d: date) -> tuple[int, int, int, int]:
    """Return (fiscal_year, fiscal_week, fiscal_period, fiscal_quarter)."""
    fy_start = _first_sunday_of_february(d.year)
    if d < fy_start:
        fiscal_year = d.year - 1
        fy_start = _first_sunday_of_february(d.year - 1)
    else:
        fiscal_year = d.year
    fiscal_week = (d - fy_start).days // 7 + 1
    cumulative = 0
    fiscal_period = len(_PERIOD_WEEKS)  # default to last period for 53-week overflow
    for idx, weeks in enumerate(_PERIOD_WEEKS, start=1):
        cumulative += weeks
        if fiscal_week <= cumulative:
            fiscal_period = idx
            break
    fiscal_quarter = (fiscal_period - 1) // 3 + 1
    return fiscal_year, fiscal_week, fiscal_period, fiscal_quarter


def holiday_name(d: date) -> str | None:
    if (d.month, d.day) == (1, 1):
        return "New Year's Day"
    if (d.month, d.day) == (7, 4):
        return "Independence Day"
    if (d.month, d.day) == (12, 25):
        return "Christmas Day"
    thanksgiving = _nth_weekday(d.year, 11, 3, 4)  # 4th Thursday of November
    if d == thanksgiving:
        return "Thanksgiving"
    if d == thanksgiving + timedelta(days=1):
        return "Black Friday"
    if d == _last_weekday(d.year, 5, 0):  # last Monday of May
        return "Memorial Day"
    if d == _nth_weekday(d.year, 9, 0, 1):  # first Monday of September
        return "Labor Day"
    return None


def build_dim_date(start: date, end: date) -> pl.DataFrame:
    rows: list[dict] = []
    d = start
    while d <= end:
        fy, fw, fp, fq = fiscal_attrs(d)
        hn = holiday_name(d)
        rows.append(
            {
                "date_sk": d.year * 10000 + d.month * 100 + d.day,
                "date": d,
                "day_of_week": d.isoweekday(),
                "day_name": _DAY_NAMES[d.weekday()],
                "week": d.isocalendar()[1],
                "month": d.month,
                "month_name": _MONTH_NAMES[d.month - 1],
                "quarter": (d.month - 1) // 3 + 1,
                "year": d.year,
                "fiscal_year": fy,
                "fiscal_week": fw,
                "fiscal_period": fp,
                "fiscal_quarter": fq,
                "is_weekend": d.weekday() >= 5,
                "is_holiday": hn is not None,
                "holiday_name": hn,
                "selling_season": _MONTH_SEASON[d.month],
            }
        )
        d += timedelta(days=1)
    return pl.DataFrame(rows).select(DIM_DATE_SPEC.column_names)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_date.py -v`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/dimensions tests/test_dim_date.py
git commit -m "feat: add dim_date builder with retail 4-5-4 calendar"
```

---

### Task 6: CLI entrypoint (end-to-end `dim_date` generation)

**Files:**
- Create: `src/techmart/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: `load_config`/`TechmartConfig` from `techmart.config`; `DIM_DATE_SPEC`/`build_dim_date` from `techmart.dimensions.dim_date`; `write_table` from `techmart.framework.writer`.
- Produces:
  - `generate(config: TechmartConfig, tables: list[str]) -> list[pathlib.Path]` — builds requested tables, writes them, returns written paths. Supported table name this phase: `"dim_date"`. Unknown names raise `ValueError`.
  - `main(argv: list[str] | None = None) -> int` — argparse CLI: `--profile`, `--seed`, `--output-dir`, `--profiles-path`, `--tables` (comma-separated, default `dim_date`). Returns process exit code.
  - `python -m techmart.cli ...` works (module `__main__` guard calling `sys.exit(main())`).

- [ ] **Step 1: Write the failing tests**

`tests/test_cli.py`:
```python
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from techmart.cli import generate, main
from techmart.config import load_config

PROFILES = Path("config/scale_profiles.yaml")


def test_generate_writes_dim_date(tmp_path: Path):
    cfg = load_config(PROFILES, "demo_lean", output_dir=tmp_path, end_date=date(2026, 1, 31))
    paths = generate(cfg, ["dim_date"])
    assert paths == [tmp_path / "core" / "dim_date.parquet"]
    df = pl.read_parquet(paths[0])
    # demo_lean = 2 years history -> 2024-01-31 .. 2026-01-31 inclusive.
    assert df.height == (date(2026, 1, 31) - date(2024, 1, 31)).days + 1


def test_generate_rejects_unknown_table(tmp_path: Path):
    cfg = load_config(PROFILES, "demo_lean", output_dir=tmp_path)
    with pytest.raises(ValueError):
        generate(cfg, ["dim_unicorn"])


def test_main_returns_zero_and_writes(tmp_path: Path):
    code = main(
        [
            "--profile", "demo_lean",
            "--output-dir", str(tmp_path),
            "--tables", "dim_date",
            "--profiles-path", str(PROFILES),
        ]
    )
    assert code == 0
    assert (tmp_path / "core" / "dim_date.parquet").exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.cli'`.

- [ ] **Step 3: Write the CLI**

`src/techmart/cli.py`:
```python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import TechmartConfig, load_config
from .dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from .framework.writer import write_table


def generate(config: TechmartConfig, tables: list[str]) -> list[Path]:
    written: list[Path] = []
    for table in tables:
        if table == "dim_date":
            df = build_dim_date(config.start_date, config.end_date)
            written.append(write_table(df, DIM_DATE_SPEC, config.output_dir))
        else:
            raise ValueError(f"Unknown table: {table!r}")
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Techmart synthetic data.")
    parser.add_argument("--profile", default=None, help="Scale profile name.")
    parser.add_argument("--seed", type=int, default=42, help="Base RNG seed.")
    parser.add_argument("--output-dir", default="data", help="Output directory.")
    parser.add_argument(
        "--profiles-path",
        default="config/scale_profiles.yaml",
        help="Path to scale_profiles.yaml.",
    )
    parser.add_argument(
        "--tables",
        default="dim_date",
        help="Comma-separated table names to generate.",
    )
    args = parser.parse_args(argv)

    config = load_config(
        Path(args.profiles_path),
        args.profile,
        seed=args.seed,
        output_dir=Path(args.output_dir),
    )
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    paths = generate(config, tables)
    for path in paths:
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_cli.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Run the full suite and the CLI for real**

Run: `python -m pytest -v && python -m techmart.cli --profile demo_lean --output-dir ./data --tables dim_date`
Expected: all tests PASS; prints `wrote data/core/dim_date.parquet`.

- [ ] **Step 6: Commit**

```bash
git add src/techmart/cli.py tests/test_cli.py
git commit -m "feat: add CLI entrypoint for dim_date generation"
```

---

## Self-Review

**1. Spec coverage (Phase 1 slice):**
- Repo scaffolding / installable package → Task 1. ✅
- Config-driven scale profiles (demo_lean / showcase-default / stress) → Task 2. ✅
- Deterministic, reproducible, seeded generation → Task 3. ✅
- Declarative `TableSpec` framework with per-column comments + schema validation + Parquet writer → Task 4. ✅
- `dim_date` with fiscal 4-5-4 calendar, holidays, selling season, `date_sk` (yyyymmdd) → Task 5. ✅
- End-to-end idempotent generation entrypoint → Task 6. ✅
- Deferred by design (later phases): all other dims (Phase 2), facts + Spark/dbldatagen writer (Phase 3), finance (Phase 4), AI/text/anomalies (Phase 5), semantic layer (Phase 6), Lakebase (Phase 7), and the Delta-deploy step that emits `COMMENT`s and applies the `techmart_` schema prefix.

**2. Placeholder scan:** No TBD/TODO; every code and test step contains complete, runnable content. ✅

**3. Type consistency:** `Column`/`TableSpec` fields and `column_names` are used identically across Tasks 4–6; `write_table(df, spec, output_dir)`, `build_dim_date(start, end)`, `fiscal_attrs`/`holiday_name` signatures, and `generate(config, tables)` match their definitions and call sites in tests. `load_config` keyword args used by CLI/tests match Task 2's signature. ✅

---

## Next phases (separate plans, written when we reach them)

2. Dimensions · 3. Core facts (introduces Spark/`dbldatagen` writer) · 4. Finance & reconciliation · 5. AI enablement · 6. Semantic layer · 7. Lakebase operational + sync.
