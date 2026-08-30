# Techmart Phase 2B — Standard Dimension Builders — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the six "standard" conformed dimensions — `dim_channel`, `dim_store`, `dim_vendor`, `dim_employee`, `dim_customer`, `dim_promotion` — on the Phase 2A machinery, each registered in `REGISTRY` and generating a schema-conforming Parquet table. (`dim_product` is Phase 2C.)

**Architecture:** Each dimension is a module under `src/techmart/dimensions/` exposing a `TableSpec` and a `build_dim_x(config) -> polars.DataFrame`, registered in `techmart.registry`. Generation is **vectorized** (NumPy arrays + Polars expressions, no per-row Python loops) so builders scale to millions of rows. Determinism comes from per-field `SeededRng` substreams. SCD2 dimensions append current-version scaffolding via `with_scd2_current`. Cross-dimension foreign keys are drawn as random integers within `[1, N]` of the referenced dimension — valid because surrogate keys are the sequential range `1..N` (referential integrity holds without reading the other table).

**Tech Stack:** Python ≥ 3.10, Polars, NumPy, PyYAML, pytest.

## Global Constraints

_Every task's requirements implicitly include this section._

- **Language/floor:** Python ≥ 3.10.
- **Naming:** `snake_case`, aligned to the Databricks retail industry model v2. Surrogate keys `*_sk` are Int64 and are the sequential range `1..N`; business keys `*_id` are zero-padded strings.
- **Comments:** every column carries a human-readable comment on its `Column` (fuels Genie).
- **Determinism:** all generation is seeded via `techmart.rng.SeededRng(config.seed).stream(name)`; the same config + seed produces identical output. **Vectorized only** — no Python per-row loops over the row count (build with NumPy arrays / Polars expressions), so builders scale to millions of rows.
- **SCD2 policy (Phase 2):** SCD2 dimensions (`store`, `vendor`, `employee`, `customer`, `promotion`) append current-version scaffolding via `with_scd2_current(df, config.start_date)`: `is_current=True`, `effective_start_ts` at history start, `effective_end_ts=null`, `version=1`. `dim_channel` is a small fixed reference dimension with no SCD2.
- **Build pattern (use exactly this shape):** build a dict of NumPy arrays / lists for the base columns → `pl.DataFrame(data)` → (SCD2 dims only) `with_scd2_current(df, config.start_date)` → `return df.cast(SPEC.polars_schema()).select(SPEC.column_names)`. The final `cast` guarantees dtype conformance; the `select` locks column order to the spec.
- **Foreign keys:** reference other dimensions by drawing `rng.integers(1, N + 1, size=n)` where `N` is that dimension's configured count. Never read another table's Parquet.
- **Config-driven scale:** counts come from `config.scale_profile` (`num_stores`, `num_customers`, `num_vendors`, `num_employees`, `num_promotions`).
- **Registration:** each builder registers itself in `techmart.registry.REGISTRY` keyed by its spec name; new builders take `config` directly (no wrapper), matching `TableBuilder.build`.
- **Secret-free; serverless-compatible pure Python + Polars.**
- **Schemas:** target schema is `core` (unprefixed); the `techmart_` prefix is applied only at deploy time (not this phase).

---

### Task 1: Dimension support helpers & reference lists

**Files:**
- Create: `src/techmart/dimensions/support.py`
- Test: `tests/test_dimension_support.py`

**Interfaces:**
- Consumes: NumPy.
- Produces (all vectorized; return NumPy arrays):
  - `surrogate_keys(n: int) -> numpy.ndarray` — `int64` array `[1..n]`.
  - `business_keys(prefix: str, n: int, width: int = 6) -> numpy.ndarray` — array of `f"{prefix}{i:0{width}d}"` for `i` in `1..n`.
  - `sample(rng: numpy.random.Generator, values: list, n: int) -> numpy.ndarray` — `n` random picks from `values` (with replacement), by fancy-indexing.
  - `random_dates(rng, start: datetime.date, end: datetime.date, n: int) -> numpy.ndarray` — `datetime64[D]` array uniformly in `[start, end)`.
  - Reference lists: `US_STATES: list[str]` (≥ 12), `CITIES: list[str]` (≥ 12), `FIRST_NAMES: list[str]` (≥ 15), `LAST_NAMES: list[str]` (≥ 15).

- [ ] **Step 1: Write the failing tests**

`tests/test_dimension_support.py`:
```python
from datetime import date

import numpy as np

from techmart.dimensions import support


def test_surrogate_keys_are_sequential_int64():
    sk = support.surrogate_keys(5)
    assert sk.tolist() == [1, 2, 3, 4, 5]
    assert sk.dtype == np.int64


def test_business_keys_zero_padded():
    keys = support.business_keys("STORE", 3, width=4)
    assert keys.tolist() == ["STORE0001", "STORE0002", "STORE0003"]


def test_sample_is_deterministic_and_in_range():
    rng1 = np.random.default_rng(7)
    rng2 = np.random.default_rng(7)
    a = support.sample(rng1, ["x", "y", "z"], 10)
    b = support.sample(rng2, ["x", "y", "z"], 10)
    assert a.tolist() == b.tolist()
    assert set(a.tolist()) <= {"x", "y", "z"}
    assert len(a) == 10


def test_random_dates_within_bounds():
    rng = np.random.default_rng(1)
    d = support.random_dates(rng, date(2020, 1, 1), date(2020, 12, 31), 100)
    lo = np.datetime64(date(2020, 1, 1))
    hi = np.datetime64(date(2020, 12, 31))
    assert d.min() >= lo and d.max() < hi
    assert len(d) == 100


def test_reference_lists_present():
    assert len(support.US_STATES) >= 12
    assert len(support.CITIES) >= 12
    assert len(support.FIRST_NAMES) >= 15
    assert len(support.LAST_NAMES) >= 15
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dimension_support.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.dimensions.support'`.

- [ ] **Step 3: Write the implementation**

`src/techmart/dimensions/support.py`:
```python
from __future__ import annotations

from datetime import date

import numpy as np

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


def surrogate_keys(n: int) -> np.ndarray:
    return np.arange(1, n + 1, dtype=np.int64)


def business_keys(prefix: str, n: int, width: int = 6) -> np.ndarray:
    nums = np.arange(1, n + 1)
    return np.char.add(prefix, np.char.zfill(nums.astype(str), width))


def sample(rng: np.random.Generator, values: list, n: int) -> np.ndarray:
    arr = np.asarray(values, dtype=object)
    return arr[rng.integers(0, len(values), size=n)]


def random_dates(rng: np.random.Generator, start: date, end: date, n: int) -> np.ndarray:
    span = (end - start).days
    offsets = rng.integers(0, span, size=n).astype("timedelta64[D]")
    return np.datetime64(start) + offsets
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_dimension_support.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/dimensions/support.py tests/test_dimension_support.py
git commit -m "feat: add vectorized dimension support helpers and reference lists"
```

---

### Task 2: `dim_channel` (fixed reference dimension)

**Files:**
- Create: `src/techmart/dimensions/dim_channel.py`
- Modify: `src/techmart/registry.py` (register `dim_channel`)
- Test: `tests/test_dim_channel.py`

**Interfaces:**
- Consumes: `Column`, `TableSpec` (framework); `TechmartConfig`.
- Produces: `DIM_CHANNEL_SPEC: TableSpec` (schema `core`); `build_dim_channel(config: TechmartConfig) -> polars.DataFrame` (fixed 5 rows; config unused).

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_channel.py`:
```python
from datetime import date
from pathlib import Path

from techmart.config import load_config
from techmart.dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
from techmart.framework.writer import validate_schema

CFG = load_config(Path("config/scale_profiles.yaml"), "demo_lean", end_date=date(2026, 1, 31))


def test_channel_has_five_rows_conforming():
    df = build_dim_channel(CFG)
    assert df.height == 5
    validate_schema(df, DIM_CHANNEL_SPEC)


def test_channel_keys_unique_and_sequential():
    df = build_dim_channel(CFG)
    assert df["channel_sk"].to_list() == [1, 2, 3, 4, 5]


def test_channel_names_and_types():
    df = build_dim_channel(CFG)
    assert set(df["channel_name"].to_list()) == {
        "In-Store", "Web", "Mobile-App", "Marketplace", "Call-Center",
    }
    assert set(df["channel_type"].to_list()) <= {"Physical", "Digital"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_channel.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.dimensions.dim_channel'`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/dim_channel.py`:
```python
from __future__ import annotations

import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec

DIM_CHANNEL_SPEC = TableSpec(
    schema="core",
    name="dim_channel",
    grain="one row per sales/interaction channel",
    columns=[
        Column("channel_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
        Column("channel_id", "Utf8", "Business key", nullable=False),
        Column("channel_name", "Utf8", "Channel name"),
        Column("channel_type", "Utf8", "Physical or Digital"),
    ],
)

_CHANNELS = [
    ("In-Store", "Physical"),
    ("Web", "Digital"),
    ("Mobile-App", "Digital"),
    ("Marketplace", "Digital"),
    ("Call-Center", "Physical"),
]


def build_dim_channel(config: TechmartConfig) -> pl.DataFrame:
    data = {
        "channel_sk": list(range(1, len(_CHANNELS) + 1)),
        "channel_id": [f"CH{i:02d}" for i in range(1, len(_CHANNELS) + 1)],
        "channel_name": [name for name, _ in _CHANNELS],
        "channel_type": [ctype for _, ctype in _CHANNELS],
    }
    df = pl.DataFrame(data)
    return df.cast(DIM_CHANNEL_SPEC.polars_schema()).select(DIM_CHANNEL_SPEC.column_names)
```

- [ ] **Step 4: Register `dim_channel`** (`src/techmart/registry.py`)

Add the import alongside the existing `dim_date` import:
```python
from .dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
```
And add an entry to the `REGISTRY` dict (new builders take `config` directly, so no wrapper):
```python
    DIM_CHANNEL_SPEC.name: TableBuilder(spec=DIM_CHANNEL_SPEC, build=build_dim_channel),
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_channel.py tests/test_registry.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/techmart/dimensions/dim_channel.py src/techmart/registry.py tests/test_dim_channel.py
git commit -m "feat: add dim_channel reference dimension"
```

---

### Task 3: `dim_store` (SCD2)

**Files:**
- Create: `src/techmart/dimensions/dim_store.py`
- Modify: `src/techmart/registry.py`
- Test: `tests/test_dim_store.py`

**Interfaces:**
- Consumes: framework `Column`/`TableSpec`/`scd2` helpers; `support`; `SeededRng`; `TechmartConfig`.
- Produces: `DIM_STORE_SPEC: TableSpec`; `build_dim_store(config) -> polars.DataFrame` (`config.scale_profile.num_stores` rows).

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_store.py`:
```python
from datetime import date

import polars as pl

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_store import DIM_STORE_SPEC, build_dim_store
from techmart.framework.writer import validate_schema


def _cfg(num_stores: int) -> TechmartConfig:
    profile = ScaleProfile("t", num_stores, 10, 1, 1, 8, 4)
    return TechmartConfig(profile, 1, __import__("pathlib").Path("data"), "techmart", "techmart_", date(2026, 1, 31))


def test_store_row_count_and_schema():
    df = build_dim_store(_cfg(50))
    assert df.height == 50
    validate_schema(df, DIM_STORE_SPEC)


def test_store_keys_sequential_and_unique():
    df = build_dim_store(_cfg(50))
    assert df["store_sk"].to_list() == list(range(1, 51))
    assert df["store_id"].n_unique() == 50


def test_store_scd2_current():
    df = build_dim_store(_cfg(20))
    assert df["is_current"].to_list() == [True] * 20
    assert df["version"].to_list() == [1] * 20
    assert df["effective_end_ts"].null_count() == 20


def test_store_is_deterministic():
    a = build_dim_store(_cfg(30))
    b = build_dim_store(_cfg(30))
    assert a.equals(b)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_store.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/dim_store.py`:
```python
from __future__ import annotations

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
_FORMATS = ["Flagship", "Standard", "Outlet", "Online-only"]

_BASE_COLUMNS = [
    Column("store_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("store_id", "Utf8", "Business key", nullable=False),
    Column("store_name", "Utf8", "Store display name"),
    Column("store_format", "Utf8", "Flagship/Standard/Outlet/Online-only"),
    Column("region_id", "Utf8", "Region business key"),
    Column("region_name", "Utf8", "Region name"),
    Column("district_id", "Utf8", "District business key"),
    Column("district_name", "Utf8", "District name"),
    Column("city", "Utf8", "City"),
    Column("state", "Utf8", "US state code"),
    Column("postal_code", "Utf8", "Postal code"),
    Column("country", "Utf8", "Country code"),
    Column("latitude", "Float64", "Latitude"),
    Column("longitude", "Float64", "Longitude"),
    Column("square_footage", "Int64", "Store square footage"),
    Column("open_date", "Date", "Store opening date"),
    Column("status", "Utf8", "Operating status"),
    Column("is_ship_from_store", "Boolean", "Fulfills online orders"),
    Column("is_bopis_enabled", "Boolean", "Supports buy-online-pickup-in-store"),
    Column("cost_center_id", "Utf8", "Finance cost center identifier"),
]

DIM_STORE_SPEC = TableSpec(
    schema="core",
    name="dim_store",
    grain="one current row per store (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_store(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_stores
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    region_idx = rng.stream("dim_store.region").integers(0, len(_REGIONS), n)
    district_idx = rng.stream("dim_store.district").integers(1, 21, n)  # 20 districts
    postal = rng.stream("dim_store.postal").integers(10000, 99999, n)
    data = {
        "store_sk": sk,
        "store_id": support.business_keys("STORE", n, 5),
        "store_name": np.char.add("Techmart ", support.business_keys("STORE", n, 5)),
        "store_format": support.sample(rng.stream("dim_store.format"), _FORMATS, n),
        "region_id": np.char.add("RGN", (region_idx + 1).astype(str)),
        "region_name": np.asarray(_REGIONS, dtype=object)[region_idx],
        "district_id": np.char.add("DST", np.char.zfill(district_idx.astype(str), 2)),
        "district_name": np.char.add("District ", np.char.zfill(district_idx.astype(str), 2)),
        "city": support.sample(rng.stream("dim_store.city"), support.CITIES, n),
        "state": support.sample(rng.stream("dim_store.state"), support.US_STATES, n),
        "postal_code": postal.astype(str),
        "country": np.full(n, "US", dtype=object),
        "latitude": rng.stream("dim_store.lat").uniform(25.0, 49.0, n),
        "longitude": rng.stream("dim_store.lon").uniform(-124.0, -67.0, n),
        "square_footage": rng.stream("dim_store.sqft").integers(15000, 45000, n),
        "open_date": support.random_dates(rng.stream("dim_store.open"), date(2005, 1, 1), date(2019, 1, 1), n),
        "status": np.full(n, "Active", dtype=object),
        "is_ship_from_store": rng.stream("dim_store.sfs").integers(0, 2, n).astype(bool),
        "is_bopis_enabled": rng.stream("dim_store.bopis").integers(0, 2, n).astype(bool),
        "cost_center_id": np.char.add("CC", np.char.zfill(sk.astype(str), 5)),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_STORE_SPEC.polars_schema()).select(DIM_STORE_SPEC.column_names)
```

Add `from datetime import date` at the top of the file (used by `random_dates` bounds).

- [ ] **Step 4: Register `dim_store`** (`src/techmart/registry.py`)

Add import `from .dimensions.dim_store import DIM_STORE_SPEC, build_dim_store` and the REGISTRY entry `DIM_STORE_SPEC.name: TableBuilder(spec=DIM_STORE_SPEC, build=build_dim_store),`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_store.py tests/test_registry.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/techmart/dimensions/dim_store.py src/techmart/registry.py tests/test_dim_store.py
git commit -m "feat: add dim_store dimension (SCD2 scaffolding)"
```

---

### Task 4: `dim_vendor` (SCD2)

**Files:**
- Create: `src/techmart/dimensions/dim_vendor.py`
- Modify: `src/techmart/registry.py`
- Test: `tests/test_dim_vendor.py`

**Interfaces:**
- Produces: `DIM_VENDOR_SPEC`; `build_dim_vendor(config) -> polars.DataFrame` (`num_vendors` rows).

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_vendor.py`:
```python
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_vendor import DIM_VENDOR_SPEC, build_dim_vendor
from techmart.framework.writer import validate_schema


def _cfg(num_vendors: int) -> TechmartConfig:
    profile = ScaleProfile("t", 5, 10, 1, 1, 8, num_vendors)
    return TechmartConfig(profile, 3, Path("data"), "techmart", "techmart_", date(2026, 1, 31))


def test_vendor_rows_and_schema():
    df = build_dim_vendor(_cfg(40))
    assert df.height == 40
    validate_schema(df, DIM_VENDOR_SPEC)


def test_vendor_keys_and_scd2():
    df = build_dim_vendor(_cfg(40))
    assert df["vendor_sk"].to_list() == list(range(1, 41))
    assert df["is_current"].to_list() == [True] * 40


def test_vendor_scorecard_in_range():
    df = build_dim_vendor(_cfg(100))
    ratings = df["vendor_scorecard_rating"].to_list()
    assert all(1 <= r <= 5 for r in ratings)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_vendor.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/dim_vendor.py`:
```python
from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_VENDOR_TYPES = ["Manufacturer", "Distributor", "Marketplace-Seller"]
_CATEGORIES = ["Computing", "Consumer Electronics", "Appliances", "Networking & DIY", "Services"]
_PAYMENT_TERMS = ["NET30", "NET45", "NET60", "NET90"]
_VENDOR_NAME_STEMS = [
    "Apex", "Summit", "Vertex", "Pioneer", "Horizon", "Nimbus", "Quantum",
    "Beacon", "Cascade", "Meridian", "Atlas", "Catalyst", "Orbit", "Vanguard",
]
_VENDOR_NAME_TAILS = ["Electronics", "Supply", "Distribution", "Trading", "Technologies", "Brands"]


def build_dim_vendor(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_vendors
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    stems = support.sample(rng.stream("dim_vendor.stem"), _VENDOR_NAME_STEMS, n)
    tails = support.sample(rng.stream("dim_vendor.tail"), _VENDOR_NAME_TAILS, n)
    names = np.char.add(np.char.add(stems.astype(str), " "), tails.astype(str))
    data = {
        "vendor_sk": sk,
        "vendor_id": support.business_keys("VEND", n, 5),
        "vendor_name": names,
        "vendor_type": support.sample(rng.stream("dim_vendor.type"), _VENDOR_TYPES, n),
        "primary_category": support.sample(rng.stream("dim_vendor.cat"), _CATEGORIES, n),
        "country": np.full(n, "US", dtype=object),
        "relationship_start_date": support.random_dates(
            rng.stream("dim_vendor.rel"), date(2000, 1, 1), date(2020, 1, 1), n
        ),
        "preferred_flag": rng.stream("dim_vendor.pref").integers(0, 2, n).astype(bool),
        "vendor_scorecard_rating": rng.stream("dim_vendor.score").integers(1, 6, n),
        "avg_lead_time_days": rng.stream("dim_vendor.lead").integers(3, 45, n),
        "payment_terms": support.sample(rng.stream("dim_vendor.terms"), _PAYMENT_TERMS, n),
        "active_flag": np.full(n, True, dtype=bool),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_VENDOR_SPEC.polars_schema()).select(DIM_VENDOR_SPEC.column_names)


_BASE_COLUMNS = [
    Column("vendor_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("vendor_id", "Utf8", "Business key", nullable=False),
    Column("vendor_name", "Utf8", "Vendor name"),
    Column("vendor_type", "Utf8", "Manufacturer/Distributor/Marketplace-Seller"),
    Column("primary_category", "Utf8", "Primary product category supplied"),
    Column("country", "Utf8", "Country code"),
    Column("relationship_start_date", "Date", "Date the vendor relationship began"),
    Column("preferred_flag", "Boolean", "Preferred vendor"),
    Column("vendor_scorecard_rating", "Int64", "Scorecard rating 1-5"),
    Column("avg_lead_time_days", "Int64", "Average lead time in days"),
    Column("payment_terms", "Utf8", "Payment terms"),
    Column("active_flag", "Boolean", "Active vendor"),
]

DIM_VENDOR_SPEC = TableSpec(
    schema="core",
    name="dim_vendor",
    grain="one current row per vendor (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)
```

Note: `DIM_VENDOR_SPEC` is defined at module level after the builder for readability; because `build_dim_vendor` references it only at call time (not import time), this ordering is fine. If you prefer, move the spec above the builder — either works.

- [ ] **Step 4: Register `dim_vendor`** — add import `from .dimensions.dim_vendor import DIM_VENDOR_SPEC, build_dim_vendor` and REGISTRY entry `DIM_VENDOR_SPEC.name: TableBuilder(spec=DIM_VENDOR_SPEC, build=build_dim_vendor),`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_vendor.py tests/test_registry.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/techmart/dimensions/dim_vendor.py src/techmart/registry.py tests/test_dim_vendor.py
git commit -m "feat: add dim_vendor dimension (SCD2 scaffolding)"
```

---

### Task 5: `dim_employee` (SCD2, FK → store)

**Files:**
- Create: `src/techmart/dimensions/dim_employee.py`
- Modify: `src/techmart/registry.py`
- Test: `tests/test_dim_employee.py`

**Interfaces:**
- Produces: `DIM_EMPLOYEE_SPEC`; `build_dim_employee(config) -> polars.DataFrame` (`num_employees` rows). `store_sk` FK drawn in `[1, num_stores]`.

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_employee.py`:
```python
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_employee import DIM_EMPLOYEE_SPEC, build_dim_employee
from techmart.framework.writer import validate_schema


def _cfg(num_stores: int) -> TechmartConfig:
    # num_employees is derived as 40 * num_stores.
    profile = ScaleProfile("t", num_stores, 10, 1, 1, 8, 4)
    return TechmartConfig(profile, 5, Path("data"), "techmart", "techmart_", date(2026, 1, 31))


def test_employee_rows_match_derived_count_and_schema():
    cfg = _cfg(3)  # 40 * 3 = 120 employees
    df = build_dim_employee(cfg)
    assert df.height == 120
    validate_schema(df, DIM_EMPLOYEE_SPEC)


def test_employee_store_fk_in_range():
    cfg = _cfg(3)
    df = build_dim_employee(cfg)
    fks = df["store_sk"].to_list()
    assert all(1 <= s <= 3 for s in fks)


def test_managers_have_no_manager():
    cfg = _cfg(3)
    df = build_dim_employee(cfg)
    managers = df.filter(df["role"] == "Manager")
    assert managers["manager_employee_sk"].null_count() == managers.height
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_employee.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/dim_employee.py`:
```python
from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_ROLES = ["Cashier", "Sales-Associate", "Manager", "Buyer", "Planner"]

_BASE_COLUMNS = [
    Column("employee_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("employee_id", "Utf8", "Business key", nullable=False),
    Column("full_name", "Utf8", "Employee full name"),
    Column("role", "Utf8", "Cashier/Sales-Associate/Manager/Buyer/Planner"),
    Column("store_sk", "Int64", "Home store (FK to dim_store)"),
    Column("hire_date", "Date", "Hire date"),
    Column("term_date", "Date", "Termination date; null if active"),
    Column("manager_employee_sk", "Int64", "Manager (FK to dim_employee); null for Managers"),
    Column("status", "Utf8", "Employment status"),
]

DIM_EMPLOYEE_SPEC = TableSpec(
    schema="core",
    name="dim_employee",
    grain="one current row per associate (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_employee(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_employees
    num_stores = config.scale_profile.num_stores
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    first = support.sample(rng.stream("dim_employee.first"), support.FIRST_NAMES, n).astype(str)
    last = support.sample(rng.stream("dim_employee.last"), support.LAST_NAMES, n).astype(str)
    full_name = np.char.add(np.char.add(first, " "), last)
    role = support.sample(rng.stream("dim_employee.role"), _ROLES, n)
    manager_sk = rng.stream("dim_employee.mgr").integers(1, n + 1, n)
    data = {
        "employee_sk": sk,
        "employee_id": support.business_keys("EMP", n, 6),
        "full_name": full_name,
        "role": role,
        "store_sk": rng.stream("dim_employee.store").integers(1, num_stores + 1, n),
        "hire_date": support.random_dates(rng.stream("dim_employee.hire"), date(2010, 1, 1), date(2024, 1, 1), n),
        "term_date": np.full(n, np.datetime64("NaT"), dtype="datetime64[D]"),
        "manager_employee_sk": manager_sk,
        "status": np.full(n, "Active", dtype=object),
    }
    df = pl.DataFrame(data)
    # Managers have no manager.
    df = df.with_columns(
        pl.when(pl.col("role") == "Manager")
        .then(None)
        .otherwise(pl.col("manager_employee_sk"))
        .alias("manager_employee_sk")
    )
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_EMPLOYEE_SPEC.polars_schema()).select(DIM_EMPLOYEE_SPEC.column_names)
```

- [ ] **Step 4: Register `dim_employee`** — add import `from .dimensions.dim_employee import DIM_EMPLOYEE_SPEC, build_dim_employee` and REGISTRY entry `DIM_EMPLOYEE_SPEC.name: TableBuilder(spec=DIM_EMPLOYEE_SPEC, build=build_dim_employee),`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_employee.py tests/test_registry.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/techmart/dimensions/dim_employee.py src/techmart/registry.py tests/test_dim_employee.py
git commit -m "feat: add dim_employee dimension (SCD2 scaffolding, store FK)"
```

---

### Task 6: `dim_customer` (SCD2, high volume)

**Files:**
- Create: `src/techmart/dimensions/dim_customer.py`
- Modify: `src/techmart/registry.py`
- Test: `tests/test_dim_customer.py`

**Interfaces:**
- Produces: `DIM_CUSTOMER_SPEC`; `build_dim_customer(config) -> polars.DataFrame` (`num_customers` rows). Must stay fully vectorized (millions of rows at showcase/stress scale).

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_customer.py`:
```python
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_customer import DIM_CUSTOMER_SPEC, build_dim_customer
from techmart.framework.writer import validate_schema


def _cfg(num_customers: int) -> TechmartConfig:
    profile = ScaleProfile("t", 5, 10, 1, 1, num_customers, 4)
    return TechmartConfig(profile, 9, Path("data"), "techmart", "techmart_", date(2026, 1, 31))


def test_customer_rows_and_schema():
    df = build_dim_customer(_cfg(500))
    assert df.height == 500
    validate_schema(df, DIM_CUSTOMER_SPEC)


def test_customer_types_and_tiers_valid():
    df = build_dim_customer(_cfg(500))
    assert set(df["customer_type"].to_list()) <= {"Retail", "Commercial-B2B"}
    assert set(df["loyalty_tier"].to_list()) <= {"None", "Bronze", "Silver", "Gold", "Platinum"}


def test_customer_email_format():
    df = build_dim_customer(_cfg(50))
    assert all("@" in e for e in df["email"].to_list())


def test_customer_scd2_current():
    df = build_dim_customer(_cfg(100))
    assert df["is_current"].to_list() == [True] * 100
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_customer.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/dim_customer.py`:
```python
from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_TYPES = ["Retail", "Commercial-B2B"]
_TIERS = ["None", "Bronze", "Silver", "Gold", "Platinum"]
_SEGMENTS = ["DIY-Pro", "Gamer", "Home-Office", "SMB", "Household", "Student"]
_ACQUISITION = ["In-Store", "Web", "Mobile-App", "Marketplace", "Referral"]

_BASE_COLUMNS = [
    Column("customer_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("customer_id", "Utf8", "Business key", nullable=False),
    Column("customer_type", "Utf8", "Retail or Commercial-B2B"),
    Column("first_name", "Utf8", "First name"),
    Column("last_name", "Utf8", "Last name"),
    Column("email", "Utf8", "Email address (synthetic)"),
    Column("city", "Utf8", "City"),
    Column("state", "Utf8", "US state code"),
    Column("postal_code", "Utf8", "Postal code"),
    Column("loyalty_member_flag", "Boolean", "Enrolled in loyalty program"),
    Column("loyalty_tier", "Utf8", "Loyalty tier"),
    Column("loyalty_enroll_date", "Date", "Loyalty enrollment date; null if not a member"),
    Column("acquisition_channel", "Utf8", "Channel that acquired the customer"),
    Column("segment", "Utf8", "Marketing/merch segment"),
    Column("email_opt_in", "Boolean", "Opted in to marketing email"),
]

DIM_CUSTOMER_SPEC = TableSpec(
    schema="core",
    name="dim_customer",
    grain="one current row per customer (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_customer(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_customers
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    customer_id = support.business_keys("CUST", n, 8)
    first = support.sample(rng.stream("dim_customer.first"), support.FIRST_NAMES, n).astype(str)
    last = support.sample(rng.stream("dim_customer.last"), support.LAST_NAMES, n).astype(str)
    email = np.char.add(
        np.char.add(np.char.add(np.char.lower(first), "."), np.char.lower(last)),
        np.char.add(np.char.add(".", customer_id.astype(str)), "@example.com"),
    )
    member = rng.stream("dim_customer.member").integers(0, 2, n).astype(bool)
    tier_idx = rng.stream("dim_customer.tier").integers(1, len(_TIERS), n)  # 1..4 (skip "None")
    tier = np.where(member, np.asarray(_TIERS, dtype=object)[tier_idx], "None")
    enroll = support.random_dates(rng.stream("dim_customer.enroll"), date(2015, 1, 1), date(2025, 1, 1), n)
    enroll = np.where(member, enroll, np.datetime64("NaT"))
    data = {
        "customer_sk": sk,
        "customer_id": customer_id,
        "customer_type": support.sample(rng.stream("dim_customer.type"), _TYPES, n),
        "first_name": first,
        "last_name": last,
        "email": email,
        "city": support.sample(rng.stream("dim_customer.city"), support.CITIES, n),
        "state": support.sample(rng.stream("dim_customer.state"), support.US_STATES, n),
        "postal_code": rng.stream("dim_customer.postal").integers(10000, 99999, n).astype(str),
        "loyalty_member_flag": member,
        "loyalty_tier": tier,
        "loyalty_enroll_date": enroll,
        "acquisition_channel": support.sample(rng.stream("dim_customer.acq"), _ACQUISITION, n),
        "segment": support.sample(rng.stream("dim_customer.seg"), _SEGMENTS, n),
        "email_opt_in": rng.stream("dim_customer.optin").integers(0, 2, n).astype(bool),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_CUSTOMER_SPEC.polars_schema()).select(DIM_CUSTOMER_SPEC.column_names)
```

Note: `enroll` uses `datetime64[D]` with `NaT`; `np.where` on datetime64 preserves `NaT`, which Polars maps to null and `cast(Date)` keeps. `tier`/`enroll` `np.where` keep arrays vectorized (no Python loop).

- [ ] **Step 4: Register `dim_customer`** — add import `from .dimensions.dim_customer import DIM_CUSTOMER_SPEC, build_dim_customer` and REGISTRY entry `DIM_CUSTOMER_SPEC.name: TableBuilder(spec=DIM_CUSTOMER_SPEC, build=build_dim_customer),`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_customer.py tests/test_registry.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/techmart/dimensions/dim_customer.py src/techmart/registry.py tests/test_dim_customer.py
git commit -m "feat: add dim_customer dimension (SCD2 scaffolding, vectorized)"
```

---

### Task 7: `dim_promotion` (SCD2 scaffolding)

**Files:**
- Create: `src/techmart/dimensions/dim_promotion.py`
- Modify: `src/techmart/registry.py`
- Test: `tests/test_dim_promotion.py`

**Interfaces:**
- Produces: `DIM_PROMOTION_SPEC`; `build_dim_promotion(config) -> polars.DataFrame` (`num_promotions` rows). Promo `start_date`/`end_date` fall within the history window `[config.start_date, config.end_date]`.

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_promotion.py`:
```python
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion
from techmart.framework.writer import validate_schema


def _cfg(history_years: int) -> TechmartConfig:
    # num_promotions is derived as 60 * history_years.
    profile = ScaleProfile("t", 5, 10, history_years, 1, 8, 4)
    return TechmartConfig(profile, 11, Path("data"), "techmart", "techmart_", date(2026, 1, 31))


def test_promotion_rows_match_derived_count_and_schema():
    cfg = _cfg(2)  # 60 * 2 = 120
    df = build_dim_promotion(cfg)
    assert df.height == 120
    validate_schema(df, DIM_PROMOTION_SPEC)


def test_promotion_dates_within_history_and_ordered():
    cfg = _cfg(2)
    df = build_dim_promotion(cfg)
    assert df["start_date"].min() >= cfg.start_date
    assert (df["end_date"] >= df["start_date"]).all()
    assert df["end_date"].max() <= cfg.end_date


def test_promotion_types_valid():
    cfg = _cfg(1)
    df = build_dim_promotion(cfg)
    assert set(df["promo_type"].to_list()) <= {
        "Markdown", "BOGO", "Bundle", "Coupon", "Vendor-Funded",
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_promotion.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/dim_promotion.py`:
```python
from __future__ import annotations

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_PROMO_TYPES = ["Markdown", "BOGO", "Bundle", "Coupon", "Vendor-Funded"]
_DISCOUNT_METHODS = ["PercentOff", "AmountOff", "BuyOneGetOne"]
_CHANNEL_SCOPES = ["All", "In-Store", "Online"]
_FUNDING = ["Retailer", "Vendor"]

_BASE_COLUMNS = [
    Column("promotion_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("promotion_id", "Utf8", "Business key", nullable=False),
    Column("promo_name", "Utf8", "Promotion name"),
    Column("promo_type", "Utf8", "Markdown/BOGO/Bundle/Coupon/Vendor-Funded"),
    Column("discount_method", "Utf8", "Discount mechanism"),
    Column("discount_value", "Float64", "Discount value (percent or amount)"),
    Column("start_date", "Date", "Promotion start date"),
    Column("end_date", "Date", "Promotion end date"),
    Column("channel_scope", "Utf8", "Channels the promotion applies to"),
    Column("funding_source", "Utf8", "Retailer or Vendor funded"),
    Column("campaign_id", "Utf8", "Parent campaign business key"),
    Column("campaign_name", "Utf8", "Parent campaign name"),
]

DIM_PROMOTION_SPEC = TableSpec(
    schema="core",
    name="dim_promotion",
    grain="one current row per promotion/offer (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_promotion(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_promotions
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    span = (config.end_date - config.start_date).days
    durations = rng.stream("dim_promotion.dur").integers(3, 30, n)  # days
    start_offsets = rng.stream("dim_promotion.start").integers(0, max(span - 30, 1), n)
    start = np.datetime64(config.start_date) + start_offsets.astype("timedelta64[D]")
    end = start + durations.astype("timedelta64[D]")
    campaign_idx = rng.stream("dim_promotion.camp").integers(1, max(n // 4, 2), n)
    data = {
        "promotion_sk": sk,
        "promotion_id": support.business_keys("PROMO", n, 5),
        "promo_name": np.char.add("Promo ", support.business_keys("PROMO", n, 5)),
        "promo_type": support.sample(rng.stream("dim_promotion.type"), _PROMO_TYPES, n),
        "discount_method": support.sample(rng.stream("dim_promotion.method"), _DISCOUNT_METHODS, n),
        "discount_value": rng.stream("dim_promotion.val").integers(5, 50, n).astype(float),
        "start_date": start,
        "end_date": end,
        "channel_scope": support.sample(rng.stream("dim_promotion.scope"), _CHANNEL_SCOPES, n),
        "funding_source": support.sample(rng.stream("dim_promotion.fund"), _FUNDING, n),
        "campaign_id": np.char.add("CAMP", np.char.zfill(campaign_idx.astype(str), 4)),
        "campaign_name": np.char.add("Campaign ", np.char.zfill(campaign_idx.astype(str), 4)),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_PROMOTION_SPEC.polars_schema()).select(DIM_PROMOTION_SPEC.column_names)
```

Note: `end` may in rare cases exceed `config.end_date` by up to the duration; the test asserts `end_date.max() <= config.end_date`, so cap it: after computing `end`, clamp with `end = np.minimum(end, np.datetime64(config.end_date))`. Include that line before building `data`.

- [ ] **Step 4: Register `dim_promotion`** — add import `from .dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion` and REGISTRY entry `DIM_PROMOTION_SPEC.name: TableBuilder(spec=DIM_PROMOTION_SPEC, build=build_dim_promotion),`.

- [ ] **Step 5: Run the full suite and commit**

Run: `python -m pytest -q`
Expected: all tests pass (Phase 1 + 2A + all six new dimensions). Also smoke-test the CLI end-to-end for a new dimension:
`python -m techmart.cli --profile demo_lean --output-dir ./data --tables dim_channel,dim_store,dim_vendor,dim_employee,dim_customer,dim_promotion`
Expected: prints one `wrote data/core/<dim>.parquet` line per table.

```bash
git add src/techmart/dimensions/dim_promotion.py src/techmart/registry.py tests/test_dim_promotion.py
git commit -m "feat: add dim_promotion dimension (SCD2 scaffolding)"
```

---

## Self-Review

**1. Spec coverage (Phase 2B slice):**
- Shared vectorized helpers + reference lists → Task 1. ✅
- `dim_channel` (fixed reference) → Task 2. ✅
- `dim_store` (SCD2, conformed dim) → Task 3. ✅
- `dim_vendor` (SCD2, vendor-relationship story) → Task 4. ✅
- `dim_employee` (SCD2, store FK, manager hierarchy) → Task 5. ✅
- `dim_customer` (SCD2, high-volume, loyalty) → Task 6. ✅
- `dim_promotion` (SCD2 scaffolding, history-bounded dates) → Task 7. ✅
- Each dimension registered in `REGISTRY` and exercised via `validate_schema`/CLI. ✅
- Deferred to Phase 2C: `dim_product` (taxonomy hierarchy + vendor FK + spec attributes).

**2. Placeholder scan:** No TBD/TODO; every code and test step contains complete, runnable content. The two inline "Note:" clauses (dim_vendor spec ordering; dim_promotion end-date clamp) give exact instructions, not placeholders.

**3. Type consistency:** Every builder follows the same signature `build_dim_x(config: TechmartConfig) -> pl.DataFrame` matching `TableBuilder.build`; each `DIM_*_SPEC` composes `_BASE_COLUMNS + scd2_columns()` (except `dim_channel`); the build pattern `cast(SPEC.polars_schema()).select(SPEC.column_names)` and `with_scd2_current(df, config.start_date)` are used identically; `support.*` helper signatures match Task 1. FK draws use `config.scale_profile.num_stores`. Tests construct a tiny `TechmartConfig`/`ScaleProfile` directly (7 positional fields: name, num_stores, num_skus, history_years, sales_lines_target, num_customers, num_vendors) — matching Phase 2A's dataclass.

---

## Carry-forward notes (from Phase 2A final review)

- Dtype validation is class-level only; builders emit correct dtypes via the final `cast`, so no parametric drift is introduced here.
- All SCD2 timestamps are microsecond (`us`) precision via `with_scd2_current`.
- A parametrized test over all three YAML profiles' sizing values remains a nice-to-have (not required for these builders, which use handcrafted tiny configs in tests).

## Next plan

**Phase 2C — `dim_product`:** the 6-level hierarchy from the curated taxonomy (`subcategory_paths()`), brand assignment (department-scoped), `spec_attributes` (JSON), `primary_vendor_sk` FK into `[1, num_vendors]`, pricing/cost, lifecycle, and SCD2 scaffolding — registered in `REGISTRY`.
