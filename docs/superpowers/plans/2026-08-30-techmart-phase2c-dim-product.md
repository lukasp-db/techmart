# Techmart Phase 2C — dim_product — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `dim_product` — the 6-level merchandise hierarchy dimension — completing Techmart's conformed dimensions.

**Architecture:** Two tasks. Task 1 adds a vectorized taxonomy-assignment helper that maps N SKUs to root-to-leaf taxonomy paths (`subcategory_paths()` from Phase 2A) and picks a department-scoped brand per SKU, returning the nine hierarchy/brand arrays. Task 2 builds `dim_product` on that helper plus generated identity, pricing, spec, and lifecycle attributes, registers it in `REGISTRY`, and applies SCD2 current-row scaffolding. Fully vectorized (NumPy + Polars) so it scales to hundreds of thousands of SKUs.

**Tech Stack:** Python ≥ 3.10, Polars, NumPy, pytest.

## Global Constraints

_Every task's requirements implicitly include this section._

- **Language/floor:** Python ≥ 3.10.
- **Naming:** `snake_case`; surrogate key `product_sk` Int64 sequential `1..num_skus`; business key `sku` zero-padded string.
- **Comments:** every column carries a comment on its `Column`.
- **Determinism:** seeded via `SeededRng(config.seed).stream(name)`; same config+seed ⇒ identical output.
- **Vectorized only:** no Python per-row loop over `num_skus`. (Small fixed-size loops over the ~49 taxonomy paths / ~6 brands are allowed — they are O(paths), not O(rows).)
- **SCD2 policy (Phase 2):** current rows only via `with_scd2_current(df, config.start_date)`.
- **Referential integrity:** `primary_vendor_sk` drawn as `rng.integers(1, num_vendors + 1, n)` ∈ `[1, num_vendors]` (valid because `dim_vendor` uses sequential SKs). Hierarchy/brand values come from the real Phase 2A taxonomy — no fabricated hierarchy strings.
- **Build pattern:** dict of arrays → `pl.DataFrame(data)` → `with_scd2_current(df, config.start_date)` → `return df.cast(SPEC.polars_schema()).select(SPEC.column_names)`.
- **np.char rule:** `np.char.*` requires string-dtype arrays; convert object-dtype arrays (from `support.sample` / taxonomy assignment) with `.astype(str)` before passing them to `np.char`.
- **Registration:** register `dim_product` in `techmart.registry.REGISTRY` (direct builder, no wrapper).
- **Secret-free; serverless-compatible pure Python + Polars. Target schema `core` (unprefixed).**

---

### Task 1: Vectorized taxonomy assignment helper

**Files:**
- Create: `src/techmart/dimensions/product_support.py`
- Test: `tests/test_product_support.py`

**Interfaces:**
- Consumes: `subcategory_paths` from `techmart.reference.taxonomy`; NumPy.
- Produces:
  - `assign_taxonomy(rng_path: numpy.random.Generator, rng_brand: numpy.random.Generator, n: int) -> dict[str, numpy.ndarray]` — returns nine object-dtype arrays of length `n`, keyed exactly: `division_id`, `division_name`, `department_id`, `department_name`, `category_id`, `category_name`, `subcategory_id`, `subcategory_name`, `brand_name`. Each SKU is assigned one taxonomy path; `brand_name` is drawn from that path's category's (department-scoped) brand list. Fully vectorized over `n` (only fixed loops over the ~49 paths).
  - `COLORS: list[str]` (≥ 8 color names).

- [ ] **Step 1: Write the failing tests**

`tests/test_product_support.py`:
```python
import numpy as np

from techmart.dimensions.product_support import COLORS, assign_taxonomy
from techmart.reference.taxonomy import subcategory_paths


def test_assign_returns_nine_arrays_of_length_n():
    out = assign_taxonomy(np.random.default_rng(1), np.random.default_rng(2), 100)
    expected = {
        "division_id", "division_name", "department_id", "department_name",
        "category_id", "category_name", "subcategory_id", "subcategory_name",
        "brand_name",
    }
    assert set(out) == expected
    assert all(len(v) == 100 for v in out.values())


def test_assign_is_deterministic():
    a = assign_taxonomy(np.random.default_rng(1), np.random.default_rng(2), 50)
    b = assign_taxonomy(np.random.default_rng(1), np.random.default_rng(2), 50)
    assert all(np.array_equal(a[k], b[k]) for k in a)


def test_hierarchy_ids_are_internally_consistent():
    # SUB id = "SUB"+dd+pp+cc+ss ; CAT id = "CAT"+dd+pp+cc — so the category's
    # digits must be the prefix of the subcategory's digits.
    out = assign_taxonomy(np.random.default_rng(3), np.random.default_rng(4), 200)
    for cat_id, sub_id in zip(out["category_id"], out["subcategory_id"]):
        assert sub_id[3:9] == cat_id[3:9]


def test_brand_belongs_to_assigned_category():
    # Build the valid (category_id -> brands) map from the taxonomy and verify
    # every row's brand is legal for its category.
    valid = {}
    for _div, _dep, cat, _sub in subcategory_paths():
        valid[cat.id] = set(cat.brands)
    out = assign_taxonomy(np.random.default_rng(5), np.random.default_rng(6), 300)
    for cat_id, brand in zip(out["category_id"], out["brand_name"]):
        assert brand in valid[cat_id]


def test_colors_present():
    assert len(COLORS) >= 8
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_product_support.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.dimensions.product_support'`.

- [ ] **Step 3: Write the implementation**

`src/techmart/dimensions/product_support.py`:
```python
from __future__ import annotations

import numpy as np

from ..reference.taxonomy import subcategory_paths

COLORS = [
    "Black", "Silver", "White", "Space Gray", "Blue",
    "Red", "Graphite", "Rose Gold", "Green", "Titanium",
]


def assign_taxonomy(
    rng_path: np.random.Generator,
    rng_brand: np.random.Generator,
    n: int,
) -> dict[str, np.ndarray]:
    """Assign each of n SKUs to a taxonomy path and a department-scoped brand.

    Vectorized over n: the only loops are over the fixed set of taxonomy paths
    (~49) and the per-path brand lists (~6), never over the row count.
    """
    paths = subcategory_paths()
    num_paths = len(paths)

    # Per-path attribute lookup arrays (length = num_paths).
    div_id = np.array([p[0].id for p in paths], dtype=object)
    div_name = np.array([p[0].name for p in paths], dtype=object)
    dep_id = np.array([p[1].id for p in paths], dtype=object)
    dep_name = np.array([p[1].name for p in paths], dtype=object)
    cat_id = np.array([p[2].id for p in paths], dtype=object)
    cat_name = np.array([p[2].name for p in paths], dtype=object)
    sub_id = np.array([p[3].id for p in paths], dtype=object)
    sub_name = np.array([p[3].name for p in paths], dtype=object)

    # Padded brand matrix so brands can be gathered by 2D fancy indexing.
    brand_lists = [p[2].brands for p in paths]
    num_brands = np.array([len(b) for b in brand_lists], dtype=np.int64)
    max_brands = int(num_brands.max())
    brand_matrix = np.empty((num_paths, max_brands), dtype=object)
    for i, brands in enumerate(brand_lists):
        for j in range(max_brands):
            brand_matrix[i, j] = brands[j] if j < len(brands) else brands[0]

    path_idx = rng_path.integers(0, num_paths, n)
    # Per-element upper bound: brand index stays within the path's brand count.
    brand_idx = rng_brand.integers(0, num_brands[path_idx])
    brand = brand_matrix[path_idx, brand_idx]

    return {
        "division_id": div_id[path_idx],
        "division_name": div_name[path_idx],
        "department_id": dep_id[path_idx],
        "department_name": dep_name[path_idx],
        "category_id": cat_id[path_idx],
        "category_name": cat_name[path_idx],
        "subcategory_id": sub_id[path_idx],
        "subcategory_name": sub_name[path_idx],
        "brand_name": brand,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_product_support.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/dimensions/product_support.py tests/test_product_support.py
git commit -m "feat: add vectorized taxonomy assignment helper for products"
```

---

### Task 2: `dim_product` builder (SCD2)

**Files:**
- Create: `src/techmart/dimensions/dim_product.py`
- Modify: `src/techmart/registry.py`
- Test: `tests/test_dim_product.py`

**Interfaces:**
- Consumes: framework `Column`/`TableSpec`, `scd2_columns`/`with_scd2_current`; `SeededRng`; `support` (business_keys, surrogate_keys, sample, random_dates); `product_support` (assign_taxonomy, COLORS); `TechmartConfig`.
- Produces: `DIM_PRODUCT_SPEC: TableSpec`; `build_dim_product(config) -> polars.DataFrame` (`config.scale_profile.num_skus` rows). `primary_vendor_sk` FK ∈ `[1, num_vendors]`.

- [ ] **Step 1: Write the failing tests**

`tests/test_dim_product.py`:
```python
import json
from datetime import date
from pathlib import Path

import polars as pl

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product
from techmart.framework.writer import validate_schema


def _cfg(num_skus: int, num_vendors: int = 20) -> TechmartConfig:
    profile = ScaleProfile("t", 5, num_skus, 1, 1, 8, num_vendors)
    return TechmartConfig(profile, 7, Path("data"), "techmart", "techmart_", date(2026, 1, 31))


def test_product_rows_and_schema():
    df = build_dim_product(_cfg(500))
    assert df.height == 500
    validate_schema(df, DIM_PRODUCT_SPEC)


def test_product_keys_sequential_and_scd2():
    df = build_dim_product(_cfg(300))
    assert df["product_sk"].to_list() == list(range(1, 301))
    assert df["is_current"].to_list() == [True] * 300
    assert df["effective_end_ts"].null_count() == 300


def test_primary_vendor_fk_in_range():
    df = build_dim_product(_cfg(400, num_vendors=10))
    fks = df["primary_vendor_sk"].to_list()
    assert all(1 <= v <= 10 for v in fks)


def test_spec_attributes_is_valid_json():
    df = build_dim_product(_cfg(50))
    for s in df["spec_attributes"].to_list():
        parsed = json.loads(s)  # raises if not valid JSON
        assert "color" in parsed


def test_prices_positive_and_cost_below_msrp():
    df = build_dim_product(_cfg(300))
    assert (df["msrp"] > 0).all()
    assert (df["standard_cost"] <= df["msrp"]).all()


def test_discontinue_date_only_for_discontinued():
    df = build_dim_product(_cfg(500))
    active = df.filter(pl.col("lifecycle_status") != "Discontinued")
    assert active["discontinue_date"].null_count() == active.height


def test_is_deterministic():
    a = build_dim_product(_cfg(200))
    b = build_dim_product(_cfg(200))
    assert a.equals(b)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dim_product.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.dimensions.dim_product'`.

- [ ] **Step 3: Write the builder**

`src/techmart/dimensions/dim_product.py`:
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
from .product_support import COLORS, assign_taxonomy

_UOMS = ["EA", "EA", "EA", "PK", "BX"]  # weighted toward "each"
_LIFECYCLE = ["Active", "Active", "Active", "Active", "Clearance", "Discontinued"]

_BASE_COLUMNS = [
    Column("product_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("sku", "Utf8", "Business key (stock-keeping unit)", nullable=False),
    Column("gtin", "Utf8", "Global trade item number (barcode)"),
    Column("model_number", "Utf8", "Manufacturer model number"),
    Column("product_name", "Utf8", "Product display name"),
    Column("product_description", "Utf8", "Rich product description (for GenAI/search)"),
    Column("manufacturer", "Utf8", "Manufacturer name"),
    Column("brand_id", "Utf8", "Brand business key (slug of brand name)"),
    Column("brand_name", "Utf8", "Brand name (hierarchy level 5)"),
    Column("division_id", "Utf8", "Division business key (hierarchy level 1)"),
    Column("division_name", "Utf8", "Division name"),
    Column("department_id", "Utf8", "Department business key (level 2)"),
    Column("department_name", "Utf8", "Department name"),
    Column("category_id", "Utf8", "Category business key (level 3)"),
    Column("category_name", "Utf8", "Category name"),
    Column("subcategory_id", "Utf8", "Subcategory business key (level 4)"),
    Column("subcategory_name", "Utf8", "Subcategory name"),
    Column("primary_vendor_sk", "Int64", "Primary vendor (FK to dim_vendor)"),
    Column("private_label_flag", "Boolean", "Techmart private-label product"),
    Column("is_marketplace", "Boolean", "Sold by a 3rd-party marketplace seller"),
    Column("marketplace_seller_id", "Utf8", "Marketplace seller id; null if first-party"),
    Column("uom", "Utf8", "Unit of measure"),
    Column("color", "Utf8", "Primary color"),
    Column("spec_attributes", "Utf8", "JSON of product specification attributes"),
    Column("weight_kg", "Float64", "Weight in kilograms"),
    Column("dimensions", "Utf8", "Package dimensions LxWxH (cm)"),
    Column("msrp", "Float64", "Manufacturer suggested retail price"),
    Column("list_price", "Float64", "Current list price"),
    Column("standard_cost", "Float64", "Standard unit cost"),
    Column("lifecycle_status", "Utf8", "Active/Clearance/Discontinued"),
    Column("launch_date", "Date", "Product launch date"),
    Column("discontinue_date", "Date", "Discontinuation date; null unless discontinued"),
]

DIM_PRODUCT_SPEC = TableSpec(
    schema="core",
    name="dim_product",
    grain="one current row per SKU (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_product(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_skus
    num_vendors = config.scale_profile.num_vendors
    rng = SeededRng(config.seed)

    tax = assign_taxonomy(rng.stream("dim_product.path"), rng.stream("dim_product.brand"), n)
    brand = tax["brand_name"].astype(str)
    subcat = tax["subcategory_name"].astype(str)

    sku = support.business_keys("SKU", n, 8)
    model_number = support.business_keys("MDL", n, 8)
    brand_id = np.char.upper(np.char.replace(brand, " ", ""))
    color = support.sample(rng.stream("dim_product.color"), COLORS, n).astype(str)

    weight = np.round(rng.stream("dim_product.weight").uniform(0.1, 20.0, n), 2)
    msrp = np.round(rng.stream("dim_product.msrp").uniform(9.99, 2999.99, n), 2)
    list_price = np.round(msrp * (1.0 - rng.stream("dim_product.disc").uniform(0.0, 0.15, n)), 2)
    standard_cost = np.round(msrp * rng.stream("dim_product.cost").uniform(0.5, 0.8, n), 2)

    length = rng.stream("dim_product.len").integers(5, 60, n)
    width = rng.stream("dim_product.wid").integers(5, 40, n)
    height = rng.stream("dim_product.hgt").integers(1, 30, n)
    dims = np.char.add(np.char.add(np.char.add(np.char.add(
        length.astype(str), "x"), width.astype(str)), "x"), height.astype(str))

    product_name = np.char.add(np.char.add(np.char.add(np.char.add(
        brand, " "), subcat), " "), model_number)
    product_description = np.char.add(np.char.add(np.char.add(np.char.add(np.char.add(
        brand, " "), subcat), np.char.add(" (", np.char.add(color, "), model "))), model_number), ".")

    weight_str = np.char.mod("%.2f", weight)
    spec_attributes = np.char.add(np.char.add(np.char.add(np.char.add(np.char.add(
        '{"color":"', color), '","weight_kg":'), weight_str), ',"brand":"'),
        np.char.add(brand, '"}'))

    status = support.sample(rng.stream("dim_product.status"), _LIFECYCLE, n).astype(str)
    launch = support.random_dates(rng.stream("dim_product.launch"), date(2015, 1, 1), date(2024, 6, 1), n)
    disc_days = rng.stream("dim_product.disc_days").integers(30, 1000, n).astype("timedelta64[D]")
    discontinue = np.where(status == "Discontinued", launch + disc_days, np.datetime64("NaT"))

    is_marketplace = rng.stream("dim_product.mkt").random(n) < 0.15
    seller_num = rng.stream("dim_product.seller").integers(1, 200, n)
    seller_id = np.char.add("SELLER", np.char.zfill(seller_num.astype(str), 4))
    marketplace_seller_id = np.where(is_marketplace, seller_id, None)

    data = {
        "product_sk": support.surrogate_keys(n),
        "sku": sku,
        "gtin": rng.stream("dim_product.gtin").integers(100000000000, 1000000000000, n).astype(str),
        "model_number": model_number,
        "product_name": product_name,
        "product_description": product_description,
        "manufacturer": brand,
        "brand_id": brand_id,
        "brand_name": brand,
        "division_id": tax["division_id"],
        "division_name": tax["division_name"],
        "department_id": tax["department_id"],
        "department_name": tax["department_name"],
        "category_id": tax["category_id"],
        "category_name": tax["category_name"],
        "subcategory_id": tax["subcategory_id"],
        "subcategory_name": tax["subcategory_name"],
        "primary_vendor_sk": rng.stream("dim_product.vendor").integers(1, num_vendors + 1, n),
        "private_label_flag": rng.stream("dim_product.pl").random(n) < 0.1,
        "is_marketplace": is_marketplace,
        "marketplace_seller_id": marketplace_seller_id,
        "uom": support.sample(rng.stream("dim_product.uom"), _UOMS, n),
        "color": color,
        "spec_attributes": spec_attributes,
        "weight_kg": weight,
        "dimensions": dims,
        "msrp": msrp,
        "list_price": list_price,
        "standard_cost": standard_cost,
        "lifecycle_status": status,
        "launch_date": launch,
        "discontinue_date": discontinue,
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_PRODUCT_SPEC.polars_schema()).select(DIM_PRODUCT_SPEC.column_names)
```

- [ ] **Step 4: Register `dim_product`** (`src/techmart/registry.py`)

Add import `from .dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product` (alphabetically among the dim imports) and the REGISTRY entry `DIM_PRODUCT_SPEC.name: TableBuilder(spec=DIM_PRODUCT_SPEC, build=build_dim_product),`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_product.py tests/test_registry.py -v`
Expected: PASS (7 dim_product + 2 registry).

- [ ] **Step 6: Run the full suite and smoke-test the CLI, then commit**

Run: `python -m pytest -q`
Expected: all tests pass (Phase 1 + 2A + 2B + dim_product).
Run: `python -m techmart.cli --profile demo_lean --output-dir ./data --tables dim_product`
Expected: prints `wrote data/core/dim_product.parquet`.

```bash
git add src/techmart/dimensions/dim_product.py src/techmart/registry.py tests/test_dim_product.py
git commit -m "feat: add dim_product dimension (6-level taxonomy, SCD2 scaffolding)"
```

---

## Self-Review

**1. Spec coverage:**
- Vectorized taxonomy+brand assignment (hierarchy levels 1-5, department-scoped brands) → Task 1. ✅
- `dim_product` full column set: identity (sku/gtin/model), hierarchy (division→subcategory + brand), sourcing (primary_vendor_sk FK, private_label, marketplace), attributes (uom/color/spec_attributes JSON/weight/dimensions), economics (msrp/list_price/standard_cost), lifecycle (status/launch/discontinue), SCD2 scaffolding → Task 2. ✅
- Registered in `REGISTRY`; exercised via `validate_schema` + CLI. ✅
- After this, all conformed dimensions from the design spec exist.

**2. Placeholder scan:** No TBD/TODO; complete runnable code and tests throughout.

**3. Type consistency:** `assign_taxonomy(rng_path, rng_brand, n)` return keys match `dim_product`'s `tax[...]` reads; build pattern (`cast(SPEC.polars_schema()).select(SPEC.column_names)`, `with_scd2_current`) matches Phase 2A/2B; `np.char` operands are `.astype(str)`-converted (`brand`, `subcat`, `color`) or already `<U` (`business_keys`, `zfill`, `np.char.mod`); FK uses `config.scale_profile.num_vendors`. Tests construct the tiny `TechmartConfig`/`ScaleProfile` (7 positional fields) as in Phase 2B.

---

## Next plan

**Phase 3 — Core facts:** introduces the Spark/`dbldatagen` generation path (for large facts on serverless) and the DAB scaffold for the first workspace deploy; `fact_sales_line` first, then inventory/fulfillment/returns/web-events/loyalty. Carry-forward cosmetic minors from Phase 2B (region_id zero-pad, support docstrings, a couple of extra assertions) can be swept in opportunistically.
