# Sales Magnitude & Seasonality Recalibration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring TTM net sales from ~$1.19T to ~$25B with realistic per-category prices, a cheap-item-dominated unit mix, and a pronounced annual seasonality (Back-to-School, Cyber-5, July event, January trough).

**Architecture:** Category price bands + unit-share weights live in a new `reference/pricing.py`. `dim_product` draws `msrp` log-uniform within its category band. `fact_sales_line` selects products in two stages — category by unit-share weight, then a product within it via a broadcast join on a within-category index — and uses a realistic single-unit-skewed quantity. `date_seasonality_weights` is deepened and the calendar gains Cyber Monday. Everything stays deterministic (`uniform_hash`) and schema-unchanged.

**Tech Stack:** Python, PySpark, dbldatagen, pytest with a session-scoped local `spark` fixture (`tests/conftest.py`).

**Spec:** `docs/superpowers/specs/2026-09-09-sales-magnitude-seasonality-recalibration-design.md`

## Global Constraints

- Determinism: all per-row randomness via `uniform_hash(...)` from `src/techmart/facts/gen.py` (pure hash of stable keys). Never `rand()`.
- No schema changes: `DIM_PRODUCT_SPEC`, `FACT_SALES_LINE_SPEC`, `DIM_DATE_SPEC` column sets/order are fixed. `select_ordered(df)` projects to spec columns and drops helper columns automatically — no manual drops needed for helpers.
- Preserve RI and measure invariants: `product_sk`/`store_sk`/etc. valid FKs; `net = gross − discount`; `gross_margin = net − cogs`; `is_marketplace ⇔ channel_sk == 4`; `quantity ≥ 1`; no null `unit_price`; basket coherence (one store/date/customer/channel/employee per `transaction_id`).
- Pricing/weight knobs are shared across scale profiles (authored in `reference/pricing.py`), not per-profile. `sales_lines_target` is unchanged (750M at showcase).
- Prices derive from msrp exactly as today: `list_price = msrp × (1 − disc)` with disc ∈ [0, 0.15); `standard_cost = msrp × cost_pct` with cost_pct ∈ [0.5, 0.8).
- Run tests from repo root: `python -m pytest <path> -v`. The `spark` fixture is local + session-scoped.
- Calibration target: analytic msrp-blended line ≈ $80–100; implied TTM net ≈ $22–28B (locked by a unit test). Empirical confirmation happens at regeneration, not in tests.

---

### Task 1: Pricing reference — category price bands, unit-share weights, analytic calibration

**Files:**
- Create: `src/techmart/reference/pricing.py`
- Test: `tests/test_pricing.py`

**Interfaces:**
- Consumes: `TAXONOMY` from `src/techmart/reference/taxonomy.py` (walk categories by name → id).
- Produces:
  - `CATEGORY_PRICE_BANDS: dict[str, tuple[float, float]]` (keyed by category **name**)
  - `CATEGORY_UNIT_WEIGHTS: dict[str, float]` (keyed by category **name**)
  - `price_bands_by_id() -> dict[str, tuple[float, float]]`
  - `unit_weights_by_id() -> dict[str, float]`
  - `category_cdf() -> list[tuple[str, float]]` (category_id, cumulative-upper in (0,1], ordered; normalized over all authored categories)
  - `loguniform_mean(low: float, high: float) -> float`
  - `expected_msrp_blended() -> float`
  - `expected_ttm_net(lines_ttm: float, qty_mean: float, list_factor: float, promo_factor: float) -> float`

- [ ] **Step 1: Write the failing test** — `tests/test_pricing.py`

```python
from techmart.reference.taxonomy import TAXONOMY
from techmart.reference import pricing


def _all_category_names():
    return [cat.name for div in TAXONOMY for dep in div.departments for cat in dep.categories]


def test_every_category_has_band_and_weight():
    names = _all_category_names()
    assert len(names) == 24
    for n in names:
        assert n in pricing.CATEGORY_PRICE_BANDS, f"missing band: {n}"
        assert n in pricing.CATEGORY_UNIT_WEIGHTS, f"missing weight: {n}"
    # no stray keys
    assert set(pricing.CATEGORY_PRICE_BANDS) == set(names)
    assert set(pricing.CATEGORY_UNIT_WEIGHTS) == set(names)


def test_bands_valid_and_weights_positive():
    for n, (lo, hi) in pricing.CATEGORY_PRICE_BANDS.items():
        assert 0 < lo < hi, f"bad band for {n}: {lo},{hi}"
    for n, w in pricing.CATEGORY_UNIT_WEIGHTS.items():
        assert w > 0, f"non-positive weight for {n}"


def test_by_id_maps_resolve_to_taxonomy_ids():
    bands = pricing.price_bands_by_id()
    weights = pricing.unit_weights_by_id()
    ids = [cat.id for div in TAXONOMY for dep in div.departments for cat in dep.categories]
    assert set(bands) == set(ids)
    assert set(weights) == set(ids)


def test_category_cdf_is_normalized_and_ordered():
    cdf = pricing.category_cdf()
    assert len(cdf) == 24
    uppers = [u for _, u in cdf]
    assert uppers == sorted(uppers)
    assert abs(uppers[-1] - 1.0) < 1e-9


def test_loguniform_mean_between_bounds():
    m = pricing.loguniform_mean(5.0, 60.0)
    assert 5.0 < m < 60.0
    assert abs(m - (55.0 / __import__("math").log(12.0))) < 1e-6


def test_calibration_hits_25B_band():
    # msrp-blended average line must sit in the realistic electronics band.
    blended = pricing.expected_msrp_blended()
    assert 80.0 <= blended <= 100.0, f"blended msrp/line = {blended:.1f}"
    # implied trailing-12-month net at showcase (~270M TTM lines, qty ~1.16,
    # list = 0.925*msrp, promo factor 0.9736) lands near $25B.
    ttm = pricing.expected_ttm_net(lines_ttm=270_000_000, qty_mean=1.16,
                                   list_factor=0.925, promo_factor=0.9736)
    assert 22e9 <= ttm <= 28e9, f"implied TTM net = ${ttm/1e9:.1f}B"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pricing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'techmart.reference.pricing'`.

- [ ] **Step 3: Create `src/techmart/reference/pricing.py`**

```python
"""Category-level price bands and unit-share weights for realistic sales economics.

Bands and weights are authored by category *name* (readable) and resolved to
category *id* against the taxonomy. `dim_product` draws msrp log-uniform within a
product's category band; `fact_sales_line` picks a category by unit-share weight
(cheap categories dominate unit volume), then a product within it. Weights are
tuned so the blended average line lands in the realistic electronics band and TTM
net revenue at showcase scale is ~$25B.
"""
from __future__ import annotations

import math

from .taxonomy import TAXONOMY

# (low, high) msrp band per category, USD. Log-uniform within the band.
CATEGORY_PRICE_BANDS: dict[str, tuple[float, float]] = {
    "Gaming Laptops": (900.0, 3000.0),
    "Ultrabooks": (700.0, 2200.0),
    "Business Laptops": (500.0, 1800.0),
    "Gaming Desktops": (800.0, 3500.0),
    "All-in-Ones": (600.0, 2200.0),
    "Graphics Cards": (200.0, 2000.0),
    "Storage Drives": (40.0, 500.0),
    "Memory": (30.0, 300.0),
    "Mirrorless Cameras": (500.0, 4000.0),
    "Action Cameras": (150.0, 700.0),
    "Smartphones": (250.0, 1500.0),
    "Tablets": (150.0, 1300.0),
    "Inkjet Printers": (40.0, 400.0),
    "Laser Printers": (120.0, 700.0),
    "Refrigerators": (600.0, 3500.0),
    "Laundry": (400.0, 2200.0),
    "Kitchen": (25.0, 500.0),
    "Home": (60.0, 900.0),
    "Routers": (40.0, 700.0),
    "Switches": (25.0, 500.0),
    "Ethernet Cabling": (5.0, 60.0),
    "Connectors & Tools": (5.0, 90.0),
    "Protection Plans": (30.0, 400.0),
    "Installation": (60.0, 350.0),
}

# Relative unit-share weight per category. Cheap, high-turn categories carry the
# large majority of unit volume; big-ticket categories are rare but high-dollar.
CATEGORY_UNIT_WEIGHTS: dict[str, float] = {
    "Gaming Laptops": 2.0,
    "Ultrabooks": 2.0,
    "Business Laptops": 2.0,
    "Gaming Desktops": 1.0,
    "All-in-Ones": 1.0,
    "Graphics Cards": 3.0,
    "Storage Drives": 30.0,
    "Memory": 70.0,
    "Mirrorless Cameras": 1.0,
    "Action Cameras": 5.0,
    "Smartphones": 6.0,
    "Tablets": 4.0,
    "Inkjet Printers": 15.0,
    "Laser Printers": 5.0,
    "Refrigerators": 1.0,
    "Laundry": 2.0,
    "Kitchen": 40.0,
    "Home": 8.0,
    "Routers": 12.0,
    "Switches": 20.0,
    "Ethernet Cabling": 380.0,
    "Connectors & Tools": 320.0,
    "Protection Plans": 50.0,
    "Installation": 15.0,
}


def _name_to_id() -> dict[str, str]:
    return {
        cat.name: cat.id
        for div in TAXONOMY
        for dep in div.departments
        for cat in dep.categories
    }


def price_bands_by_id() -> dict[str, tuple[float, float]]:
    n2i = _name_to_id()
    return {n2i[name]: band for name, band in CATEGORY_PRICE_BANDS.items()}


def unit_weights_by_id() -> dict[str, float]:
    n2i = _name_to_id()
    return {n2i[name]: w for name, w in CATEGORY_UNIT_WEIGHTS.items()}


def category_cdf() -> list[tuple[str, float]]:
    """Ordered (category_id, cumulative-upper) over all categories, normalized to 1.0."""
    weights = unit_weights_by_id()
    ordered = sorted(weights.items())  # deterministic order by category_id
    total = sum(w for _, w in ordered)
    cdf: list[tuple[str, float]] = []
    acc = 0.0
    for cid, w in ordered:
        acc += w / total
        cdf.append((cid, acc))
    return cdf


def loguniform_mean(low: float, high: float) -> float:
    """Mean of a log-uniform draw on [low, high): (high-low)/ln(high/low)."""
    return (high - low) / math.log(high / low)


def expected_msrp_blended() -> float:
    """Unit-share-weighted mean msrp per sales line (uniform product pick within category)."""
    total_w = sum(CATEGORY_UNIT_WEIGHTS.values())
    return sum(
        (CATEGORY_UNIT_WEIGHTS[name] / total_w) * loguniform_mean(lo, hi)
        for name, (lo, hi) in CATEGORY_PRICE_BANDS.items()
    )


def expected_ttm_net(lines_ttm: float, qty_mean: float,
                     list_factor: float, promo_factor: float) -> float:
    """Analytic TTM net = lines * qty * (list_factor*msrp_blended) * promo_factor."""
    return lines_ttm * qty_mean * (list_factor * expected_msrp_blended()) * promo_factor
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_pricing.py -v`
Expected: PASS (6 tests). If `test_calibration_hits_25B_band` fails, adjust `CATEGORY_UNIT_WEIGHTS` (shift weight between cheap and mid categories) until `expected_msrp_blended()` is in [80, 100] — this is the intended tuning knob. Do not change bands to pass it.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/reference/pricing.py tests/test_pricing.py
git commit -m "Add category price bands + unit-share weights with \$25B calibration

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: `dim_product` — draw msrp log-uniform within category band

**Files:**
- Modify: `src/techmart/spark/dimensions/dim_product.py` (imports; remove the uniform msrp/list/cost dbldatagen columns; add post-join band pricing)
- Test: `tests/test_dim_product_spark.py` (add band + magnitude assertions), `tests/test_lookups.py` (unchanged assertions must still hold)

**Interfaces:**
- Consumes: `price_bands_by_id()` (Task 1); `uniform_hash` from `src/techmart/facts/gen.py`.
- Produces: `dim_product` with `msrp`/`list_price`/`standard_cost` drawn from each product's category band. Schema unchanged.

- [ ] **Step 1: Add the failing test** — append to `tests/test_dim_product_spark.py`

```python
def test_prices_within_category_bands_and_realistic(spark):
    from techmart.reference.pricing import price_bands_by_id
    df = build_dim_product(spark, _CFG)
    bands = price_bands_by_id()
    # bring bands into the frame to check per-row containment
    bands_rows = [(cid, lo, hi) for cid, (lo, hi) in bands.items()]
    bdf = spark.createDataFrame(bands_rows, "category_id string, lo double, hi double")
    j = df.join(bdf, "category_id", "left")
    assert j.filter(F.col("lo").isNull()).count() == 0  # every category has a band
    out = j.filter((F.col("msrp") < F.col("lo") - 0.01) | (F.col("msrp") > F.col("hi") + 0.01)).count()
    assert out == 0, "msrp outside its category band"
    # catalog mean is far below the old uniform ~1505/unit
    mean_lp = df.agg(F.avg("list_price")).first()[0]
    assert mean_lp < 900.0, f"catalog mean list_price still high: {mean_lp:.0f}"
    # list_price/standard_cost derive from msrp
    bad = df.filter(
        (F.col("list_price") > F.col("msrp") + 0.01)
        | (F.col("standard_cost") > F.col("msrp") + 0.01)
        | (F.col("standard_cost") <= 0) | (F.col("list_price") <= 0)
    ).count()
    assert bad == 0


def test_dim_product_pricing_deterministic(spark):
    a = build_dim_product(spark, _CFG).agg(F.round(F.sum("msrp"), 2)).first()[0]
    b = build_dim_product(spark, _CFG).agg(F.round(F.sum("msrp"), 2)).first()[0]
    assert a == b
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_dim_product_spark.py::test_prices_within_category_bands_and_realistic -v`
Expected: FAIL — msrp is currently uniform $9.99–$2,999.99, unrelated to category, so rows fall outside their bands and the catalog mean is ~$1,505.

- [ ] **Step 3: Edit `src/techmart/spark/dimensions/dim_product.py`**

3a. Add imports near the top (after the existing `from ..scd2 import ...` line):

```python
from ...facts.gen import uniform_hash
from ...reference.pricing import price_bands_by_id
```

3b. In `build_dim_product`, **remove** the uniform pricing columns from the dbldatagen chain (the block currently at lines 167–181):

```python
        # --- pricing: msrp first, then list_price and standard_cost derived from it ---
        .withColumn("msrp_raw", "double", minValue=9.99, maxValue=2999.99, random=True, omit=True)
        .withColumn("msrp", "double", expr="round(msrp_raw, 2)", baseColumn="msrp_raw")
        .withColumn("disc_pct", "double", minValue=0.0, maxValue=0.15, random=True, omit=True)
        .withColumn(
            "list_price", "double",
            expr="round(msrp * (1.0 - disc_pct), 2)",
            baseColumn=["msrp", "disc_pct"],
        )
        .withColumn("cost_pct", "double", minValue=0.5, maxValue=0.8, random=True, omit=True)
        .withColumn(
            "standard_cost", "double",
            expr="round(msrp * cost_pct, 2)",
            baseColumn=["msrp", "cost_pct"],
        )
```

Delete that entire block (all four `withColumn` pricing calls + the two omitted helper draws). Leave the surrounding `uom`/marketplace/lifecycle columns intact.

3c. After the "derive columns that depend on joined brand/subcategory data" block (the one that sets `manufacturer`, `product_name`, `product_description`, `spec_attributes`, ending around line 251), and **before** `df = with_scd2_current(...)`, insert band-based pricing:

```python
    # --- category-band pricing: msrp log-uniform within the product's category band ---
    bands = price_bands_by_id()
    bands_df = F.broadcast(
        spark.createDataFrame(
            [(cid, float(lo), float(hi)) for cid, (lo, hi) in bands.items()],
            "category_id string, price_low double, price_high double",
        )
    )
    df = df.join(bands_df, on="category_id", how="left")
    df = (
        df
        .withColumn("_u_msrp", uniform_hash(F.col("product_sk"), salt="msrp"))
        .withColumn(
            "msrp",
            F.round(F.col("price_low") * F.pow(F.col("price_high") / F.col("price_low"), F.col("_u_msrp")), 2),
        )
        .withColumn("_disc", uniform_hash(F.col("product_sk"), salt="disc") * F.lit(0.15))
        .withColumn("list_price", F.round(F.col("msrp") * (F.lit(1.0) - F.col("_disc")), 2))
        .withColumn("_cost_pct", F.lit(0.5) + uniform_hash(F.col("product_sk"), salt="cost") * F.lit(0.3))
        .withColumn("standard_cost", F.round(F.col("msrp") * F.col("_cost_pct"), 2))
    )
```

(`select_ordered` at the return drops the `price_low`/`price_high`/`_u_msrp`/`_disc`/`_cost_pct` helpers. `category_id` is already present from the taxonomy lookup join.)

- [ ] **Step 4: Run the dim_product + lookups tests**

Run: `python -m pytest tests/test_dim_product_spark.py tests/test_lookups.py -v`
Expected: PASS — new band/magnitude/determinism tests pass; existing `test_dim_product` (schema, hierarchy, vendor range, discontinue) and `test_product_economics_one_row_per_sku` (min price > 0) still pass.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/spark/dimensions/dim_product.py tests/test_dim_product_spark.py
git commit -m "Price dim_product from category bands (log-uniform within band)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: `fact_sales_line` — two-stage category-weighted selection + realistic quantity

**Files:**
- Modify: `src/techmart/facts/fact_sales_line.py`
- Test: `tests/test_fact_sales_line.py` (adjust RI bound source; add quantity + category-share tests)

**Interfaces:**
- Consumes: `dim_product` with `category_id` + economics (Task 2); `unit_weights_by_id` from `reference/pricing.py`; `uniform_hash` from `facts/gen.py`.
- Produces: `build_fact_sales_line(...)` unchanged signature; product selection is now category-weighted two-stage; quantity averages ~1.16.

- [ ] **Step 1: Add/adjust tests** — `tests/test_fact_sales_line.py`

Add these tests (keep the existing schema/RI/measures/basket/determinism tests — they remain valid):

```python
def test_quantity_is_single_unit_skewed(spark):
    df = _build(spark, rows=6000)
    qmean = df.agg(F.avg("quantity")).first()[0]
    assert 1.05 <= qmean <= 1.35, f"quantity mean {qmean:.2f} not single-unit-skewed"
    assert df.filter(F.col("quantity") < 1).count() == 0


def test_cheap_categories_outsell_expensive(spark):
    # At a scale where every category has products, the top unit-weight category
    # (Ethernet Cabling) must sell more lines than a big-ticket one (Gaming Laptops).
    from techmart.config import ScaleProfile, TechmartConfig
    from pathlib import Path
    sp = ScaleProfile("t", 20, 480, 1, 20000, 400, 20)  # 480 skus over 24 cats => ~20 each
    cfg = TechmartConfig(scale_profile=sp, seed=42, output_dir=Path("data"),
                         catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
    counts = {"store": 20, "customer": 400, "employee": sp.num_employees,
              "promotion": sp.num_promotions, "product": 480}
    dp = build_dim_product(spark, cfg)
    dd = build_dim_date(spark, cfg)
    df = build_fact_sales_line(spark, cfg, dim_product=dp, dim_date=dd, dim_counts=counts, rows=20000)
    j = df.join(dp.select("product_sk", "category_name"), "product_sk")
    by_cat = {r["category_name"]: r["n"] for r in
              j.groupBy("category_name").agg(F.count("*").alias("n")).collect()}
    assert by_cat.get("Ethernet Cabling", 0) > by_cat.get("Gaming Laptops", 0)
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_fact_sales_line.py::test_quantity_is_single_unit_skewed -v`
Expected: FAIL — quantity is currently uniform 1–5 (mean ~3.0).

- [ ] **Step 3: Edit `src/techmart/facts/fact_sales_line.py`**

3a. Add imports at the top (with the other imports):

```python
from pyspark.sql import Window
from ..reference.pricing import unit_weights_by_id
```

(`product_economics` is no longer used by this module — remove it from the `from .lookups import ...` line, keeping `date_seasonality_weights`.)

3b. Replace the per-line product/quantity selection + economics join (the block currently spanning roughly lines 112–162, from `num_products = dim_counts["product"]` through the `joined = lines.join(econ, ...)` line) with the two-stage selection:

```python
    num_promotions = dim_counts["promotion"]

    def _u(salt: str) -> "Column":  # noqa: F821
        """Uniform pseudo-random double in [0, 1) keyed on (txn, line, salt)."""
        return uniform_hash(F.col("transaction_id"), F.col("line_number"), salt=salt)

    # --- product lookup: within-category index + economics (broadcast) ---
    win = Window.partitionBy("category_id").orderBy("product_sk")
    lookup = (
        dim_product.select("product_sk", "category_id", "list_price", "standard_cost")
        .withColumn("cat_local_idx", (F.row_number().over(win) - F.lit(1)).cast("long"))
    )
    cat_size = lookup.groupBy("category_id").agg(F.count("*").alias("cat_size"))

    # --- category CDF over the categories actually present, using authored weights ---
    weights = unit_weights_by_id()
    present = [r["category_id"] for r in cat_size.select("category_id").orderBy("category_id").collect()]
    ws = [(c, float(weights.get(c, 0.0))) for c in present if weights.get(c, 0.0) > 0]
    total_w = sum(w for _, w in ws)
    acc = 0.0
    cdf = []
    for c, w in ws:
        acc += w / total_w
        cdf.append((c, acc))

    # --- stage 1: pick category by weight (inverse-CDF when-chain on u1) ---
    u1 = _u("cat")
    cat_expr = F.lit(cdf[-1][0])
    for c, upper in reversed(cdf[:-1]):
        cat_expr = F.when(u1 < F.lit(upper), F.lit(c)).otherwise(cat_expr)
    lines = lines.withColumn("category_id", cat_expr)

    # --- stage 2: pick a product within the category (local index -> join) ---
    lines = (
        lines.join(F.broadcast(cat_size), "category_id")
        .withColumn("cat_local_idx", F.floor(_u("prod") * F.col("cat_size")).cast("long"))
        .drop("cat_size")
    )
    lines = lines.join(F.broadcast(lookup), ["category_id", "cat_local_idx"], "left")

    # --- realistic quantity: single-unit skewed (avg ~1.16) ---
    uq = _u("q")
    lines = lines.withColumn(
        "quantity",
        F.when(uq < F.lit(0.88), F.lit(1))
        .when(uq < F.lit(0.97), F.lit(2))
        .when(uq < F.lit(0.99), F.lit(3))
        .otherwise(F.lit(4))
        .cast("int"),
    )

    # --- promotion + tender (unchanged logic) ---
    lines = (
        lines
        .withColumn(
            "promotion_sk",
            F.when(
                _u("pr") < 0.22,
                (
                    F.pmod(
                        F.hash(F.col("transaction_id"), F.col("line_number"), F.lit("ps")),
                        F.lit(num_promotions),
                    )
                    + 1
                ).cast("long"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "tender_type",
            F.element_at(
                F.array(
                    F.lit("Card"), F.lit("Card"), F.lit("Card"),
                    F.lit("Cash"), F.lit("Gift Card"), F.lit("Mobile Pay"),
                ),
                (F.pmod(F.hash(F.col("transaction_id"), F.col("line_number"), F.lit("t")), 6) + 1),
            ),
        )
    )
```

3c. The measure-derivation block that follows currently starts by joining `econ` and referencing `list_price`/`standard_cost`. Since `list_price`/`standard_cost` now arrive on `lines` from the `lookup` join, remove the `econ`/`joined` join and derive measures directly from `lines`. Replace the `joined = lines.join(econ, ...)` + `df = (joined ...)` opening with:

```python
    df = (
        lines
        .withColumn("unit_price", F.round(F.col("list_price"), 2))
        .withColumn("unit_cost", F.round(F.col("standard_cost"), 2))
        .withColumn("receipt_id", F.concat(F.lit("RCPT-"), F.col("transaction_id").cast("string")))
        .withColumn("gross_sales_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
```

(Keep the rest of the measure chain — discount, net, tax, cogs, gross_margin, loyalty_points, is_return, is_marketplace — exactly as-is. `select_ordered` drops `category_id`/`cat_local_idx`/`list_price`/`standard_cost` helpers.)

- [ ] **Step 4: Run the sales-line + date-spread tests**

Run: `python -m pytest tests/test_fact_sales_line.py tests/test_fact_sales_line_date_spread.py -v`
Expected: PASS — RI (`product_sk` valid, no null price), measures, basket coherence, determinism, new quantity-skew test, and cheap-outsell-expensive test all pass.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/facts/fact_sales_line.py tests/test_fact_sales_line.py
git commit -m "Two-stage category-weighted product selection + realistic quantity

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: Seasonality — Cyber Monday + deepened weight profile

**Files:**
- Modify: `src/techmart/spark/calendar.py` (add Cyber Monday)
- Modify: `src/techmart/facts/lookups.py` (`date_seasonality_weights`)
- Test: `tests/test_lookups.py` (seasonality assertions), `tests/test_dim_date_spark.py` (Cyber Monday recognized — additive, existing Christmas assertion unchanged)

**Interfaces:**
- Consumes: `dim_date` columns `selling_season`, `holiday_name`, `is_weekend`, `year`, `month`, `day` (derive day-of-month from `date`), `fiscal_week`.
- Produces: deepened integer weights per `date_sk`; `holiday_name` returns `"Cyber Monday"` on Thanksgiving+4. Feeds both `fact_sales_line` and `fact_web_events` (no change needed there).

- [ ] **Step 1: Add failing tests**

In `tests/test_lookups.py`, replace `test_date_weights_cover_calendar_and_are_positive` with a stronger version and add a Cyber-5 test:

```python
def test_date_weights_cover_calendar_and_are_positive(spark):
    dim = build_dim_date(spark, _CONFIG)
    date_sks, weights = date_seasonality_weights(dim)
    total_days = dim.count()
    assert len(date_sks) == total_days == len(weights)
    assert min(weights) >= 1
    assert date_sks == sorted(date_sks)
    assert max(weights) > 100


def test_cyber5_and_seasons_are_pronounced(spark):
    from pyspark.sql import functions as F
    dim = build_dim_date(spark, _CONFIG)
    date_sks, weights = date_seasonality_weights(dim)
    w = dict(zip(date_sks, weights))
    rows = {r["date_sk"]: r for r in dim.select(
        "date_sk", "holiday_name", "selling_season", "month").collect()}
    bf = [sk for sk, r in rows.items() if r["holiday_name"] == "Black Friday"]
    cm = [sk for sk, r in rows.items() if r["holiday_name"] == "Cyber Monday"]
    assert bf and cm, "Black Friday / Cyber Monday not present in calendar"
    # Cyber-5 peak days are the heaviest in the year.
    peak = max(w.values())
    assert w[bf[0]] >= 0.8 * peak and w[cm[0]] >= 0.8 * peak
    # Post-holiday trough (Jan/Feb) sits below baseline 100.
    trough = [sk for sk, r in rows.items() if r["selling_season"] == "Post-Holiday"]
    assert min(w[sk] for sk in trough) < 100
```

In `tests/test_dim_date_spark.py`, add (leave the existing Christmas assertion intact):

```python
def test_cyber_monday_recognized(spark):
    df = build_dim_date(spark, _CFG)
    # 2025 Thanksgiving = Nov 27; Cyber Monday = Dec 1, 2025 -> date_sk 20251201
    row = df.filter(F.col("date_sk") == 20251201).first()
    assert row is not None and row["holiday_name"] == "Cyber Monday"
```

(Confirm `_CFG` in `test_dim_date_spark.py` spans that date — its `end_date` is 2026-01-31 with ≥1 history year, so Dec 2025 is included.)

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_lookups.py::test_cyber5_and_seasons_are_pronounced tests/test_dim_date_spark.py::test_cyber_monday_recognized -v`
Expected: FAIL — `holiday_name` has no "Cyber Monday"; current weights don't drive a trough below 100 or a Cyber-5 peak.

- [ ] **Step 3a: Add Cyber Monday to `src/techmart/spark/calendar.py`**

In `holiday_name`, after the Black Friday branch, add:

```python
    if d == thanksgiving + timedelta(days=4):
        return "Cyber Monday"
```

- [ ] **Step 3b: Deepen `date_seasonality_weights` in `src/techmart/facts/lookups.py`**

Replace the body of `date_seasonality_weights` with a richer profile. It keeps the two-parallel-list return contract:

```python
def date_seasonality_weights(dim_date: DataFrame) -> tuple[list[int], list[int]]:
    """Integer sampling weights per ``date_sk`` with a pronounced retail cycle.

    Baseline 100, modulated by weekends, Back-to-School, a December ramp toward
    Christmas, a sharp Cyber-5 peak (Black Friday / Cyber Monday), a mid-July sale
    event, and a post-holiday trough, plus mild YoY growth. Returned as two
    parallel lists (ordered by ``date_sk``) for dbldatagen ``values=``/``weights=``.
    """
    min_year = dim_date.agg(F.min("year")).collect()[0][0]
    day_of_month = F.dayofmonth(F.col("date"))

    # Base seasonal stack (multiplicative, capped before the event override).
    base = (
        F.lit(100.0)
        * F.when(F.col("is_weekend"), 1.4).otherwise(1.0)
        * F.when(F.col("selling_season") == "Back-to-School", 2.2)
        .when(F.col("selling_season") == "Holiday", 2.0)
        .when(F.col("selling_season") == "Post-Holiday", 0.7)
        .otherwise(1.0)
        # December ramp toward Christmas (overrides the flat Holiday 2.0 upward).
        * F.when(F.col("month") == 12, 1.0 + 0.06 * F.least(day_of_month, F.lit(25)))
        .otherwise(1.0)
        # Mid-July sale event (~Prime Day window).
        * F.when((F.col("month") == 7) & (day_of_month >= 10) & (day_of_month <= 16), 2.0)
        .otherwise(1.0)
        * (1.0 + 0.08 * (F.col("year") - F.lit(min_year)))
    )

    # Cyber-5 event override: take the max of the seasonal stack and a fixed spike
    # so the peak days dominate without runaway multiplicative stacking.
    event = (
        F.when(F.col("holiday_name") == "Black Friday", 500.0)
        .when(F.col("holiday_name") == "Cyber Monday", 500.0)
        .when(F.col("holiday_name") == "Thanksgiving", 250.0)
        .otherwise(F.lit(0.0))
    )

    weighted = (
        dim_date.select(
            "date_sk",
            F.greatest(base, event).alias("w"),
        )
        .withColumn("w", F.greatest(F.round("w").cast("int"), F.lit(1)))
        .orderBy("date_sk")
        .collect()
    )
    date_sks = [r["date_sk"] for r in weighted]
    weights = [r["w"] for r in weighted]
    return date_sks, weights
```

- [ ] **Step 4: Run the seasonality + web-events tests**

Run: `python -m pytest tests/test_lookups.py tests/test_dim_date_spark.py tests/test_fact_web_events_spread.py -v`
Expected: PASS — Cyber Monday recognized, Cyber-5 peak dominant, post-holiday trough < 100, weights ≥ 1; web-events decorrelation test unaffected.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/spark/calendar.py src/techmart/facts/lookups.py tests/test_lookups.py tests/test_dim_date_spark.py
git commit -m "Deepen seasonality: Cyber-5, BTS, December ramp, July event, Jan trough

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: Full-suite regression + rollout notes

**Files:** none (verification task)

- [ ] **Step 1: Run the full suite**

Run: `python -m pytest -q`
Expected: PASS. Investigate any failure in facts derived from sales/products (returns, fulfillment, loyalty, gl_actuals, budget, inventory valuation) — those read the new prices/quantities but their invariants are unchanged; a failure means a real coupling to fix, not a threshold to loosen.

- [ ] **Step 2: Record rollout steps in the ledger** (executed by Lukas post-merge)

Full regeneration required (prices cascade everywhere):
1. `databricks bundle deploy` at showcase scale.
2. Run `techmart-generate` (dims → facts → finance; decide on AI/ops/semantic re-run — the price change updates joined economics on read, but any materialized AI outputs referencing old prices would be stale).
3. Validate: trailing-12-month `SUM(net_sales_amount)` ≈ $22–28B; per-category avg `unit_price` within bands; a monthly net-sales series shows the Back-to-School bump, the Cyber-5 spike, and the January trough.

## Known trade-off (documented, not a defect)

The taxonomy has few genuinely-cheap categories, so hitting $25B with realistic prices forces heavy unit-share weight onto Ethernet Cabling / Connectors (~70% of units combined). Unit counts will therefore be dominated by cabling/accessories — defensible for electronics-retail *unit* volume, but if "top products by units" reads oddly for the demo, the follow-up is to **broaden the accessory taxonomy** (more sub-$60 categories: media, chargers, mounts, batteries, phone cases) and redistribute weight — or accept a higher blend (~$45B, Best Buy scale). Isolated to `pricing.py` + `taxonomy.py`.

## Self-Review

- **Spec coverage:** category price bands → Task 1 (author) + Task 2 (apply); unit-share weights → Task 1; two-stage selection → Task 3; quantity realism → Task 3; seasonality + Cyber Monday → Task 4; calibration to $25B → Task 1 (analytic test, tuned weights); full-regen/rollout → Task 5. No gaps.
- **Placeholder scan:** none — every code step is concrete; the only "tune until" is the intended calibration knob in Task 1 Step 4, with an explicit target range.
- **Type consistency:** `price_bands_by_id`/`unit_weights_by_id`/`category_cdf` defined in Task 1 and consumed identically in Tasks 2–3; `uniform_hash(col, salt=...)` used consistently; the `build_fact_sales_line` signature is unchanged; `date_seasonality_weights` keeps its `(list[int], list[int])` return contract consumed by both sales and web-events builders.
