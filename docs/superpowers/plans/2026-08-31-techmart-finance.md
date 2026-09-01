# Techmart Finance (`techmart_finance`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `techmart_finance` schema — `dim_department`, `dim_gl_account`, and the `fact_gl_actuals` / `fact_budget_plan` / `fact_inventory_valuation` facts — deriving finance actuals from the real core facts with small deterministic injected deltas, so the gross↔net reconciliation story is real and resolvable.

**Architecture:** Serverless-native dbldatagen/PySpark, identical discipline to Phase 4. Two deterministic dimensions (`spark.createDataFrame`, no SCD2 — the `dim_channel` pattern). Three facts built by aggregating persisted core facts to the fiscal 4-5-4 period, keyed on the period-end `date_sk` (RI by construction). Injected deltas (allowances, markdowns, timing shift, budget variance, opex jitter) use hash-keyed factors from `facts/gen.py` — never `rand()`. Written to `<catalog>.techmart_finance.<table>` via the existing `write_table_uc` (`spec.schema="finance"`), deployed as a serverless notebook task that `depends_on` `generate_facts`.

**Tech Stack:** PySpark, dbldatagen (only where standalone rows are needed — here everything derives, so no dbldatagen builders), Delta/Unity Catalog, Databricks Asset Bundle, pytest + local Spark.

## Global Constraints

- **Determinism:** no `rand()`, `monotonically_increasing_id()`, `current_timestamp()`, `uuid()`, or `xxhash`-of-nondeterministic input anywhere. Injected factors use `facts/gen.py` `uniform_hash(*keys, salt)` / `bounded_int(*keys, salt, lo, hi)` keyed on stable columns. Same seed/inputs → byte-identical output.
- **Referential integrity by construction:** every finance fact is keyed on a **period-end `date_sk`** that is a real `dim_date` row; `store_sk`, `gl_account_sk`, `department_sk`, `category_id` come from real dim/fact rows. Zero orphan FKs is a build invariant, not a cleanup step.
- **Fiscal calendar:** use `dim_date.fiscal_year` + `dim_date.fiscal_period` (1–12). A period's ordinal is `pidx = fiscal_year*12 + (fiscal_period-1)`. Period-end `date_sk` = `MAX(date_sk)` within a `(fiscal_year, fiscal_period)`.
- **Schema routing:** finance specs set `schema="finance"`; `write_table_uc` writes to `<catalog>.<schema_prefix>finance.<name>` and emits the table-level grain `COMMENT`. Do not hardcode catalog/schema in builders.
- **Sign convention (documented, consistent across all facts):** revenue-section accounts contribute *signed* to Net Sales — Gross Product Sales positive, contra accounts (Sales Returns, Sales Allowances) **negative**. Expense-section accounts (COGS, Opex) store **positive** expense magnitudes. `dim_gl_account.normal_balance` documents debit/credit.
- **Reconciliation invariants (exact, to the penny given the injected rates):**
  - Σ `Gross Product Sales`.actual_amount over all rows == Σ `fact_sales_line.gross_sales_amount` (timing shift only *reclassifies* gross between adjacent periods; it never creates or destroys it).
  - Σ `Sales Returns`.actual_amount == −Σ `fact_returns.refund_amount`.
  - Σ `Sales Allowances`.actual_amount == −`allowance_rate` × Σ gross.
  - Net Sales (Σ of the three revenue accounts) == Σ gross − Σ returns − `allowance_rate`×Σ gross.
- **`vendor_sk` is intentionally dropped** from `fact_inventory_valuation` (conflicts with the category grain). Do not add it.
- **Naming note (not a bug):** `dim_product` already has a taxonomy-level `department_id`/`department_name` (a merchandising category). The finance `dim_department` is a *functional* department (cost-center function). They live in separate tables/schemas; the later semantic layer disambiguates via synonyms. Do not rename either.
- Commit after each task. Commit messages end with `Co-authored-by: Isaac <no-reply@databricks.com>`.
- Run tests with `python -m pytest`. Public repo stays secret-free (no workspace URLs/tokens).

---

### Task 1: Reconciliation-lever config fields

**Files:**
- Modify: `src/techmart/config.py` (ScaleProfile dataclass)
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `ScaleProfile.allowance_rate: float`, `.markdown_rate: float`, `.timing_shift_pct: float`, `.budget_variance: float` — behavioral levers shared across all profiles via defaults; every downstream finance task consumes them off `config.scale_profile`.

These are behavioral (not volume) knobs, so they get dataclass defaults and are **not** added to `config/scale_profiles.yaml` (all four profiles inherit the defaults; `load_profiles` keyword-expands the yaml, so absent keys fall to the defaults).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_config.py`:

```python
def test_finance_levers_default(tmp_path):
    from techmart.config import load_config
    import textwrap, pathlib
    p = pathlib.Path(__file__).parent.parent / "config" / "scale_profiles.yaml"
    cfg = load_config(p, "smoke")
    sp = cfg.scale_profile
    assert sp.allowance_rate == 0.010
    assert sp.markdown_rate == 0.015
    assert sp.timing_shift_pct == 0.05
    assert sp.budget_variance == 0.08
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config.py::test_finance_levers_default -v`
Expected: FAIL (`AttributeError: 'ScaleProfile' object has no attribute 'allowance_rate'`).

- [ ] **Step 3: Add the fields**

In `src/techmart/config.py`, add to the `ScaleProfile` dataclass after `web_events_target`:

```python
    # Finance reconciliation levers (behavioral; shared across profiles via defaults).
    allowance_rate: float = 0.010
    markdown_rate: float = 0.015
    timing_shift_pct: float = 0.05
    budget_variance: float = 0.08
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS (existing positional `ScaleProfile(...)` constructions in other tests still work — new fields have defaults and follow the existing defaulted fields).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/config.py tests/test_config.py
git commit -m "Add finance reconciliation levers to ScaleProfile

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Chart-of-accounts reference

**Files:**
- Create: `src/techmart/reference/gl_accounts.py`
- Test: `tests/test_gl_accounts.py`

**Interfaces:**
- Produces: `GL_ACCOUNTS: list[dict]` — each dict has keys `account_number, account_name, account_type, statement, statement_section, account_category, normal_balance, is_contra`. Consumed by `dim_gl_account` (Task 4) and referenced by the fact builders for account numbers. Account numbers are the stable business keys used in the fact derivation.

Author a realistic ~40-account chart. The derivation targets (Tasks 6/8) reference these exact `account_number` strings, so they MUST be present exactly: `4000` Gross Product Sales, `4100` Sales Returns (contra), `4200` Sales Allowances (contra), `5000` Product COGS, `5100` Freight-In, `5200` Markdowns, `5300` Inventory Shrink, `6000` Store Payroll, `6100` Occupancy, `6200` Marketing, `6300` Supply-Chain Opex, `6400` General & Administrative, `6500` Depreciation, `1400` Merchandise Inventory (asset).

- [ ] **Step 1: Write the failing test**

Create `tests/test_gl_accounts.py`:

```python
from techmart.reference.gl_accounts import GL_ACCOUNTS

_REQUIRED = {
    "4000", "4100", "4200", "5000", "5100", "5200", "5300",
    "6000", "6100", "6200", "6300", "6400", "6500", "1400",
}


def test_required_accounts_present():
    nums = {a["account_number"] for a in GL_ACCOUNTS}
    assert _REQUIRED <= nums


def test_unique_account_numbers():
    nums = [a["account_number"] for a in GL_ACCOUNTS]
    assert len(nums) == len(set(nums))
    assert len(GL_ACCOUNTS) >= 40


def test_contra_flags_and_enums():
    by_num = {a["account_number"]: a for a in GL_ACCOUNTS}
    assert by_num["4100"]["is_contra"] and by_num["4200"]["is_contra"]
    assert by_num["4000"]["is_contra"] is False
    for a in GL_ACCOUNTS:
        assert a["account_type"] in {"Revenue", "COGS", "Opex", "Asset"}
        assert a["statement"] in {"P&L", "Balance-Sheet"}
        assert a["normal_balance"] in {"Debit", "Credit"}
        assert isinstance(a["is_contra"], bool)


def test_asset_on_balance_sheet():
    by_num = {a["account_number"]: a for a in GL_ACCOUNTS}
    assert by_num["1400"]["account_type"] == "Asset"
    assert by_num["1400"]["statement"] == "Balance-Sheet"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_gl_accounts.py -v`
Expected: FAIL (`ModuleNotFoundError: techmart.reference.gl_accounts`).

- [ ] **Step 3: Write the reference module**

Create `src/techmart/reference/gl_accounts.py`:

```python
"""Techmart chart of accounts (engine-agnostic reference data).

The fact derivation (fact_gl_actuals / fact_inventory_valuation) references the
account_number business keys directly, so the derivation-target accounts must
keep their exact numbers. Extra accounts add realism (not every account carries
activity every period) and are intentionally left unpopulated by the facts.
"""
from __future__ import annotations


def _a(number, name, atype, statement, section, category, normal, contra=False):
    return {
        "account_number": number,
        "account_name": name,
        "account_type": atype,
        "statement": statement,
        "statement_section": section,
        "account_category": category,
        "normal_balance": normal,
        "is_contra": contra,
    }


GL_ACCOUNTS: list[dict] = [
    # --- Revenue (P&L) ---
    _a("4000", "Gross Product Sales", "Revenue", "P&L", "Net Sales", "Product Sales", "Credit"),
    _a("4010", "Service & Warranty Revenue", "Revenue", "P&L", "Net Sales", "Service Revenue", "Credit"),
    _a("4020", "Shipping Revenue", "Revenue", "P&L", "Net Sales", "Other Revenue", "Credit"),
    _a("4100", "Sales Returns", "Revenue", "P&L", "Net Sales", "Contra Revenue", "Debit", contra=True),
    _a("4200", "Sales Allowances", "Revenue", "P&L", "Net Sales", "Contra Revenue", "Debit", contra=True),
    # --- Cost of goods sold (P&L) ---
    _a("5000", "Product COGS", "COGS", "P&L", "Cost of Goods Sold", "Merchandise Cost", "Debit"),
    _a("5100", "Freight-In", "COGS", "P&L", "Cost of Goods Sold", "Inbound Freight", "Debit"),
    _a("5200", "Markdowns", "COGS", "P&L", "Cost of Goods Sold", "Markdowns", "Debit"),
    _a("5300", "Inventory Shrink", "COGS", "P&L", "Cost of Goods Sold", "Shrink", "Debit"),
    _a("5400", "Vendor Allowances", "COGS", "P&L", "Cost of Goods Sold", "Vendor Funding", "Credit", contra=True),
    # --- Operating expense (P&L) ---
    _a("6000", "Store Payroll", "Opex", "P&L", "Operating Expenses", "Payroll", "Debit"),
    _a("6010", "Store Benefits", "Opex", "P&L", "Operating Expenses", "Payroll", "Debit"),
    _a("6100", "Occupancy", "Opex", "P&L", "Operating Expenses", "Occupancy", "Debit"),
    _a("6110", "Utilities", "Opex", "P&L", "Operating Expenses", "Occupancy", "Debit"),
    _a("6200", "Marketing", "Opex", "P&L", "Operating Expenses", "Marketing", "Debit"),
    _a("6210", "Digital Advertising", "Opex", "P&L", "Operating Expenses", "Marketing", "Debit"),
    _a("6300", "Supply-Chain Opex", "Opex", "P&L", "Operating Expenses", "Supply Chain", "Debit"),
    _a("6310", "Distribution Center Costs", "Opex", "P&L", "Operating Expenses", "Supply Chain", "Debit"),
    _a("6400", "General & Administrative", "Opex", "P&L", "Operating Expenses", "G&A", "Debit"),
    _a("6410", "IT & Systems", "Opex", "P&L", "Operating Expenses", "G&A", "Debit"),
    _a("6420", "Professional Fees", "Opex", "P&L", "Operating Expenses", "G&A", "Debit"),
    _a("6500", "Depreciation", "Opex", "P&L", "Operating Expenses", "Depreciation", "Debit"),
    _a("6510", "Amortization", "Opex", "P&L", "Operating Expenses", "Depreciation", "Debit"),
    _a("6600", "Credit Card Fees", "Opex", "P&L", "Operating Expenses", "Other Opex", "Debit"),
    _a("6610", "Insurance", "Opex", "P&L", "Operating Expenses", "Other Opex", "Debit"),
    _a("6700", "Bad Debt Expense", "Opex", "P&L", "Operating Expenses", "Other Opex", "Debit"),
    _a("6800", "Interest Expense", "Opex", "P&L", "Operating Expenses", "Interest", "Debit"),
    _a("6900", "Income Tax Expense", "Opex", "P&L", "Operating Expenses", "Taxes", "Debit"),
    # --- Assets (Balance-Sheet) ---
    _a("1000", "Cash & Equivalents", "Asset", "Balance-Sheet", "Current Assets", "Cash", "Debit"),
    _a("1100", "Accounts Receivable", "Asset", "Balance-Sheet", "Current Assets", "Receivables", "Debit"),
    _a("1400", "Merchandise Inventory", "Asset", "Balance-Sheet", "Current Assets", "Inventory", "Debit"),
    _a("1410", "Inventory Reserve", "Asset", "Balance-Sheet", "Current Assets", "Inventory", "Credit", contra=True),
    _a("1500", "Prepaid Expenses", "Asset", "Balance-Sheet", "Current Assets", "Prepaids", "Debit"),
    _a("1600", "Property & Equipment", "Asset", "Balance-Sheet", "Non-Current Assets", "PP&E", "Debit"),
    _a("1610", "Accumulated Depreciation", "Asset", "Balance-Sheet", "Non-Current Assets", "PP&E", "Credit", contra=True),
    _a("1700", "Right-of-Use Assets", "Asset", "Balance-Sheet", "Non-Current Assets", "Leases", "Debit"),
    _a("1800", "Goodwill", "Asset", "Balance-Sheet", "Non-Current Assets", "Intangibles", "Debit"),
    _a("1810", "Intangible Assets", "Asset", "Balance-Sheet", "Non-Current Assets", "Intangibles", "Debit"),
    _a("1900", "Other Assets", "Asset", "Balance-Sheet", "Non-Current Assets", "Other", "Debit"),
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_gl_accounts.py -v`
Expected: PASS (4 tests; ≥40 accounts, all required present).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/reference/gl_accounts.py tests/test_gl_accounts.py
git commit -m "Add chart-of-accounts reference data

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: `dim_department`

**Files:**
- Create: `src/techmart/finance/__init__.py` (empty)
- Create: `src/techmart/finance/dim_department.py`
- Test: `tests/test_dim_department.py`

**Interfaces:**
- Produces: `DIM_DEPARTMENT_SPEC: SparkTableSpec` (schema="finance"), `build_dim_department(spark, config) -> DataFrame` with columns `department_sk`(long), `department_name`(string), `department_group`(string). Consumed by `fact_gl_actuals` (Task 6) and `fact_budget_plan` (Task 7), which join on `department_name` to resolve `department_sk`.
- The department names MUST be exactly: `Merchandising`, `Store Operations`, `Supply Chain`, `Marketing`, `E-commerce`, `Finance & Admin`, `G&A` (the fact builders map account → one of these names).

- [ ] **Step 1: Write the failing test**

Create `tests/test_dim_department.py`:

```python
from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.finance.dim_department import DIM_DEPARTMENT_SPEC, build_dim_department

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 40, 1, 3000, 200, 20), seed=42,
    output_dir=Path("data"), catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_EXPECTED = {
    "Merchandising", "Store Operations", "Supply Chain", "Marketing",
    "E-commerce", "Finance & Admin", "G&A",
}


def test_schema_and_rows(spark):
    df = build_dim_department(spark, _CFG)
    assert df.columns == DIM_DEPARTMENT_SPEC.column_names
    assert DIM_DEPARTMENT_SPEC.schema == "finance"
    names = {r["department_name"] for r in df.collect()}
    assert names == _EXPECTED


def test_unique_sk_and_groups(spark):
    df = build_dim_department(spark, _CFG)
    rows = df.collect()
    assert len({r["department_sk"] for r in rows}) == len(rows)
    assert all(r["department_group"] in {"COGS-bearing", "Opex"} for r in rows)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dim_department.py -v`
Expected: FAIL (`ModuleNotFoundError: techmart.finance.dim_department`).

- [ ] **Step 3: Write the builder**

Create `src/techmart/finance/__init__.py` (empty file).

Create `src/techmart/finance/dim_department.py`:

```python
"""dim_department: functional cost-center department (techmart_finance)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec

DIM_DEPARTMENT_SPEC = SparkTableSpec(
    schema="finance",
    name="dim_department",
    grain="one row per functional cost-center department",
    columns=[
        SparkColumn("department_sk", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("department_name", "string", "Functional department name", nullable=False),
        SparkColumn("department_group", "string", "COGS-bearing or Opex grouping"),
    ],
)

_DEPARTMENTS = [
    ("Merchandising", "COGS-bearing"),
    ("E-commerce", "COGS-bearing"),
    ("Supply Chain", "COGS-bearing"),
    ("Store Operations", "Opex"),
    ("Marketing", "Opex"),
    ("G&A", "Opex"),
    ("Finance & Admin", "Opex"),
]


def build_dim_department(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    rows = [(i, name, group) for i, (name, group) in enumerate(_DEPARTMENTS, start=1)]
    return spark.createDataFrame(rows, schema=DIM_DEPARTMENT_SPEC.struct_type())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_department.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/finance/__init__.py src/techmart/finance/dim_department.py tests/test_dim_department.py
git commit -m "Add dim_department (techmart_finance)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: `dim_gl_account`

**Files:**
- Create: `src/techmart/finance/dim_gl_account.py`
- Test: `tests/test_dim_gl_account.py`

**Interfaces:**
- Consumes: `GL_ACCOUNTS` (Task 2), `SparkColumn`/`SparkTableSpec`.
- Produces: `DIM_GL_ACCOUNT_SPEC: SparkTableSpec` (schema="finance"), `build_dim_gl_account(spark, config) -> DataFrame` with columns `gl_account_sk`(long), `account_number`(string), `account_name`(string), `account_type`(string), `statement`(string), `statement_section`(string), `account_category`(string), `normal_balance`(string), `is_contra`(boolean). Consumed by all three facts (join on `account_number` → `gl_account_sk`).
- `gl_account_sk` is assigned 1..N in `GL_ACCOUNTS` list order (stable).

- [ ] **Step 1: Write the failing test**

Create `tests/test_dim_gl_account.py`:

```python
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.finance.dim_gl_account import DIM_GL_ACCOUNT_SPEC, build_dim_gl_account
from techmart.reference.gl_accounts import GL_ACCOUNTS

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 40, 1, 3000, 200, 20), seed=42,
    output_dir=Path("data"), catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_schema_and_count(spark):
    df = build_dim_gl_account(spark, _CFG)
    assert df.columns == DIM_GL_ACCOUNT_SPEC.column_names
    assert DIM_GL_ACCOUNT_SPEC.schema == "finance"
    assert df.count() == len(GL_ACCOUNTS)


def test_unique_sk_and_number(spark):
    df = build_dim_gl_account(spark, _CFG)
    assert df.select("gl_account_sk").distinct().count() == df.count()
    assert df.select("account_number").distinct().count() == df.count()
    # sequential 1..N
    sks = sorted(r["gl_account_sk"] for r in df.collect())
    assert sks == list(range(1, len(GL_ACCOUNTS) + 1))


def test_contra_boolean_and_required(spark):
    df = build_dim_gl_account(spark, _CFG)
    by = {r["account_number"]: r for r in df.collect()}
    assert by["4100"]["is_contra"] is True
    assert by["4000"]["account_type"] == "Revenue"
    assert by["1400"]["statement"] == "Balance-Sheet"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_dim_gl_account.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the builder**

Create `src/techmart/finance/dim_gl_account.py`:

```python
"""dim_gl_account: chart of accounts (techmart_finance)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig
from ..reference.gl_accounts import GL_ACCOUNTS
from ..spark.framework import SparkColumn, SparkTableSpec

DIM_GL_ACCOUNT_SPEC = SparkTableSpec(
    schema="finance",
    name="dim_gl_account",
    grain="one row per general-ledger account",
    columns=[
        SparkColumn("gl_account_sk", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("account_number", "string", "Account business key", nullable=False),
        SparkColumn("account_name", "string", "Account name"),
        SparkColumn("account_type", "string", "Revenue/COGS/Opex/Asset"),
        SparkColumn("statement", "string", "P&L or Balance-Sheet"),
        SparkColumn("statement_section", "string", "Rollup level 1"),
        SparkColumn("account_category", "string", "Rollup level 2"),
        SparkColumn("normal_balance", "string", "Debit or Credit"),
        SparkColumn("is_contra", "boolean", "True for contra accounts", nullable=False),
    ],
)


def build_dim_gl_account(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    rows = [
        (
            i, a["account_number"], a["account_name"], a["account_type"],
            a["statement"], a["statement_section"], a["account_category"],
            a["normal_balance"], a["is_contra"],
        )
        for i, a in enumerate(GL_ACCOUNTS, start=1)
    ]
    return spark.createDataFrame(rows, schema=DIM_GL_ACCOUNT_SPEC.struct_type())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_dim_gl_account.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/finance/dim_gl_account.py tests/test_dim_gl_account.py
git commit -m "Add dim_gl_account (techmart_finance)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: Fiscal-period helper

**Files:**
- Create: `src/techmart/finance/periods.py`
- Test: `tests/test_finance_periods.py`

**Interfaces:**
- Consumes: a `dim_date` DataFrame with `date_sk`, `fiscal_year`, `fiscal_period`, `fiscal_week`.
- Produces:
  - `date_periods(dim_date) -> DataFrame[date_sk, fiscal_year, fiscal_period, fiscal_week, pidx]` — maps each date to its period ordinal `pidx = fiscal_year*12 + (fiscal_period-1)`.
  - `period_end_lookup(dim_date) -> DataFrame[fiscal_year, fiscal_period, pidx, period_end_date_sk, period_max_week]` — one row per fiscal period; `period_end_date_sk = MAX(date_sk)`, `period_max_week = MAX(fiscal_week)` within the period.
- Both consumed by Tasks 6, 7, 8.

- [ ] **Step 1: Write the failing test**

Create `tests/test_finance_periods.py`:

```python
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.finance.periods import date_periods, period_end_lookup

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 40, 1, 3000, 200, 20), seed=42,
    output_dir=Path("data"), catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_period_end_one_row_per_period(spark):
    dd = build_dim_date(spark, _CFG)
    pe = period_end_lookup(dd)
    assert pe.count() == dd.select("fiscal_year", "fiscal_period").distinct().count()
    assert pe.select("pidx").distinct().count() == pe.count()


def test_period_end_is_real_and_max(spark):
    dd = build_dim_date(spark, _CFG)
    pe = period_end_lookup(dd)
    # every period_end_date_sk is a real dim_date row
    orphans = pe.select(F.col("period_end_date_sk").alias("date_sk")).join(
        dd.select("date_sk"), "date_sk", "left_anti"
    ).count()
    assert orphans == 0
    # it is the max date_sk within its period
    joined = date_periods(dd).groupBy("fiscal_year", "fiscal_period").agg(
        F.max("date_sk").alias("mx")
    ).join(pe, ["fiscal_year", "fiscal_period"])
    assert joined.filter(F.col("mx") != F.col("period_end_date_sk")).count() == 0


def test_pidx_formula(spark):
    dd = build_dim_date(spark, _CFG)
    dp = date_periods(dd)
    bad = dp.filter(
        F.col("pidx") != (F.col("fiscal_year") * F.lit(12) + (F.col("fiscal_period") - F.lit(1)))
    ).count()
    assert bad == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_finance_periods.py -v`
Expected: FAIL (`ModuleNotFoundError: techmart.finance.periods`).

- [ ] **Step 3: Write the helper**

Create `src/techmart/finance/periods.py`:

```python
"""Fiscal 4-5-4 period helpers shared by the finance facts."""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def _pidx() -> "F.Column":
    return (F.col("fiscal_year") * F.lit(12) + (F.col("fiscal_period") - F.lit(1))).cast("int")


def date_periods(dim_date: DataFrame) -> DataFrame:
    """date_sk -> (fiscal_year, fiscal_period, fiscal_week, pidx)."""
    return dim_date.select(
        "date_sk", "fiscal_year", "fiscal_period", "fiscal_week"
    ).withColumn("pidx", _pidx())


def period_end_lookup(dim_date: DataFrame) -> DataFrame:
    """One row per fiscal period with its period-end date_sk and max fiscal week."""
    return (
        dim_date.groupBy("fiscal_year", "fiscal_period")
        .agg(
            F.max("date_sk").alias("period_end_date_sk"),
            F.max("fiscal_week").alias("period_max_week"),
        )
        .withColumn("pidx", _pidx())
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_finance_periods.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/finance/periods.py tests/test_finance_periods.py
git commit -m "Add fiscal-period helpers for finance facts

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: `fact_gl_actuals`

**Files:**
- Create: `src/techmart/finance/fact_gl_actuals.py`
- Test: `tests/test_fact_gl_actuals.py`

**Interfaces:**
- Consumes: `date_periods`/`period_end_lookup` (Task 5); `uniform_hash` (`facts/gen.py`); persisted core facts `fact_sales_line` (`store_sk, channel_sk, date_sk, gross_sales_amount, cogs_amount`), `fact_returns` (`store_sk, date_sk, refund_amount`), `fact_inventory_movement` (`store_sk, date_sk, movement_type, quantity, unit_cost`); `dim_date`, `dim_gl_account`, `dim_department`; `config.scale_profile.{allowance_rate, markdown_rate, timing_shift_pct}`.
- Produces: `FACT_GL_ACTUALS_SPEC`, `build_fact_gl_actuals(spark, config, *, fact_sales_line, fact_returns, fact_inventory_movement, dim_date, dim_gl_account, dim_department) -> DataFrame`.

**Design (see Global Constraints for the exact reconciliation invariants):**
- **Group A — revenue/COGS split by online-ness**, grain (store, is_online, pidx): Gross Product Sales (`4000`) and Product COGS (`5000`). `is_online = channel_sk IN (2,3,4)`; department = `E-commerce` if online else `Merchandising`. Gross gets the **timing shift**.
- **Timing shift (telescoping, total-conserving):** per (store, is_online, pidx), `shift_out = timing_shift_pct * last_week_gross` where `last_week_gross` = gross in the period's `period_max_week`; `shift_out` is zeroed for the maximum pidx present (so every shifted amount lands in an in-window successor period). `recognized_gross = gross - shift_out + shift_out_of(pidx-1)`. Σ recognized == Σ gross exactly.
- **Group B — store-level lines**, grain (store, pidx): Sales Returns (`4100`, −returns, Merchandising), Sales Allowances (`4200`, −allowance_rate×store_recognized_gross, Merchandising), Markdowns (`5200`, +markdown_rate×store_recognized_gross, Merchandising), Inventory Shrink (`5300`, +Σ abs(qty)×unit_cost of Shrink movements, Supply Chain), and Opex accounts `6000/6100/6200/6300/6400/6500` (formulas below), where `store_recognized_gross(store,pidx) = Σ over is_online of recognized_gross`.
- **Opex formulas** (deterministic; `j(acct) = 0.95 + 0.10*uniform_hash(store_sk, lit(acct), salt=<OPEX_SALT>)`), all against `g = store_recognized_gross`:
  - `6000` Store Payroll = (8000 + 0.11*g) * j → Store Operations
  - `6100` Occupancy = 6000 * j (fixed base) → Store Operations
  - `6200` Marketing = 0.04*g * j → Marketing
  - `6300` Supply-Chain Opex = 0.03*g * j → Supply Chain
  - `6400` General & Administrative = (4000 + 0.02*g) * j → G&A
  - `6500` Depreciation = 3500 * j (fixed base) → G&A
- Produce lines via `explode(F.array(F.struct(...)))` of `(account_number, actual_amount, department_name)`, then join `dim_gl_account` on `account_number` → `gl_account_sk` and `dim_department` on `department_name` → `department_sk`, and `period_end_lookup` on `pidx` → `period_end_date_sk` (aliased `date_sk`), `fiscal_year`, `fiscal_period`. `currency = "USD"`. Round every `actual_amount` to 2 dp.

**Spec columns:** `date_sk`(long,key), `gl_account_sk`(long,key), `store_sk`(long,key), `department_sk`(long,key), `fiscal_year`(int), `fiscal_period`(int), `actual_amount`(double), `currency`(string). Grain: `"one row per GL account × store × department × fiscal period"`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_fact_gl_actuals.py`:

```python
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.spark.dimensions.dim_customer import build_dim_customer
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_returns import build_fact_returns
from techmart.facts.fact_inventory_movement import build_fact_inventory_movement
from techmart.finance.dim_department import build_dim_department
from techmart.finance.dim_gl_account import build_dim_gl_account
from techmart.finance.fact_gl_actuals import FACT_GL_ACTUALS_SPEC, build_fact_gl_actuals

_P = ScaleProfile("t", 8, 40, 1, 4000, 300, 20)
_CFG = TechmartConfig(
    scale_profile=_P, seed=42, output_dir=Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_COUNTS = {"store": 8, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 40}


def _inputs(spark):
    dd = build_dim_date(spark, _CFG)
    dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=4000)
    returns = build_fact_returns(spark, _CFG, fact_sales_line=sales, dim_date=dd)
    mov = build_fact_inventory_movement(spark, _CFG, dim_date=dd, dim_product=dp, dim_counts=_COUNTS)
    return dd, dp, sales, returns, mov


def _build(spark):
    dd, dp, sales, returns, mov = _inputs(spark)
    return build_fact_gl_actuals(
        spark, _CFG, fact_sales_line=sales, fact_returns=returns, fact_inventory_movement=mov,
        dim_date=dd, dim_gl_account=build_dim_gl_account(spark, _CFG),
        dim_department=build_dim_department(spark, _CFG),
    ), sales, returns, dd


def test_schema_and_grain(spark):
    df, *_ = _build(spark)
    assert df.columns == FACT_GL_ACTUALS_SPEC.column_names
    grain_cols = ["gl_account_sk", "store_sk", "department_sk", "date_sk"]
    assert df.groupBy(*grain_cols).count().filter(F.col("count") > 1).count() == 0


def test_referential_integrity(spark):
    df, _, _, dd = _build(spark)
    gl = build_dim_gl_account(spark, _CFG).select("gl_account_sk")
    dep = build_dim_department(spark, _CFG).select("department_sk")
    assert df.select("date_sk").distinct().join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
    assert df.select("gl_account_sk").distinct().join(gl, "gl_account_sk", "left_anti").count() == 0
    assert df.select("department_sk").distinct().join(dep, "department_sk", "left_anti").count() == 0


def test_gross_conserved_by_timing_shift(spark):
    df, sales, _, _ = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    gross_num = [a for a in gl.collect() if a["account_number"] == "4000"][0]["gl_account_sk"]
    recognized = df.filter(F.col("gl_account_sk") == gross_num).agg(F.round(F.sum("actual_amount"), 2)).first()[0]
    merch_gross = sales.agg(F.round(F.sum("gross_sales_amount"), 2)).first()[0]
    assert abs(recognized - merch_gross) < 0.05  # penny-rounding tolerance across many rows


def test_net_sales_reconciliation(spark):
    df, sales, returns, _ = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    rev_sks = [a["gl_account_sk"] for a in gl.collect() if a["account_number"] in {"4000", "4100", "4200"}]
    net = df.filter(F.col("gl_account_sk").isin(rev_sks)).agg(F.sum("actual_amount")).first()[0]
    gross = sales.agg(F.sum("gross_sales_amount")).first()[0]
    ret = returns.agg(F.sum("refund_amount")).first()[0]
    expected = gross - ret - _P.allowance_rate * gross
    assert abs(net - expected) < 0.5  # rounding across rows


def test_opex_present_and_positive(spark):
    df, *_ = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    opex_sks = [a["gl_account_sk"] for a in gl.collect() if a["account_number"] in
                {"6000", "6100", "6200", "6300", "6400", "6500"}]
    opex = df.filter(F.col("gl_account_sk").isin(opex_sks))
    assert opex.count() > 0
    assert opex.filter(F.col("actual_amount") < 0).count() == 0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("actual_amount"), 2).alias("s")).first()
    b = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("actual_amount"), 2).alias("s")).first()
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_fact_gl_actuals.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the builder**

Create `src/techmart/finance/fact_gl_actuals.py`:

```python
"""fact_gl_actuals: GL actuals derived from real core facts + injected deltas."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from ..config import TechmartConfig
from ..facts.gen import uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec
from .periods import date_periods, period_end_lookup

_OPEX_SALT = 730_001

FACT_GL_ACTUALS_SPEC = SparkTableSpec(
    schema="finance",
    name="fact_gl_actuals",
    grain="one row per GL account × store × department × fiscal period",
    columns=[
        SparkColumn("date_sk", "long", "Period-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("gl_account_sk", "long", "GL account FK (dim_gl_account)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store/cost-center FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("department_sk", "long", "Department FK (dim_department)", is_key=True, nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("actual_amount", "double", "Actual amount (contra revenue negative)"),
        SparkColumn("currency", "string", "ISO currency code"),
    ],
)


def build_fact_gl_actuals(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_sales_line: DataFrame,
    fact_returns: DataFrame,
    fact_inventory_movement: DataFrame,
    dim_date: DataFrame,
    dim_gl_account: DataFrame,
    dim_department: DataFrame,
) -> DataFrame:
    sp = config.scale_profile
    periods = date_periods(dim_date)
    pe = period_end_lookup(dim_date)  # fiscal_year, fiscal_period, pidx, period_end_date_sk, period_max_week
    max_pidx = pe.agg(F.max("pidx")).first()[0]

    # --- Group A: gross + cogs by (store, is_online, pidx), with timing shift on gross ---
    s = (
        fact_sales_line.select(
            "store_sk", "channel_sk", "date_sk", "gross_sales_amount", "cogs_amount"
        )
        .join(periods, "date_sk")
        .withColumn("is_online", F.col("channel_sk").isin(2, 3, 4))
    )
    a = s.groupBy("store_sk", "is_online", "pidx").agg(
        F.sum("gross_sales_amount").alias("gross"),
        F.sum("cogs_amount").alias("cogs"),
    )
    # last-week gross per (store, is_online, pidx)
    lw = (
        s.join(pe.select("pidx", "period_max_week"), "pidx")
        .filter(F.col("fiscal_week") == F.col("period_max_week"))
        .groupBy("store_sk", "is_online", "pidx")
        .agg(F.sum("gross_sales_amount").alias("last_week_gross"))
    )
    a = a.join(lw, ["store_sk", "is_online", "pidx"], "left").fillna(0.0, ["last_week_gross"])
    a = a.withColumn(
        "shift_out",
        F.when(F.col("pidx") < F.lit(max_pidx), F.lit(sp.timing_shift_pct) * F.col("last_week_gross")).otherwise(F.lit(0.0)),
    )
    shift_in = a.select(
        "store_sk", "is_online", (F.col("pidx") + F.lit(1)).alias("pidx"),
        F.col("shift_out").alias("shift_in"),
    )
    a = a.join(shift_in, ["store_sk", "is_online", "pidx"], "left").fillna(0.0, ["shift_in"])
    a = a.withColumn("recognized_gross", F.col("gross") - F.col("shift_out") + F.col("shift_in"))
    a = a.withColumn(
        "dept_name", F.when(F.col("is_online"), F.lit("E-commerce")).otherwise(F.lit("Merchandising"))
    )

    rev_lines = a.select(
        "store_sk", "pidx", F.col("dept_name"),
        F.explode(
            F.array(
                F.struct(F.lit("4000").alias("acct"), F.col("recognized_gross").alias("amt")),
                F.struct(F.lit("5000").alias("acct"), F.col("cogs").alias("amt")),
            )
        ).alias("line"),
    ).select("store_sk", "pidx", "dept_name", F.col("line.acct").alias("account_number"), F.col("line.amt").alias("actual_amount"))

    # --- Group B: store-level lines, grain (store, pidx) ---
    store_g = a.groupBy("store_sk", "pidx").agg(F.sum("recognized_gross").alias("g"))

    ret = (
        fact_returns.select("store_sk", "date_sk", "refund_amount")
        .join(periods.select("date_sk", "pidx"), "date_sk")
        .groupBy("store_sk", "pidx").agg(F.sum("refund_amount").alias("returns_amt"))
    )
    shrink = (
        fact_inventory_movement.filter(F.col("movement_type") == "Shrink")
        .select("store_sk", "date_sk", "quantity", "unit_cost")
        .join(periods.select("date_sk", "pidx"), "date_sk")
        .groupBy("store_sk", "pidx")
        .agg(F.sum(F.abs(F.col("quantity")) * F.col("unit_cost")).alias("shrink_cost"))
    )
    b = (
        store_g.join(ret, ["store_sk", "pidx"], "left")
        .join(shrink, ["store_sk", "pidx"], "left")
        .fillna(0.0, ["returns_amt", "shrink_cost"])
    )

    def j(acct: str) -> "F.Column":
        return F.lit(0.95) + F.lit(0.10) * uniform_hash("store_sk", F.lit(acct), salt=_OPEX_SALT)

    store_lines = b.select(
        "store_sk", "pidx",
        F.explode(
            F.array(
                F.struct(F.lit("4100").alias("acct"), (-F.col("returns_amt")).alias("amt"), F.lit("Merchandising").alias("dep")),
                F.struct(F.lit("4200").alias("acct"), (-F.lit(sp.allowance_rate) * F.col("g")).alias("amt"), F.lit("Merchandising").alias("dep")),
                F.struct(F.lit("5200").alias("acct"), (F.lit(sp.markdown_rate) * F.col("g")).alias("amt"), F.lit("Merchandising").alias("dep")),
                F.struct(F.lit("5300").alias("acct"), F.col("shrink_cost").alias("amt"), F.lit("Supply Chain").alias("dep")),
                F.struct(F.lit("6000").alias("acct"), ((F.lit(8000.0) + F.lit(0.11) * F.col("g")) * j("6000")).alias("amt"), F.lit("Store Operations").alias("dep")),
                F.struct(F.lit("6100").alias("acct"), (F.lit(6000.0) * j("6100")).alias("amt"), F.lit("Store Operations").alias("dep")),
                F.struct(F.lit("6200").alias("acct"), (F.lit(0.04) * F.col("g") * j("6200")).alias("amt"), F.lit("Marketing").alias("dep")),
                F.struct(F.lit("6300").alias("acct"), (F.lit(0.03) * F.col("g") * j("6300")).alias("amt"), F.lit("Supply Chain").alias("dep")),
                F.struct(F.lit("6400").alias("acct"), ((F.lit(4000.0) + F.lit(0.02) * F.col("g")) * j("6400")).alias("amt"), F.lit("G&A").alias("dep")),
                F.struct(F.lit("6500").alias("acct"), (F.lit(3500.0) * j("6500")).alias("amt"), F.lit("G&A").alias("dep")),
            )
        ).alias("line"),
    ).select(
        "store_sk", "pidx",
        F.col("line.dep").alias("dept_name"),
        F.col("line.acct").alias("account_number"),
        F.col("line.amt").alias("actual_amount"),
    )

    lines = rev_lines.select("store_sk", "pidx", "dept_name", "account_number", "actual_amount").unionByName(
        store_lines.select("store_sk", "pidx", "dept_name", "account_number", "actual_amount")
    )

    out = (
        lines.join(dim_gl_account.select("account_number", "gl_account_sk"), "account_number")
        .join(dim_department.select(F.col("department_name").alias("dept_name"), "department_sk"), "dept_name")
        .join(pe.select("pidx", "period_end_date_sk", "fiscal_year", "fiscal_period"), "pidx")
        .withColumn("date_sk", F.col("period_end_date_sk"))
        .withColumn("actual_amount", F.round("actual_amount", 2))
        .withColumn("currency", F.lit("USD"))
    )
    return FACT_GL_ACTUALS_SPEC.select_ordered(out)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_fact_gl_actuals.py -v`
Expected: PASS (6 tests). If `test_net_sales_reconciliation` fails, check the contra sign and that allowances use `store_recognized_gross` (`g`), not raw gross.

- [ ] **Step 5: Commit**

```bash
git add src/techmart/finance/fact_gl_actuals.py tests/test_fact_gl_actuals.py
git commit -m "Add fact_gl_actuals with gross-net reconciliation

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: `fact_budget_plan`

**Files:**
- Create: `src/techmart/finance/fact_budget_plan.py`
- Test: `tests/test_fact_budget_plan.py`

**Interfaces:**
- Consumes: `fact_gl_actuals` (Task 6 output), `dim_gl_account` (for `statement` filter), `uniform_hash`; `config.scale_profile.budget_variance`.
- Produces: `FACT_BUDGET_PLAN_SPEC`, `build_fact_budget_plan(spark, config, *, fact_gl_actuals, dim_gl_account) -> DataFrame`.
- **Design:** filter actuals to **P&L accounts only** (join `dim_gl_account` on `gl_account_sk`, keep `statement == "P&L"`). For each such row, emit 3 `plan_version` rows (`Budget`, `Forecast`, `Latest-Estimate`) via `explode`. `plan_amount = round(actual_amount * (1 + variance), 2)` where `variance = (uniform_hash(store_sk, gl_account_sk, lit(plan_version), salt=<BUDGET_SALT>) * 2 - 1) * budget_variance` (signed, in ±budget_variance). `plan_units = cast(floor(abs(plan_amount) / 50.0) as long)` (documented unit proxy). `scenario = "Base"`. Carry `date_sk`, `store_sk`, `department_sk`, `gl_account_sk`, `fiscal_year`, `fiscal_period` from the actuals row.

**Spec columns:** `date_sk`(long,key), `gl_account_sk`(long,key), `store_sk`(long,key), `department_sk`(long,key), `plan_version`(string,key), `fiscal_year`(int), `fiscal_period`(int), `plan_amount`(double), `plan_units`(long), `scenario`(string). Grain: `"one row per department × store × GL account × fiscal period × plan version"`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_fact_budget_plan.py`:

```python
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_returns import build_fact_returns
from techmart.facts.fact_inventory_movement import build_fact_inventory_movement
from techmart.finance.dim_department import build_dim_department
from techmart.finance.dim_gl_account import build_dim_gl_account
from techmart.finance.fact_gl_actuals import build_fact_gl_actuals
from techmart.finance.fact_budget_plan import FACT_BUDGET_PLAN_SPEC, build_fact_budget_plan

_P = ScaleProfile("t", 8, 40, 1, 4000, 300, 20)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 8, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 40}


def _actuals(spark):
    dd = build_dim_date(spark, _CFG); dp = build_dim_product(spark, _CFG)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=4000)
    returns = build_fact_returns(spark, _CFG, fact_sales_line=sales, dim_date=dd)
    mov = build_fact_inventory_movement(spark, _CFG, dim_date=dd, dim_product=dp, dim_counts=_COUNTS)
    return build_fact_gl_actuals(spark, _CFG, fact_sales_line=sales, fact_returns=returns,
                                 fact_inventory_movement=mov, dim_date=dd,
                                 dim_gl_account=build_dim_gl_account(spark, _CFG),
                                 dim_department=build_dim_department(spark, _CFG))


def _build(spark):
    return build_fact_budget_plan(spark, _CFG, fact_gl_actuals=_actuals(spark),
                                  dim_gl_account=build_dim_gl_account(spark, _CFG))


def test_schema_and_three_versions(spark):
    df = _build(spark)
    assert df.columns == FACT_BUDGET_PLAN_SPEC.column_names
    assert {r["plan_version"] for r in df.select("plan_version").distinct().collect()} == \
        {"Budget", "Forecast", "Latest-Estimate"}


def test_pl_only(spark):
    df = _build(spark)
    gl = build_dim_gl_account(spark, _CFG)
    bs_sks = [a["gl_account_sk"] for a in gl.collect() if a["statement"] == "Balance-Sheet"]
    assert df.filter(F.col("gl_account_sk").isin(bs_sks)).count() == 0


def test_attainment_within_variance(spark):
    df = _build(spark); actuals = _actuals(spark)
    keys = ["date_sk", "gl_account_sk", "store_sk", "department_sk"]
    budget = df.filter(F.col("plan_version") == "Budget").select(*keys, F.col("plan_amount"))
    joined = actuals.select(*keys, "actual_amount").join(budget, keys).filter(F.abs("actual_amount") > 1.0)
    bad = joined.filter(
        F.abs(F.col("plan_amount") - F.col("actual_amount")) > (_P.budget_variance + 0.001) * F.abs("actual_amount")
    ).count()
    assert bad == 0


def test_grain_unique(spark):
    df = _build(spark)
    g = ["date_sk", "gl_account_sk", "store_sk", "department_sk", "plan_version"]
    assert df.groupBy(*g).count().filter(F.col("count") > 1).count() == 0


def test_deterministic(spark):
    a = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("plan_amount"), 2).alias("s")).first()
    b = _build(spark).agg(F.count("*").alias("n"), F.round(F.sum("plan_amount"), 2).alias("s")).first()
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_fact_budget_plan.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the builder**

Create `src/techmart/finance/fact_budget_plan.py`:

```python
"""fact_budget_plan: budget/forecast plans derived from actuals with variance."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..facts.gen import uniform_hash
from ..spark.framework import SparkColumn, SparkTableSpec

_BUDGET_SALT = 730_007

FACT_BUDGET_PLAN_SPEC = SparkTableSpec(
    schema="finance",
    name="fact_budget_plan",
    grain="one row per department × store × GL account × fiscal period × plan version",
    columns=[
        SparkColumn("date_sk", "long", "Period-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("gl_account_sk", "long", "GL account FK (dim_gl_account)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("department_sk", "long", "Department FK (dim_department)", is_key=True, nullable=False),
        SparkColumn("plan_version", "string", "Budget/Forecast/Latest-Estimate", is_key=True, nullable=False),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("plan_amount", "double", "Planned amount"),
        SparkColumn("plan_units", "long", "Planned units (proxy)"),
        SparkColumn("scenario", "string", "Planning scenario"),
    ],
)

_VERSIONS = ["Budget", "Forecast", "Latest-Estimate"]


def build_fact_budget_plan(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_gl_actuals: DataFrame,
    dim_gl_account: DataFrame,
) -> DataFrame:
    var = config.scale_profile.budget_variance
    pl_sks = dim_gl_account.filter(F.col("statement") == "P&L").select("gl_account_sk")
    base = fact_gl_actuals.join(pl_sks, "gl_account_sk")
    exploded = base.withColumn("plan_version", F.explode(F.array(*[F.lit(v) for v in _VERSIONS])))
    variance = (
        uniform_hash("store_sk", "gl_account_sk", "plan_version", salt=_BUDGET_SALT) * F.lit(2.0) - F.lit(1.0)
    ) * F.lit(var)
    out = (
        exploded.withColumn("plan_amount", F.round(F.col("actual_amount") * (F.lit(1.0) + variance), 2))
        .withColumn("plan_units", F.floor(F.abs("plan_amount") / F.lit(50.0)).cast("long"))
        .withColumn("scenario", F.lit("Base"))
    )
    return FACT_BUDGET_PLAN_SPEC.select_ordered(out)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_fact_budget_plan.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/finance/fact_budget_plan.py tests/test_fact_budget_plan.py
git commit -m "Add fact_budget_plan (budget-vs-actual)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: `fact_inventory_valuation`

**Files:**
- Create: `src/techmart/finance/fact_inventory_valuation.py`
- Test: `tests/test_fact_inventory_valuation.py`

**Interfaces:**
- Consumes: `date_periods`/`period_end_lookup` (Task 5); persisted `fact_inventory_snapshot` (`store_sk, product_sk, date_sk, on_hand_cost_value, on_hand_retail_value`), `fact_sales_line` (`store_sk, product_sk, date_sk, net_sales_amount, cogs_amount, gross_sales_amount`); `dim_product` (`product_sk, category_id, category_name`); `dim_date`; `config.scale_profile.{markdown_rate}`.
- Produces: `FACT_INVENTORY_VALUATION_SPEC`, `build_fact_inventory_valuation(spark, config, *, fact_inventory_snapshot, fact_sales_line, dim_product, dim_date) -> DataFrame`.
- **Design:**
  - `on_hand_*` from the **period-end** snapshot: join `fact_inventory_snapshot.date_sk` to `period_end_lookup.period_end_date_sk`, join `dim_product` for `category_id`/`category_name`, aggregate `on_hand_cost_value`/`on_hand_retail_value` by (`store_sk`, `category_id`, `period_end_date_sk`).
  - Sales rolled to (store, category, pidx): join `fact_sales_line` → `date_periods` → `dim_product`, aggregate `gross_sales_amount`, `net_sales_amount`, `cogs_amount`. Map pidx → period_end_date_sk via `period_end_lookup`.
  - `cogs_amount` = category sales COGS. `markdown_amount = round(markdown_rate * category_gross, 2)`. `shrink_amount` = derived from snapshot movement is out of scope at category grain; use `shrink_amount = round(0.005 * on_hand_cost_value, 2)` (documented category-level shrink proxy consistent in spirit with the GL shrink). `gmroi = round((net_sales - cogs) / greatest(on_hand_cost_value, 1.0), 4)`.
  - **Left-join sales onto the snapshot base** (a period-end store×category always has a snapshot; it may have no sales) and `fillna(0.0)` the sales-derived measures so every valuation row is a real inventory position.

**Spec columns:** `date_sk`(long,key), `store_sk`(long,key), `category_id`(string,key), `category_name`(string), `fiscal_year`(int), `fiscal_period`(int), `on_hand_cost_value`(double), `on_hand_retail_value`(double), `cogs_amount`(double), `markdown_amount`(double), `shrink_amount`(double), `gmroi`(double). Grain: `"one row per store × category × fiscal period"`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_fact_inventory_valuation.py`:

```python
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.spark.dimensions.dim_store import build_dim_store
from techmart.facts.fact_sales_line import build_fact_sales_line
from techmart.facts.fact_inventory_snapshot import build_fact_inventory_snapshot
from techmart.finance.fact_inventory_valuation import (
    FACT_INVENTORY_VALUATION_SPEC, build_fact_inventory_valuation,
)

_P = ScaleProfile("t", 6, 40, 1, 4000, 300, 20)
_CFG = TechmartConfig(scale_profile=_P, seed=42, output_dir=Path("data"),
                      catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31))
_COUNTS = {"store": 6, "customer": 300, "employee": _P.num_employees,
           "promotion": _P.num_promotions, "vendor": 20, "product": 40}


def _build(spark):
    dd = build_dim_date(spark, _CFG); dp = build_dim_product(spark, _CFG)
    ds = build_dim_store(spark, _CFG)
    snap = build_fact_inventory_snapshot(spark, _CFG, dim_store=ds, dim_product=dp, dim_date=dd)
    sales = build_fact_sales_line(spark, _CFG, dim_product=dp, dim_date=dd, dim_counts=_COUNTS, rows=4000)
    val = build_fact_inventory_valuation(spark, _CFG, fact_inventory_snapshot=snap,
                                         fact_sales_line=sales, dim_product=dp, dim_date=dd)
    return val, snap, dp, dd


def test_schema_and_grain(spark):
    val, *_ = _build(spark)
    assert val.columns == FACT_INVENTORY_VALUATION_SPEC.column_names
    g = ["store_sk", "category_id", "date_sk"]
    assert val.groupBy(*g).count().filter(F.col("count") > 1).count() == 0


def test_referential_integrity(spark):
    val, snap, dp, dd = _build(spark)
    cats = dp.select("category_id").distinct()
    assert val.select("date_sk").distinct().join(dd.select("date_sk"), "date_sk", "left_anti").count() == 0
    assert val.select("category_id").distinct().join(cats, "category_id", "left_anti").count() == 0


def test_measures_valid(spark):
    val, *_ = _build(spark)
    assert val.filter((F.col("on_hand_cost_value") < 0) | (F.col("shrink_amount") < 0)
                      | (F.col("markdown_amount") < 0)).count() == 0
    # gmroi finite (no divide-by-zero blowups)
    assert val.filter(F.col("gmroi").isNull() | F.isnan("gmroi")).count() == 0


def test_cost_value_ties_to_snapshot(spark):
    val, snap, dp, dd = _build(spark)
    from techmart.finance.periods import period_end_lookup
    pe = period_end_lookup(dd).select(F.col("period_end_date_sk").alias("date_sk"))
    snap_pe_total = snap.join(pe, "date_sk").agg(F.round(F.sum("on_hand_cost_value"), 2)).first()[0]
    val_total = val.agg(F.round(F.sum("on_hand_cost_value"), 2)).first()[0]
    assert abs(snap_pe_total - val_total) < 1.0


def test_deterministic(spark):
    a = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("on_hand_cost_value"), 2).alias("s")).first()
    b = _build(spark)[0].agg(F.count("*").alias("n"), F.round(F.sum("on_hand_cost_value"), 2).alias("s")).first()
    assert a == b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_fact_inventory_valuation.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the builder**

Create `src/techmart/finance/fact_inventory_valuation.py`:

```python
"""fact_inventory_valuation: finance view of inventory, ties to fact_inventory_snapshot."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .periods import date_periods, period_end_lookup

FACT_INVENTORY_VALUATION_SPEC = SparkTableSpec(
    schema="finance",
    name="fact_inventory_valuation",
    grain="one row per store × category × fiscal period",
    columns=[
        SparkColumn("date_sk", "long", "Period-end date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("store_sk", "long", "Store FK (dim_store)", is_key=True, nullable=False),
        SparkColumn("category_id", "string", "Product category FK (dim_product)", is_key=True, nullable=False),
        SparkColumn("category_name", "string", "Product category name"),
        SparkColumn("fiscal_year", "int", "Retail fiscal year"),
        SparkColumn("fiscal_period", "int", "Retail fiscal period (1-12)"),
        SparkColumn("on_hand_cost_value", "double", "Period-end inventory at cost"),
        SparkColumn("on_hand_retail_value", "double", "Period-end inventory at retail"),
        SparkColumn("cogs_amount", "double", "Category COGS for the period"),
        SparkColumn("markdown_amount", "double", "Injected markdown value"),
        SparkColumn("shrink_amount", "double", "Category-level shrink proxy"),
        SparkColumn("gmroi", "double", "Gross-margin return on inventory investment"),
    ],
)


def build_fact_inventory_valuation(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    fact_inventory_snapshot: DataFrame,
    fact_sales_line: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    markdown_rate = config.scale_profile.markdown_rate
    periods = date_periods(dim_date)
    pe = period_end_lookup(dim_date)
    cat = dim_product.select("product_sk", "category_id", "category_name")

    # period-end inventory position by (store, category, period-end date_sk)
    pe_sk = pe.select(
        F.col("period_end_date_sk").alias("date_sk"), "pidx", "fiscal_year", "fiscal_period"
    )
    base = (
        fact_inventory_snapshot.select("store_sk", "product_sk", "date_sk", "on_hand_cost_value", "on_hand_retail_value")
        .join(pe_sk, "date_sk")
        .join(cat, "product_sk")
        .groupBy("store_sk", "category_id", "category_name", "date_sk", "pidx", "fiscal_year", "fiscal_period")
        .agg(
            F.sum("on_hand_cost_value").alias("on_hand_cost_value"),
            F.sum("on_hand_retail_value").alias("on_hand_retail_value"),
        )
    )

    # sales rolled to (store, category, pidx)
    sales_cat = (
        fact_sales_line.select("store_sk", "product_sk", "date_sk", "gross_sales_amount", "net_sales_amount", "cogs_amount")
        .join(periods.select("date_sk", "pidx"), "date_sk")
        .join(cat.select("product_sk", "category_id"), "product_sk")
        .groupBy("store_sk", "category_id", "pidx")
        .agg(
            F.sum("gross_sales_amount").alias("cat_gross"),
            F.sum("net_sales_amount").alias("cat_net"),
            F.sum("cogs_amount").alias("cat_cogs"),
        )
    )

    out = (
        base.join(sales_cat, ["store_sk", "category_id", "pidx"], "left")
        .fillna(0.0, ["cat_gross", "cat_net", "cat_cogs"])
        .withColumn("cogs_amount", F.round("cat_cogs", 2))
        .withColumn("markdown_amount", F.round(F.lit(markdown_rate) * F.col("cat_gross"), 2))
        .withColumn("shrink_amount", F.round(F.lit(0.005) * F.col("on_hand_cost_value"), 2))
        .withColumn(
            "gmroi",
            F.round((F.col("cat_net") - F.col("cat_cogs")) / F.greatest(F.col("on_hand_cost_value"), F.lit(1.0)), 4),
        )
        .withColumn("on_hand_cost_value", F.round("on_hand_cost_value", 2))
        .withColumn("on_hand_retail_value", F.round("on_hand_retail_value", 2))
    )
    return FACT_INVENTORY_VALUATION_SPEC.select_ordered(out)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_fact_inventory_valuation.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/finance/fact_inventory_valuation.py tests/test_fact_inventory_valuation.py
git commit -m "Add fact_inventory_valuation

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Finance registry

**Files:**
- Create: `src/techmart/finance/registry.py`
- Test: `tests/test_finance_registry.py`

**Interfaces:**
- Consumes: the five finance specs (Tasks 3, 4, 6, 7, 8).
- Produces: `FINANCE_SPECS: list[SparkTableSpec]` — all five, used by wiring/verification.

- [ ] **Step 1: Write the failing test**

Create `tests/test_finance_registry.py`:

```python
from techmart.finance.registry import FINANCE_SPECS


def test_all_specs_present():
    names = {s.name for s in FINANCE_SPECS}
    assert names == {
        "dim_department", "dim_gl_account", "fact_gl_actuals",
        "fact_budget_plan", "fact_inventory_valuation",
    }


def test_all_finance_schema_and_grain():
    for s in FINANCE_SPECS:
        assert s.schema == "finance"
        assert s.grain and isinstance(s.grain, str)
    assert len({s.name for s in FINANCE_SPECS}) == len(FINANCE_SPECS)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_finance_registry.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the registry**

Create `src/techmart/finance/registry.py`:

```python
"""Registry of techmart_finance table specs."""
from __future__ import annotations

from ..spark.framework import SparkTableSpec
from .dim_department import DIM_DEPARTMENT_SPEC
from .dim_gl_account import DIM_GL_ACCOUNT_SPEC
from .fact_budget_plan import FACT_BUDGET_PLAN_SPEC
from .fact_gl_actuals import FACT_GL_ACTUALS_SPEC
from .fact_inventory_valuation import FACT_INVENTORY_VALUATION_SPEC

FINANCE_SPECS: list[SparkTableSpec] = [
    DIM_DEPARTMENT_SPEC,
    DIM_GL_ACCOUNT_SPEC,
    FACT_GL_ACTUALS_SPEC,
    FACT_BUDGET_PLAN_SPEC,
    FACT_INVENTORY_VALUATION_SPEC,
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_finance_registry.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/techmart/finance/registry.py tests/test_finance_registry.py
git commit -m "Add finance table registry

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 10: Notebook + job wiring (`generate_finance`)

**Files:**
- Create: `notebooks/generate_finance.py`
- Create: `src/techmart/jobs/generate_finance.py`
- Modify: `resources/generate_facts_job.yml` (add `generate_finance` task, `depends_on: generate_facts`)
- Test: `tests/test_generate_finance.py`, and extend `tests/test_dab_bundle.py`

**Interfaces:**
- Consumes: `FINANCE_SPECS`, all finance builders, `write_table_uc`, `load_config`. Reads persisted core tables via `spark.read.table` from `<catalog>.<schema_prefix>core`.
- Produces: `src/techmart/jobs/generate_finance.py::main(spark, config, catalog, schema_prefix)` mirroring the notebook sequence with no `dbutils`.
- **Build order in both:** `dim_department`, `dim_gl_account` (independent) → read core `fact_sales_line`, `fact_returns`, `fact_inventory_movement`, `fact_inventory_snapshot`, `dim_product`, `dim_date` → `fact_gl_actuals` (needs sales/returns/movement + the two finance dims) → `fact_budget_plan` (needs the persisted `fact_gl_actuals` + `dim_gl_account`) → `fact_inventory_valuation` (needs snapshot/sales/dim_product/dim_date). Write each via `write_table_uc`; re-read `fact_gl_actuals` from UC before building budget so budget builds off the persisted rows.

Look at `tests/test_generate_facts.py` and `tests/test_notebooks.py` for the exact import-smoke / notebook-parse patterns already used, and mirror them.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_generate_finance.py` (mirror `tests/test_generate_facts.py`'s structure — a local end-to-end build at tiny scale writing to a temp warehouse dir or asserting the `main` sequence runs and produces the 5 tables). Concretely:

```python
import ast
from pathlib import Path

_NB = Path(__file__).parent.parent / "notebooks" / "generate_finance.py"
_JOB = Path(__file__).parent.parent / "src" / "techmart" / "jobs" / "generate_finance.py"


def test_notebook_parses_and_covers_all_tables():
    src = _NB.read_text()
    ast.parse(src)
    for name in ("dim_department", "dim_gl_account", "fact_gl_actuals",
                 "fact_budget_plan", "fact_inventory_valuation"):
        assert name in src


def test_job_module_parses_and_has_main():
    src = _JOB.read_text()
    tree = ast.parse(src)
    assert any(isinstance(n, ast.FunctionDef) and n.name == "main" for n in tree.body)
    assert "dbutils" not in src
```

Add to `tests/test_dab_bundle.py` (mirror the existing YAML-shape assertions there):

```python
def test_generate_finance_task_wired():
    import yaml, pathlib
    y = yaml.safe_load((pathlib.Path(__file__).parent.parent / "resources" / "generate_facts_job.yml").read_text())
    tasks = y["resources"]["jobs"]["generate_facts"]["tasks"]
    by_key = {t["task_key"]: t for t in tasks}
    assert "generate_finance" in by_key
    deps = {d["task_key"] for d in by_key["generate_finance"].get("depends_on", [])}
    assert "generate_facts" in deps
    assert by_key["generate_finance"]["notebook_task"]["notebook_path"].endswith("generate_finance.py")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_generate_finance.py tests/test_dab_bundle.py -v`
Expected: FAIL (files missing / task not wired).

- [ ] **Step 3: Write the notebook**

Create `notebooks/generate_finance.py` (mirror `notebooks/generate_facts.py` header/widgets exactly):

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
from techmart.finance.dim_department import DIM_DEPARTMENT_SPEC, build_dim_department
from techmart.finance.dim_gl_account import DIM_GL_ACCOUNT_SPEC, build_dim_gl_account
from techmart.finance.fact_gl_actuals import FACT_GL_ACTUALS_SPEC, build_fact_gl_actuals
from techmart.finance.fact_budget_plan import FACT_BUDGET_PLAN_SPEC, build_fact_budget_plan
from techmart.finance.fact_inventory_valuation import FACT_INVENTORY_VALUATION_SPEC, build_fact_inventory_valuation

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
config = load_config(
    Path("../config/scale_profiles.yaml"), dbutils.widgets.get("scale_profile"),
    seed=int(dbutils.widgets.get("seed")), catalog=catalog, schema_prefix=schema_prefix,
)
core = f"{catalog}.{schema_prefix}core"
fin = f"{catalog}.{schema_prefix}finance"

dim_date = spark.read.table(f"{core}.dim_date")
dim_product = spark.read.table(f"{core}.dim_product")
sales = spark.read.table(f"{core}.fact_sales_line")
returns = spark.read.table(f"{core}.fact_returns")
movement = spark.read.table(f"{core}.fact_inventory_movement")
snapshot = spark.read.table(f"{core}.fact_inventory_snapshot")

# --- finance dims ---
dim_department = build_dim_department(spark, config)
print("wrote", write_table_uc(spark, dim_department, DIM_DEPARTMENT_SPEC, catalog, schema_prefix))
dim_gl_account = build_dim_gl_account(spark, config)
print("wrote", write_table_uc(spark, dim_gl_account, DIM_GL_ACCOUNT_SPEC, catalog, schema_prefix))

# --- gl actuals (derived) ---
actuals = build_fact_gl_actuals(spark, config, fact_sales_line=sales, fact_returns=returns,
                                fact_inventory_movement=movement, dim_date=dim_date,
                                dim_gl_account=dim_gl_account, dim_department=dim_department)
print("wrote", write_table_uc(spark, actuals, FACT_GL_ACTUALS_SPEC, catalog, schema_prefix))
actuals = spark.read.table(f"{fin}.fact_gl_actuals")

# --- budget (off persisted actuals) ---
budget = build_fact_budget_plan(spark, config, fact_gl_actuals=actuals, dim_gl_account=dim_gl_account)
print("wrote", write_table_uc(spark, budget, FACT_BUDGET_PLAN_SPEC, catalog, schema_prefix))

# --- inventory valuation (derived) ---
val = build_fact_inventory_valuation(spark, config, fact_inventory_snapshot=snapshot,
                                     fact_sales_line=sales, dim_product=dim_product, dim_date=dim_date)
print("wrote", write_table_uc(spark, val, FACT_INVENTORY_VALUATION_SPEC, catalog, schema_prefix))

for t in ("dim_department", "dim_gl_account", "fact_gl_actuals", "fact_budget_plan", "fact_inventory_valuation"):
    print(t, spark.table(f"{fin}.{t}").count())
```

- [ ] **Step 4: Write the job module**

Create `src/techmart/jobs/generate_finance.py` — a `main(spark, config, catalog, schema_prefix)` mirroring the notebook sequence, no `dbutils` (look at `src/techmart/jobs/generate_facts.py` for the exact shape, imports, and how it reads core tables and calls `write_table_uc`).

- [ ] **Step 5: Wire the DAB task**

In `resources/generate_facts_job.yml`, append a third task after `generate_facts`:

```yaml
        - task_key: generate_finance
          depends_on:
            - task_key: generate_facts
          notebook_task:
            notebook_path: ../notebooks/generate_finance.py
            base_parameters:
              catalog: ${var.catalog}
              schema_prefix: ${var.schema_prefix}
              scale_profile: ${var.scale_profile}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/test_generate_finance.py tests/test_dab_bundle.py tests/test_notebooks.py -v`
Expected: PASS. Then run the full suite: `python -m pytest -q` — all green.

- [ ] **Step 7: Commit**

```bash
git add notebooks/generate_finance.py src/techmart/jobs/generate_finance.py resources/generate_facts_job.yml tests/test_generate_finance.py tests/test_dab_bundle.py
git commit -m "Wire generate_finance notebook, job, and DAB task

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 11: Deploy + verify on the workspace

**Files:** none (deploy/verify task; produces evidence, not code).

This task deploys the finance layer to field-eng-east and verifies counts, RI, and the reconciliation invariants on real data — first at `smoke`, then at `showcase`. Use `/opt/homebrew/bin/databricks` (v1.6.0); NEVER set `DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION=1`. Warehouse `ec3c986a891e0b79` (techmart_warehouse). Catalog `stable_classic_ppke9o`.

- [ ] **Step 1: Validate the bundle**

Run: `/opt/homebrew/bin/databricks bundle validate -t dev -p field-eng-east`
Expected: valid; the job now shows three tasks (generate_dims → generate_facts → generate_finance).

- [ ] **Step 2: Deploy and run at smoke**

```bash
/opt/homebrew/bin/databricks bundle deploy -t dev -p field-eng-east
/opt/homebrew/bin/databricks bundle run generate_facts -t dev -p field-eng-east
```
Expected: all three tasks TERMINATED SUCCESS. Note: `dev` defaults to `smoke`.

- [ ] **Step 3: Verify at smoke via Statement Execution API** (warehouse `ec3c986a891e0b79`)

Confirm on `stable_classic_ppke9o.techmart_finance`:
- All 5 tables exist with rows; `dim_department` = 7, `dim_gl_account` = len(GL_ACCOUNTS).
- **RI:** `fact_gl_actuals`/`fact_budget_plan` `date_sk`, `gl_account_sk`, `department_sk`, `store_sk` — 0 orphans against core dims + `dim_gl_account`/`dim_department`. `fact_inventory_valuation` `date_sk`/`category_id` — 0 orphans.
- **Reconciliation (the payoff):** Σ Gross Product Sales (account `4000`) == Σ `techmart_core.fact_sales_line.gross_sales_amount` (within penny tolerance); Net Sales (accounts 4000+4100+4200) == Σ gross − Σ returns − allowance_rate×Σ gross.
- `fact_inventory_valuation` Σ `on_hand_cost_value` == period-end Σ from `fact_inventory_snapshot` (within tolerance).
- 100% column comments + table-level grain comments on all 5 finance tables.

- [ ] **Step 4: Run at showcase**

```bash
/opt/homebrew/bin/databricks bundle run generate_facts -t dev -p field-eng-east --var="scale_profile=showcase"
```
Expected: TERMINATED SUCCESS. Re-verify the RI + reconciliation invariants at showcase scale. Record counts and evidence in the progress ledger.

- [ ] **Step 5: Record evidence**

Append counts, reconciliation deltas, and RI results to `.superpowers/sdd/progress.md`. No code changes expected from the deploy.

---

## Self-Review

**Spec coverage:** dim_department ✅(T3) · dim_gl_account ✅(T4) · fact_gl_actuals w/ derive+inject+timing ✅(T6) · fact_budget_plan ✅(T7) · fact_inventory_valuation, vendor_sk dropped ✅(T8) · reconciliation invariants ✅(Global Constraints + T6/T11) · config levers ✅(T1) · CoA reference ✅(T2) · fiscal-period helper ✅(T5) · registry ✅(T9) · notebook/job/DAB wiring ✅(T10) · deploy proof ✅(T11).

**Placeholder scan:** every code step carries complete code except T10 Step 4 (`jobs/generate_finance.py` body) and T11 (deploy commands), which reference the exact existing files to mirror (`jobs/generate_facts.py`) and give the precise sequence/imports — no vague "add handling."

**Type consistency:** `pidx` int everywhere; period-end join key `period_end_date_sk`→`date_sk`(long); `account_number` string join key across CoA/dims/facts; `department_name` string join key; all `*_sk` long; `plan_version` string key. Reconciliation sign convention (contra negative) stated once in Global Constraints and used in T6/T7/T11.
