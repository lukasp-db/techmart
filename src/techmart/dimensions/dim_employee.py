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
