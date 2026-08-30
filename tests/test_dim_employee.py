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
