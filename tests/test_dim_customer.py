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
