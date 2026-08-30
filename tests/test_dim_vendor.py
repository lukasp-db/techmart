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
