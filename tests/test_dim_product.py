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
