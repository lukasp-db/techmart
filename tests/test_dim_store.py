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
