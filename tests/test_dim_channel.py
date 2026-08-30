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
