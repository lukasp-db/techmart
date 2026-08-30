from datetime import date
from pathlib import Path

from techmart.config import load_config
from techmart.dimensions.dim_date import DIM_DATE_SPEC
from techmart.framework.writer import validate_schema
from techmart.registry import REGISTRY, TableBuilder

PROFILES = Path("config/scale_profiles.yaml")


def test_registry_contains_dim_date():
    assert "dim_date" in REGISTRY
    assert isinstance(REGISTRY["dim_date"], TableBuilder)
    assert REGISTRY["dim_date"].spec is DIM_DATE_SPEC


def test_registry_builder_produces_conforming_dataframe():
    cfg = load_config(PROFILES, "demo_lean", end_date=date(2026, 1, 31))
    df = REGISTRY["dim_date"].build(cfg)
    validate_schema(df, DIM_DATE_SPEC)  # no raise
