from datetime import date
from pathlib import Path

import polars as pl
import pytest

from techmart.cli import generate, main
from techmart.config import load_config

PROFILES = Path("config/scale_profiles.yaml")


def test_generate_writes_dim_date(tmp_path: Path):
    cfg = load_config(PROFILES, "demo_lean", output_dir=tmp_path, end_date=date(2026, 1, 31))
    paths = generate(cfg, ["dim_date"])
    assert paths == [tmp_path / "core" / "dim_date.parquet"]
    df = pl.read_parquet(paths[0])
    # demo_lean = 2 years history -> 2024-01-31 .. 2026-01-31 inclusive.
    assert df.height == (date(2026, 1, 31) - date(2024, 1, 31)).days + 1


def test_generate_rejects_unknown_table(tmp_path: Path):
    cfg = load_config(PROFILES, "demo_lean", output_dir=tmp_path)
    with pytest.raises(ValueError):
        generate(cfg, ["dim_unicorn"])


def test_main_returns_zero_and_writes(tmp_path: Path):
    code = main(
        [
            "--profile", "demo_lean",
            "--output-dir", str(tmp_path),
            "--tables", "dim_date",
            "--profiles-path", str(PROFILES),
        ]
    )
    assert code == 0
    assert (tmp_path / "core" / "dim_date.parquet").exists()
