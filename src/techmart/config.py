from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

DEFAULT_PROFILE = "showcase"


@dataclass(frozen=True)
class ScaleProfile:
    name: str
    num_stores: int
    num_skus: int
    history_years: int
    sales_lines_target: int


@dataclass(frozen=True)
class TechmartConfig:
    scale_profile: ScaleProfile
    seed: int
    output_dir: Path
    catalog: str
    schema_prefix: str
    end_date: date

    @property
    def start_date(self) -> date:
        """First calendar day of the generated history window."""
        target_year = self.end_date.year - self.scale_profile.history_years
        try:
            return self.end_date.replace(year=target_year)
        except ValueError:
            # Handle Feb 29 end dates on non-leap target years.
            return self.end_date.replace(year=target_year, day=28)


def load_profiles(path: Path) -> dict[str, ScaleProfile]:
    raw = yaml.safe_load(Path(path).read_text())
    return {
        name: ScaleProfile(name=name, **cfg)
        for name, cfg in raw["profiles"].items()
    }


def load_config(
    profiles_path: Path,
    profile_name: str | None = None,
    *,
    seed: int = 42,
    output_dir: Path = Path("data"),
    catalog: str = "techmart",
    schema_prefix: str = "techmart_",
    end_date: date = date(2026, 1, 31),
) -> TechmartConfig:
    profiles = load_profiles(profiles_path)
    name = profile_name or DEFAULT_PROFILE
    return TechmartConfig(
        scale_profile=profiles[name],
        seed=seed,
        output_dir=Path(output_dir),
        catalog=catalog,
        schema_prefix=schema_prefix,
        end_date=end_date,
    )
