from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

DEFAULT_PROFILE: str = "showcase"

ASSOCIATES_PER_STORE = 40
CAMPAIGNS_PER_YEAR = 60


@dataclass(frozen=True)
class ScaleProfile:
    name: str
    num_stores: int
    num_skus: int
    history_years: int
    sales_lines_target: int
    num_customers: int
    num_vendors: int
    inventory_snapshot_days: int = 7
    inventory_movements_target: int = 1000
    web_events_target: int = 1000
    # Finance reconciliation levers (behavioral; shared across profiles via defaults).
    allowance_rate: float = 0.010
    markdown_rate: float = 0.015
    timing_shift_pct: float = 0.05
    budget_variance: float = 0.08
    # AI layer levers (Phase 6).
    num_reviews: int = 200
    num_service_cases: int = 100
    forecast_active_products: int = 200
    forecast_horizon_weeks: int = 26

    @property
    def num_employees(self) -> int:
        """Derived associate headcount across all stores."""
        return ASSOCIATES_PER_STORE * self.num_stores

    @property
    def num_promotions(self) -> int:
        """Derived promotion/campaign count across the history window."""
        return CAMPAIGNS_PER_YEAR * self.history_years


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
    name = profile_name if profile_name is not None else DEFAULT_PROFILE
    return TechmartConfig(
        scale_profile=profiles[name],
        seed=seed,
        output_dir=Path(output_dir),
        catalog=catalog,
        schema_prefix=schema_prefix,
        end_date=end_date,
    )
