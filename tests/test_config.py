from datetime import date
from pathlib import Path

import pytest

from techmart.config import (
    DEFAULT_PROFILE,
    ScaleProfile,
    load_config,
    load_profiles,
)

PROFILES = Path("config/scale_profiles.yaml")


def test_loads_all_three_profiles():
    profiles = load_profiles(PROFILES)
    assert {"demo_lean", "showcase", "stress", "smoke"} <= set(profiles)
    assert all(isinstance(p, ScaleProfile) for p in profiles.values())


def test_default_profile_is_showcase():
    cfg = load_config(PROFILES)
    assert cfg.scale_profile.name == DEFAULT_PROFILE == "showcase"


def test_start_date_derived_from_history_years():
    cfg = load_config(PROFILES, "showcase", end_date=date(2026, 1, 31))
    assert cfg.scale_profile.history_years == 3
    assert cfg.start_date == date(2023, 1, 31)


def test_unknown_profile_raises():
    with pytest.raises(KeyError):
        load_config(PROFILES, "does_not_exist")


def test_profiles_carry_customer_and_vendor_counts():
    cfg = load_config(PROFILES, "showcase")
    assert cfg.scale_profile.num_customers == 5_000_000
    assert cfg.scale_profile.num_vendors == 5_000


def test_derived_employee_and_promotion_counts():
    cfg = load_config(PROFILES, "showcase")  # 1000 stores, 3 years history
    assert cfg.scale_profile.num_employees == 40 * 1000
    assert cfg.scale_profile.num_promotions == 60 * 3
