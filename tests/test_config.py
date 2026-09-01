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


def test_loads_required_profiles():
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


def test_scale_profiles_have_phase4_knobs():
    profiles = load_profiles(PROFILES)
    for name in ("smoke", "demo_lean", "showcase", "stress"):
        p = profiles[name]
        assert p.inventory_snapshot_days >= 1
        assert p.inventory_movements_target >= 1
        assert p.web_events_target >= 1
    # smoke is intentionally tiny so the deploy proof is fast
    smoke = profiles["smoke"]
    assert smoke.inventory_snapshot_days == 30
    assert smoke.inventory_movements_target == 20000
    assert smoke.web_events_target == 100000


def test_scale_profile_defaults_keep_positional_construction():
    p = ScaleProfile("t", 5, 500, 1, 50000, 1000, 20)
    assert p.inventory_snapshot_days == 7
    assert p.inventory_movements_target == 1000
    assert p.web_events_target == 1000


def test_finance_levers_default():
    from techmart.config import load_config
    import pathlib
    p = pathlib.Path(__file__).parent.parent / "config" / "scale_profiles.yaml"
    cfg = load_config(p, "smoke")
    sp = cfg.scale_profile
    assert sp.allowance_rate == 0.010
    assert sp.markdown_rate == 0.015
    assert sp.timing_shift_pct == 0.05
    assert sp.budget_variance == 0.08
