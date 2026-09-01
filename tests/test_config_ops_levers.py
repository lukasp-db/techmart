from pathlib import Path

from techmart.config import load_profiles

_YAML = Path(__file__).resolve().parents[1] / "config" / "scale_profiles.yaml"


def test_ops_levers_present_and_positive():
    profiles = load_profiles(_YAML)
    for name in ["smoke", "demo_lean", "showcase", "stress"]:
        p = profiles[name]
        assert p.num_replen_orders > 0
        assert p.num_forecast_overrides > 0
        assert p.forecast_serving_rows > 0


def test_ops_levers_scale_up():
    profiles = load_profiles(_YAML)
    assert profiles["smoke"].num_replen_orders <= profiles["showcase"].num_replen_orders
    assert profiles["smoke"].forecast_serving_rows <= profiles["showcase"].forecast_serving_rows
    assert profiles["smoke"].num_forecast_overrides <= profiles["showcase"].num_forecast_overrides
