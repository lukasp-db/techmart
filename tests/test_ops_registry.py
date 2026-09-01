from techmart.ops.registry import OPS_SPECS


def test_ops_specs_contents():
    names = [s.name for s in OPS_SPECS]
    assert names == ["replenishment_order", "forecast_override"]
    assert len(set(names)) == len(names)
    for s in OPS_SPECS:
        assert s.primary_key  # every ops table has a PK
