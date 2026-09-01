from techmart.facts.registry import FACT_SPECS


def test_registry_has_all_core_facts():
    assert set(FACT_SPECS) == {
        "fact_sales_line",
        "fact_inventory_snapshot",
        "fact_inventory_movement",
        "fact_returns",
        "fact_fulfillment",
        "fact_loyalty_activity",
        "fact_web_events",
    }
    for name, spec in FACT_SPECS.items():
        assert spec.name == name
        assert spec.schema == "core"
        assert spec.grain  # non-empty grain string for the table COMMENT
