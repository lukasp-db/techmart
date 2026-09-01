from techmart.finance.registry import FINANCE_SPECS


def test_all_specs_present():
    names = {s.name for s in FINANCE_SPECS}
    assert names == {
        "dim_department", "dim_gl_account", "fact_gl_actuals",
        "fact_budget_plan", "fact_inventory_valuation",
    }


def test_all_finance_schema_and_grain():
    for s in FINANCE_SPECS:
        assert s.schema == "finance"
        assert s.grain and isinstance(s.grain, str)
    assert len({s.name for s in FINANCE_SPECS}) == len(FINANCE_SPECS)
