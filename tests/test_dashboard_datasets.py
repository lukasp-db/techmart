from techmart.dashboards import datasets as D

def _sql(lines): return "\n".join(lines)

def test_all_datasets_registered_with_stable_names():
    names = {d.name for d in D.DATASETS}
    assert names == {"sales", "inventory", "bridge", "lost_sales", "ai_takeaways"}
    for d in D.DATASETS:
        assert d.query_lines and all(isinstance(x, str) for x in d.query_lines)

def test_dataset_sql_is_unqualified():
    # never hard-code catalog/schema; tables referenced bare
    for d in D.DATASETS:
        s = _sql(d.query_lines)
        assert "stable_classic_ppke9o" not in s
        assert ".semantic." not in s and "techmart_" not in s

def test_sales_uses_measure_and_real_measures():
    s = _sql(D.sales_querylines())
    assert "MEASURE(net_sales)" in s and "MEASURE(gross_margin)" in s and "MEASURE(units)" in s
    assert "FROM mv_sales" in s

def test_inventory_uses_latest_snapshot():
    s = _sql(D.inventory_querylines())
    assert "MAX(date)" in s and "FROM mv_inventory" in s
    assert "MEASURE(on_hand_qty)" in s and "MEASURE(on_hand_cost_value)" in s

def test_bridge_ratios_are_nullif_guarded():
    s = _sql(D.bridge_querylines())
    for ratio in ("sell_through_pct", "weeks_of_supply", "gmroi", "inventory_turns"):
        assert ratio in s
    assert s.count("NULLIF(") >= 4  # one guard per ratio, at least

def test_ai_takeaways_calls_ai_query_with_endpoint_placeholder():
    s = _sql(D.ai_takeaways_querylines())
    assert "ai_query(" in s and ":llm_endpoint" in s  # dashboard param, injected at deploy
    assert "takeaways" in s
