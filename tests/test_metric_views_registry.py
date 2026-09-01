import re

from techmart.ai.registry import AI_SPECS
from techmart.facts.registry import FACT_SPECS
from techmart.finance.registry import FINANCE_SPECS
from techmart.semantic.metric_views import METRIC_VIEW_SPECS

def _all_specs():
    from techmart.spark.dimensions.dim_date import DIM_DATE_SPEC
    from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC
    from techmart.spark.dimensions.dim_store import DIM_STORE_SPEC
    from techmart.spark.dimensions.dim_customer import DIM_CUSTOMER_SPEC
    from techmart.spark.dimensions.dim_channel import DIM_CHANNEL_SPEC
    from techmart.spark.dimensions.dim_promotion import DIM_PROMOTION_SPEC
    from techmart.spark.dimensions.dim_vendor import DIM_VENDOR_SPEC
    from techmart.spark.dimensions.dim_employee import DIM_EMPLOYEE_SPEC
    dims = [DIM_DATE_SPEC, DIM_PRODUCT_SPEC, DIM_STORE_SPEC, DIM_CUSTOMER_SPEC,
            DIM_CHANNEL_SPEC, DIM_PROMOTION_SPEC, DIM_VENDOR_SPEC, DIM_EMPLOYEE_SPEC]
    facts = list(FACT_SPECS.values()) + list(FINANCE_SPECS) + list(AI_SPECS)
    return {(s.schema, s.name): set(s.column_names) for s in dims + facts}


def test_six_views_distinct_names():
    names = [v.name for v in METRIC_VIEW_SPECS]
    assert names == ["mv_sales", "mv_inventory", "mv_inventory_valuation",
                     "mv_forecast", "mv_gl_actuals", "mv_budget_plan"]
    assert len(set(names)) == 6


def test_sources_are_real_tables():
    cols = _all_specs()
    for v in METRIC_VIEW_SPECS:
        assert (v.source_schema, v.source_table) in cols


def test_flagship_views_are_materialized():
    by = {v.name: v for v in METRIC_VIEW_SPECS}
    assert by["mv_sales"].materialization is not None
    assert by["mv_inventory"].materialization is not None
    # materialized dims/measures must reference this view's own field names
    for name in ("mv_sales", "mv_inventory"):
        v = by[name]
        dim_names = {d.name for d in v.dimensions}
        meas_names = {m.name for m in v.measures}
        for mv in v.materialization.materialized_views:
            assert set(mv.dimensions) <= dim_names
            assert set(mv.measures) <= meas_names


def _refs(expr):
    # extract alias.column tokens (e.g. source.net_sales_amount, dim_date.fiscal_year)
    return re.findall(r"\b([a-z_]+)\.([a-z_]+)\b", expr)


def test_every_expr_references_valid_columns():
    cols = _all_specs()
    for v in METRIC_VIEW_SPECS:
        alias_to_table = {"source": (v.source_schema, v.source_table)}
        for j in v.joins:
            alias_to_table[j.name] = (j.schema, j.table)
        for f in list(v.dimensions) + list(v.measures):
            for alias, col in _refs(f.expr):
                assert alias in alias_to_table, f"{v.name}.{f.name}: unknown alias {alias}"
                schema, table = alias_to_table[alias]
                assert col in cols[(schema, table)], \
                    f"{v.name}.{f.name}: {alias}.{col} not on {schema}.{table}"


def test_join_on_predicates_reference_valid_columns():
    cols = _all_specs()
    for v in METRIC_VIEW_SPECS:
        alias_to_table = {"source": (v.source_schema, v.source_table)}
        for j in v.joins:
            alias_to_table[j.name] = (j.schema, j.table)
        for j in v.joins:
            for alias, col in _refs(j.on):
                assert alias in alias_to_table
                assert col in cols[alias_to_table[alias]]
