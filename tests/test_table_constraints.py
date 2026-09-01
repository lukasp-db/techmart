from techmart.ai.registry import AI_SPECS
from techmart.facts.registry import FACT_SPECS
from techmart.finance.registry import FINANCE_SPECS
from techmart.semantic.registry import METRIC_VIEW_SPECS, TABLE_CONSTRAINTS


def _spec_index():
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
    return {(s.schema, s.name): s for s in dims + facts}


def test_registry_reexports():
    assert len(METRIC_VIEW_SPECS) == 6
    assert len(TABLE_CONSTRAINTS) >= 21  # 10 dims + 11 facts


def test_pk_columns_exist_and_not_null():
    idx = _spec_index()
    for tc in TABLE_CONSTRAINTS:
        spec = idx[(tc.schema, tc.table)]
        by = {c.name: c for c in spec.columns}
        assert tc.primary_key, f"{tc.table} has no PK"
        for col in tc.primary_key:
            assert col in by, f"{tc.table}.{col} missing"
            assert by[col].nullable is False, f"{tc.table}.{col} PK column must be NOT NULL"


def test_fk_targets_exist_and_are_pks():
    idx = _spec_index()
    pk_by_table = {(tc.schema, tc.table): tc.primary_key for tc in TABLE_CONSTRAINTS}
    for tc in TABLE_CONSTRAINTS:
        spec_cols = {c.name for c in idx[(tc.schema, tc.table)].columns}
        for fk in tc.foreign_keys:
            for col in fk.columns:
                assert col in spec_cols, f"{tc.table}.{col} FK column missing"
            ref = (fk.ref_schema, fk.ref_table)
            assert ref in idx, f"{tc.table}: FK target {ref} not a known table"
            ref_cols = {c.name for c in idx[ref].columns}
            for col in fk.ref_columns:
                assert col in ref_cols, f"{ref}.{col} missing"
            # FK must reference the target's declared PK
            assert tuple(fk.ref_columns) == tuple(pk_by_table[ref]), \
                f"{tc.table}: FK to {ref} must reference its PK {pk_by_table[ref]}"


def test_fk_columns_are_marked_is_key_on_facts():
    idx = _spec_index()
    for tc in TABLE_CONSTRAINTS:
        spec = idx[(tc.schema, tc.table)]
        if not tc.table.startswith("fact_"):
            continue
        key_cols = {c.name for c in spec.columns if c.is_key}
        for fk in tc.foreign_keys:
            for col in fk.columns:
                assert col in key_cols, f"{tc.table}.{col} should be is_key=True"


def test_every_gold_fact_has_constraints():
    covered = {(tc.schema, tc.table) for tc in TABLE_CONSTRAINTS}
    for name in FACT_SPECS:  # core facts
        assert ("core", name) in covered
    for s in FINANCE_SPECS:
        if s.name.startswith("fact_"):
            assert (s.schema, s.name) in covered
    assert ("ai", "fact_sales_forecast") in covered
