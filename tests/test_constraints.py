from techmart.semantic.constraints import (
    ForeignKey, TableConstraints, drop_pk_ddl, fk_ddl, pk_ddl,
)

_TC = TableConstraints(
    schema="core", table="fact_sales_line",
    primary_key=("transaction_id", "line_number"),
    foreign_keys=(
        ForeignKey(columns=("date_sk",), ref_schema="core", ref_table="dim_date",
                   ref_columns=("date_sk",)),
        ForeignKey(columns=("promotion_sk",), ref_schema="core", ref_table="dim_promotion",
                   ref_columns=("promotion_sk",)),
    ),
)


def test_pk_ddl_rely():
    sql = pk_ddl(_TC, catalog="cat", schema_prefix="tm_")
    assert sql == (
        "ALTER TABLE cat.tm_core.fact_sales_line "
        "ADD CONSTRAINT fact_sales_line_pk "
        "PRIMARY KEY (transaction_id, line_number) NOT ENFORCED RELY;"
    )


def test_fk_ddl_rely():
    sql = fk_ddl(_TC, _TC.foreign_keys[0], catalog="cat", schema_prefix="tm_")
    assert sql == (
        "ALTER TABLE cat.tm_core.fact_sales_line "
        "ADD CONSTRAINT fact_sales_line_date_sk_fk "
        "FOREIGN KEY (date_sk) REFERENCES cat.tm_core.dim_date (date_sk) "
        "NOT ENFORCED RELY;"
    )


def test_fk_constraint_names_unique_per_table():
    names = {fk_ddl(_TC, fk, catalog="c", schema_prefix="tm_").split("ADD CONSTRAINT ")[1].split(" ")[0]
             for fk in _TC.foreign_keys}
    assert len(names) == len(_TC.foreign_keys)


def test_drop_pk_ddl_if_exists():
    sql = drop_pk_ddl(_TC, catalog="cat", schema_prefix="tm_")
    assert sql == (
        "ALTER TABLE cat.tm_core.fact_sales_line "
        "DROP CONSTRAINT IF EXISTS fact_sales_line_pk CASCADE;"
    )


def test_set_not_null_ddls():
    from techmart.semantic.constraints import set_not_null_ddls
    sqls = set_not_null_ddls(_TC, catalog="cat", schema_prefix="tm_")
    assert sqls == [
        "ALTER TABLE cat.tm_core.fact_sales_line ALTER COLUMN transaction_id SET NOT NULL;",
        "ALTER TABLE cat.tm_core.fact_sales_line ALTER COLUMN line_number SET NOT NULL;",
    ]
