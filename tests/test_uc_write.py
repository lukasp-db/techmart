from techmart.spark.framework import SparkColumn, SparkTableSpec
from techmart.spark.uc_write import target_table_name, table_comment_sql

SPEC = SparkTableSpec(
    schema="core", name="dim_demo", grain="one per demo",
    columns=[SparkColumn("demo_sk", "long", "Surrogate key", is_key=True, nullable=False)],
)


def test_target_table_name():
    assert target_table_name(SPEC, "cat", "techmart_") == "cat.techmart_core.dim_demo"


def test_table_comment_sql_basic():
    assert (
        table_comment_sql("cat.techmart_core.fact_x", "one row per widget")
        == "COMMENT ON TABLE cat.techmart_core.fact_x IS 'one row per widget'"
    )


def test_table_comment_sql_escapes_single_quotes():
    assert table_comment_sql("t", "customer's basket") == "COMMENT ON TABLE t IS 'customer''s basket'"
