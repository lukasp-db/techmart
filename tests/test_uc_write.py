from techmart.spark.framework import SparkColumn, SparkTableSpec
from techmart.spark.uc_write import target_table_name

SPEC = SparkTableSpec(
    schema="core", name="dim_demo", grain="one per demo",
    columns=[SparkColumn("demo_sk", "long", "Surrogate key", is_key=True, nullable=False)],
)


def test_target_table_name():
    assert target_table_name(SPEC, "cat", "techmart_") == "cat.techmart_core.dim_demo"
