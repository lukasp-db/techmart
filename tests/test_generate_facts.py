from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.fact_sales_line import FACT_SALES_LINE_SPEC
from techmart.facts.registry import FACT_SPECS
from techmart.jobs.generate_facts import generate_sales_line_local

_PROFILE = ScaleProfile(
    name="test", num_stores=10, num_skus=40, history_years=1,
    sales_lines_target=2500, num_customers=200, num_vendors=20,
)
_CONFIG = TechmartConfig(
    scale_profile=_PROFILE, seed=42, output_dir=Path("data"),
    catalog="techmart", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_registry_contains_sales_line():
    assert FACT_SPECS["fact_sales_line"] is FACT_SALES_LINE_SPEC


def test_generate_sales_line_local_end_to_end(spark):
    dp = build_dim_product(spark, _CONFIG)
    dd = build_dim_date(spark, _CONFIG)
    dim_counts = {
        "store": _PROFILE.num_stores,
        "customer": _PROFILE.num_customers,
        "employee": _PROFILE.num_employees,
        "promotion": _PROFILE.num_promotions,
        "product": dp.count(),
    }
    df = generate_sales_line_local(spark, _CONFIG, dp, dd, dim_counts, rows=2500)
    assert df.columns == FACT_SALES_LINE_SPEC.column_names
    assert df.count() > 0
