from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.finance.dim_department import DIM_DEPARTMENT_SPEC, build_dim_department

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 40, 1, 3000, 200, 20), seed=42,
    output_dir=Path("data"), catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)
_EXPECTED = {
    "Merchandising", "Store Operations", "Supply Chain", "Marketing",
    "E-commerce", "Finance & Admin", "G&A",
}


def test_schema_and_rows(spark):
    df = build_dim_department(spark, _CFG)
    assert df.columns == DIM_DEPARTMENT_SPEC.column_names
    assert DIM_DEPARTMENT_SPEC.schema == "finance"
    names = {r["department_name"] for r in df.collect()}
    assert names == _EXPECTED


def test_unique_sk_and_groups(spark):
    df = build_dim_department(spark, _CFG)
    rows = df.collect()
    assert len({r["department_sk"] for r in rows}) == len(rows)
    assert all(r["department_group"] in {"COGS-bearing", "Opex"} for r in rows)
