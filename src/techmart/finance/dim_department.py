"""dim_department: functional cost-center department (techmart_finance)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec

DIM_DEPARTMENT_SPEC = SparkTableSpec(
    schema="finance",
    name="dim_department",
    grain="one row per functional cost-center department",
    columns=[
        SparkColumn("department_sk", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("department_name", "string", "Functional department name", nullable=False),
        SparkColumn("department_group", "string", "COGS-bearing or Opex grouping"),
    ],
)

_DEPARTMENTS = [
    ("Merchandising", "COGS-bearing"),
    ("E-commerce", "COGS-bearing"),
    ("Supply Chain", "COGS-bearing"),
    ("Store Operations", "Opex"),
    ("Marketing", "Opex"),
    ("G&A", "Opex"),
    ("Finance & Admin", "Opex"),
]


def build_dim_department(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    rows = [(i, name, group) for i, (name, group) in enumerate(_DEPARTMENTS, start=1)]
    return spark.createDataFrame(rows, schema=DIM_DEPARTMENT_SPEC.struct_type())
