"""Spark dim_employee builder using dbldatagen + SCD2."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ...reference.pools import FIRST_NAMES, LAST_NAMES
from ..dim_builder import build_scd2_dim, sql_array
from ..framework import SparkColumn, SparkTableSpec
from ..scd2 import scd2_columns

_ROLES = ["Cashier", "Sales-Associate", "Manager", "Buyer", "Planner"]

_BASE_COLUMNS: list[SparkColumn] = [
    SparkColumn("employee_sk", "long", "Surrogate key", is_key=True, nullable=False),
    SparkColumn("employee_id", "string", "Business key", nullable=False),
    SparkColumn("full_name", "string", "Employee full name"),
    SparkColumn("role", "string", "Cashier/Sales-Associate/Manager/Buyer/Planner"),
    SparkColumn("store_sk", "long", "Home store (FK to dim_store)"),
    SparkColumn("hire_date", "date", "Hire date"),
    SparkColumn("term_date", "date", "Termination date; null if active"),
    SparkColumn("manager_employee_sk", "long", "Manager (FK to dim_employee); null for Managers"),
    SparkColumn("status", "string", "Employment status"),
]

DIM_EMPLOYEE_SPEC = SparkTableSpec(
    schema="core",
    name="dim_employee",
    grain="one current row per associate (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)

# Pre-build SQL array literals for element_at expressions (1-based indexing).
_FIRST_NAMES_ARR = sql_array(FIRST_NAMES)
_LAST_NAMES_ARR = sql_array(LAST_NAMES)

# Hire date range: 2010-01-01 to 2024-01-01 (~5113 days).
_HIRE_DATE_BASE = "2010-01-01"
_HIRE_DATE_MAX_OFFSET = 5113


def build_dim_employee(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    """Generate dim_employee rows with dbldatagen; mark all rows as SCD2 current."""
    n = config.scale_profile.num_employees
    num_stores = config.scale_profile.num_stores

    def add_columns(gen):
        return (
            gen
            # --- surrogate / business keys ---
            .withColumn("employee_sk", "long", expr="id + 1", baseColumn="id")
            .withColumn(
                "employee_id", "string",
                expr="concat('EMP', lpad(cast(id + 1 as string), 6, '0'))",
                baseColumn="id",
            )
            # --- full name from name pools ---
            .withColumn("fi", "int", minValue=1, maxValue=len(FIRST_NAMES), random=True, omit=True)
            .withColumn("li", "int", minValue=1, maxValue=len(LAST_NAMES), random=True, omit=True)
            .withColumn(
                "full_name", "string",
                expr=f"concat(element_at({_FIRST_NAMES_ARR}, fi), ' ', element_at({_LAST_NAMES_ARR}, li))",
                baseColumn=["fi", "li"],
            )
            # --- role (must be generated before manager_employee_sk) ---
            .withColumn("role", "string", values=_ROLES, random=True)
            # --- store FK ---
            .withColumn("store_sk", "long", minValue=1, maxValue=num_stores, random=True)
            # --- hire date ---
            .withColumn("hire_off", "int", minValue=0, maxValue=_HIRE_DATE_MAX_OFFSET, random=True, omit=True)
            .withColumn(
                "hire_date", "date",
                expr=f"date_add(to_date('{_HIRE_DATE_BASE}'), hire_off)",
                baseColumn="hire_off",
            )
            # --- term date (null for all; active workforce snapshot) ---
            .withColumn("term_date", "date", expr="cast(null as date)")
            # --- manager FK: null for Managers, random employee SK for all others ---
            .withColumn(
                "manager_employee_sk", "long",
                expr=f"case when role = 'Manager' then null else pmod(abs(hash(id, 'mgr')), {n}) + 1 end",
                baseColumn=["id", "role"],
            )
            # --- employment status ---
            .withColumn("status", "string", expr="'Active'")
        )

    return build_scd2_dim(spark, config, DIM_EMPLOYEE_SPEC, n, add_columns)
