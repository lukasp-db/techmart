"""Registry of techmart_finance table specs."""
from __future__ import annotations

from ..spark.framework import SparkTableSpec
from .dim_department import DIM_DEPARTMENT_SPEC
from .dim_gl_account import DIM_GL_ACCOUNT_SPEC
from .fact_budget_plan import FACT_BUDGET_PLAN_SPEC
from .fact_gl_actuals import FACT_GL_ACTUALS_SPEC
from .fact_inventory_valuation import FACT_INVENTORY_VALUATION_SPEC

FINANCE_SPECS: list[SparkTableSpec] = [
    DIM_DEPARTMENT_SPEC,
    DIM_GL_ACCOUNT_SPEC,
    FACT_GL_ACTUALS_SPEC,
    FACT_BUDGET_PLAN_SPEC,
    FACT_INVENTORY_VALUATION_SPEC,
]
