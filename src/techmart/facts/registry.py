from __future__ import annotations

from ..spark.framework import SparkTableSpec
from .fact_sales_line import FACT_SALES_LINE_SPEC

FACT_SPECS: dict[str, SparkTableSpec] = {
    FACT_SALES_LINE_SPEC.name: FACT_SALES_LINE_SPEC,
}
