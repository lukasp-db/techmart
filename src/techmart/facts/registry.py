from __future__ import annotations

from ..spark.framework import FactSpec
from .fact_sales_line import FACT_SALES_LINE_SPEC

FACT_SPECS: dict[str, FactSpec] = {
    FACT_SALES_LINE_SPEC.name: FACT_SALES_LINE_SPEC,
}
