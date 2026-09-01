"""techmart_ops table registry."""
from __future__ import annotations

from .forecast_override import FORECAST_OVERRIDE_SPEC
from .replenishment_order import REPLENISHMENT_ORDER_SPEC

OPS_SPECS = [REPLENISHMENT_ORDER_SPEC, FORECAST_OVERRIDE_SPEC]
