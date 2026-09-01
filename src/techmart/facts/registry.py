from __future__ import annotations

from ..spark.framework import SparkTableSpec
from .fact_fulfillment import FACT_FULFILLMENT_SPEC
from .fact_inventory_movement import FACT_INVENTORY_MOVEMENT_SPEC
from .fact_inventory_snapshot import FACT_INVENTORY_SNAPSHOT_SPEC
from .fact_loyalty_activity import FACT_LOYALTY_ACTIVITY_SPEC
from .fact_returns import FACT_RETURNS_SPEC
from .fact_sales_line import FACT_SALES_LINE_SPEC
from .fact_web_events import FACT_WEB_EVENTS_SPEC

FACT_SPECS: dict[str, SparkTableSpec] = {
    s.name: s
    for s in (
        FACT_SALES_LINE_SPEC,
        FACT_INVENTORY_SNAPSHOT_SPEC,
        FACT_INVENTORY_MOVEMENT_SPEC,
        FACT_RETURNS_SPEC,
        FACT_FULFILLMENT_SPEC,
        FACT_LOYALTY_ACTIVITY_SPEC,
        FACT_WEB_EVENTS_SPEC,
    )
}
