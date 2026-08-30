from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from .config import TechmartConfig
from .dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
from .dimensions.dim_customer import DIM_CUSTOMER_SPEC, build_dim_customer
from .dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from .dimensions.dim_employee import DIM_EMPLOYEE_SPEC, build_dim_employee
from .dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product
from .dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion
from .dimensions.dim_store import DIM_STORE_SPEC, build_dim_store
from .dimensions.dim_vendor import DIM_VENDOR_SPEC, build_dim_vendor
from .framework.schema import TableSpec


@dataclass(frozen=True)
class TableBuilder:
    spec: TableSpec
    build: Callable[[TechmartConfig], pl.DataFrame]


def _build_dim_date(config: TechmartConfig) -> pl.DataFrame:
    return build_dim_date(config.start_date, config.end_date)


REGISTRY: dict[str, TableBuilder] = {
    DIM_CHANNEL_SPEC.name: TableBuilder(spec=DIM_CHANNEL_SPEC, build=build_dim_channel),
    DIM_CUSTOMER_SPEC.name: TableBuilder(spec=DIM_CUSTOMER_SPEC, build=build_dim_customer),
    DIM_DATE_SPEC.name: TableBuilder(spec=DIM_DATE_SPEC, build=_build_dim_date),
    DIM_EMPLOYEE_SPEC.name: TableBuilder(spec=DIM_EMPLOYEE_SPEC, build=build_dim_employee),
    DIM_PRODUCT_SPEC.name: TableBuilder(spec=DIM_PRODUCT_SPEC, build=build_dim_product),
    DIM_PROMOTION_SPEC.name: TableBuilder(spec=DIM_PROMOTION_SPEC, build=build_dim_promotion),
    DIM_STORE_SPEC.name: TableBuilder(spec=DIM_STORE_SPEC, build=build_dim_store),
    DIM_VENDOR_SPEC.name: TableBuilder(spec=DIM_VENDOR_SPEC, build=build_dim_vendor),
}
