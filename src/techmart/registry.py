from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from .config import TechmartConfig
from .dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
from .dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from .dimensions.dim_store import DIM_STORE_SPEC, build_dim_store
from .framework.schema import TableSpec


@dataclass(frozen=True)
class TableBuilder:
    spec: TableSpec
    build: Callable[[TechmartConfig], pl.DataFrame]


def _build_dim_date(config: TechmartConfig) -> pl.DataFrame:
    return build_dim_date(config.start_date, config.end_date)


REGISTRY: dict[str, TableBuilder] = {
    DIM_CHANNEL_SPEC.name: TableBuilder(spec=DIM_CHANNEL_SPEC, build=build_dim_channel),
    DIM_DATE_SPEC.name: TableBuilder(spec=DIM_DATE_SPEC, build=_build_dim_date),
    DIM_STORE_SPEC.name: TableBuilder(spec=DIM_STORE_SPEC, build=build_dim_store),
}
