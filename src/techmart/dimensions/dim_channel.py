from __future__ import annotations

import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec

DIM_CHANNEL_SPEC = TableSpec(
    schema="core",
    name="dim_channel",
    grain="one row per sales/interaction channel",
    columns=[
        Column("channel_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
        Column("channel_id", "Utf8", "Business key", nullable=False),
        Column("channel_name", "Utf8", "Channel name"),
        Column("channel_type", "Utf8", "Physical or Digital"),
    ],
)

_CHANNELS = [
    ("In-Store", "Physical"),
    ("Web", "Digital"),
    ("Mobile-App", "Digital"),
    ("Marketplace", "Digital"),
    ("Call-Center", "Physical"),
]


def build_dim_channel(config: TechmartConfig) -> pl.DataFrame:
    data = {
        "channel_sk": list(range(1, len(_CHANNELS) + 1)),
        "channel_id": [f"CH{i:02d}" for i in range(1, len(_CHANNELS) + 1)],
        "channel_name": [name for name, _ in _CHANNELS],
        "channel_type": [ctype for _, ctype in _CHANNELS],
    }
    df = pl.DataFrame(data)
    return df.cast(DIM_CHANNEL_SPEC.polars_schema()).select(DIM_CHANNEL_SPEC.column_names)
