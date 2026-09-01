from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ..framework import SparkColumn, SparkTableSpec

DIM_CHANNEL_SPEC = SparkTableSpec(
    schema="core",
    name="dim_channel",
    grain="one row per sales/interaction channel",
    columns=[
        SparkColumn("channel_sk", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("channel_id", "string", "Business key", nullable=False),
        SparkColumn("channel_name", "string", "Channel name"),
        SparkColumn("channel_type", "string", "Physical or Digital"),
    ],
)

_CHANNELS = [
    ("In-Store", "Physical"), ("Web", "Digital"), ("Mobile-App", "Digital"),
    ("Marketplace", "Digital"), ("Call-Center", "Physical"),
]


def build_dim_channel(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    rows = [
        (i, f"CH{i:02d}", name, ctype)
        for i, (name, ctype) in enumerate(_CHANNELS, start=1)
    ]
    return spark.createDataFrame(rows, schema=DIM_CHANNEL_SPEC.struct_type())
