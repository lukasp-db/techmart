from datetime import date

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_channel import DIM_CHANNEL_SPEC, build_dim_channel
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 500, 1, 50000, 1000, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_channel(spark):
    df = build_dim_channel(spark, _CFG)
    validate_spark_schema(df, DIM_CHANNEL_SPEC)
    rows = {r["channel_sk"]: r for r in df.collect()}
    assert df.count() == 5
    assert rows[1]["channel_name"] == "In-Store" and rows[1]["channel_type"] == "Physical"
    assert rows[4]["channel_name"] == "Marketplace" and rows[4]["channel_type"] == "Digital"
