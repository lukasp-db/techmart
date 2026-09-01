from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_product import build_dim_product

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 300, 1, 50000, 500, 20),  # num_skus=300, num_vendors=20
    seed=42, output_dir=Path("data"), catalog="c", schema_prefix="techmart_",
    end_date=date(2026, 1, 31),
)


def test_color_and_vendor_are_independent(spark):
    df = build_dim_product(spark, _CFG)
    combos = df.select("color", "primary_vendor_sk").distinct().count()
    # correlated ("fixed") build functionally ties color to vendor (~30 combos);
    # independent streams populate the 10x20 grid (>150 combos with 300 rows).
    assert combos > 60, f"color/vendor combos collapsed to {combos}"
