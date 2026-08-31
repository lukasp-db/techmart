from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_date import build_dim_date
from techmart.spark.dimensions.dim_product import build_dim_product
from techmart.facts.lookups import (
    date_seasonality_weights,
    product_economics,
)

_PROFILE = ScaleProfile(
    name="test",
    num_stores=10,
    num_skus=40,
    history_years=1,
    sales_lines_target=2000,
    num_customers=200,
    num_vendors=20,
)
_CONFIG = TechmartConfig(
    scale_profile=_PROFILE,
    seed=42,
    output_dir=__import__("pathlib").Path("data"),
    catalog="techmart",
    schema_prefix="techmart_",
    end_date=date(2026, 1, 31),
)


def test_product_economics_one_row_per_sku(spark):
    dim = build_dim_product(spark, _CONFIG)
    econ = product_economics(dim)
    assert econ.count() == _PROFILE.num_skus
    assert set(econ.columns) == {"product_sk", "list_price", "standard_cost", "msrp"}

    row = econ.agg(
        F.min("product_sk").alias("lo"),
        F.max("product_sk").alias("hi"),
        F.min("list_price").alias("minprice"),
    ).collect()[0]
    assert row["lo"] == 1 and row["hi"] == _PROFILE.num_skus
    assert row["minprice"] > 0


def test_date_weights_cover_calendar_and_are_positive(spark):
    dim = build_dim_date(spark, _CONFIG)
    date_sks, weights = date_seasonality_weights(dim)
    total_days = dim.count()
    assert len(date_sks) == total_days
    assert len(weights) == total_days
    assert min(weights) >= 1
    assert date_sks == sorted(date_sks)
    # Holiday-season days should on average outweigh a flat baseline of 100.
    assert max(weights) > 100
