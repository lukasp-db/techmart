from datetime import date

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.spark.dimensions.dim_product import DIM_PRODUCT_SPEC, build_dim_product
from techmart.spark.framework import validate_spark_schema

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 300, 1, 50000, 500, 20),
    seed=42, output_dir=__import__("pathlib").Path("data"),
    catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_dim_product(spark):
    df = build_dim_product(spark, _CFG)
    validate_spark_schema(df, DIM_PRODUCT_SPEC)
    assert df.count() == 300
    r = df.agg(F.min("product_sk").alias("lo"), F.max("product_sk").alias("hi"),
               F.countDistinct("product_sk").alias("d")).first()
    assert r["lo"] == 1 and r["hi"] == 300 and r["d"] == 300
    # Hierarchy always populated; primary_vendor_sk in range; JSON specs parse.
    # All six hierarchy levels populated (the join guarantees they come from one path row).
    assert df.filter(
        F.col("division_name").isNull() | F.col("department_name").isNull()
        | F.col("category_name").isNull() | F.col("subcategory_name").isNull()
        | F.col("brand_name").isNull()
    ).count() == 0
    assert df.filter((F.col("primary_vendor_sk") < 1) | (F.col("primary_vendor_sk") > 20)).count() == 0
    assert df.filter(F.get_json_object(F.col("spec_attributes"), "$.brand").isNull()).count() == 0
    # discontinue_date only when discontinued, and within the window.
    assert df.filter((F.col("lifecycle_status") != "Discontinued") & F.col("discontinue_date").isNotNull()).count() == 0
    assert df.filter(F.col("discontinue_date") > F.lit(_CFG.end_date)).count() == 0


def test_prices_within_category_bands_and_realistic(spark):
    from techmart.reference.pricing import price_bands_by_id
    df = build_dim_product(spark, _CFG)
    bands = price_bands_by_id()
    # bring bands into the frame to check per-row containment
    bands_rows = [(cid, lo, hi) for cid, (lo, hi) in bands.items()]
    bdf = spark.createDataFrame(bands_rows, "category_id string, lo double, hi double")
    j = df.join(bdf, "category_id", "left")
    assert j.filter(F.col("lo").isNull()).count() == 0  # every category has a band
    out = j.filter((F.col("msrp") < F.col("lo") - 0.01) | (F.col("msrp") > F.col("hi") + 0.01)).count()
    assert out == 0, "msrp outside its category band"
    # catalog mean is far below the old uniform ~1505/unit
    mean_lp = df.agg(F.avg("list_price")).first()[0]
    assert mean_lp < 900.0, f"catalog mean list_price still high: {mean_lp:.0f}"
    # list_price/standard_cost derive from msrp
    bad = df.filter(
        (F.col("list_price") > F.col("msrp") + 0.01)
        | (F.col("standard_cost") > F.col("msrp") + 0.01)
        | (F.col("standard_cost") <= 0) | (F.col("list_price") <= 0)
    ).count()
    assert bad == 0


def test_dim_product_pricing_deterministic(spark):
    a = build_dim_product(spark, _CFG).agg(F.round(F.sum("msrp"), 2)).first()[0]
    b = build_dim_product(spark, _CFG).agg(F.round(F.sum("msrp"), 2)).first()[0]
    assert a == b
