"""Spark dim_product builder using dbldatagen + SCD2.

Uses a paths-with-brand lookup DataFrame (built once from taxonomy) joined to
a dbldatagen base to guarantee all six hierarchy levels are internally
consistent per product.
"""
from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ...config import TechmartConfig
from ...dimensions.product_support import COLORS
from ...reference.taxonomy import subcategory_paths
from ..framework import SparkColumn, SparkTableSpec
from ..scd2 import scd2_columns, with_scd2_current

_UOMS = ["EA", "EA", "EA", "PK", "BX"]
_LIFECYCLE = ["Active", "Active", "Active", "Active", "Clearance", "Discontinued"]

# Days from 2015-01-01 to 2024-06-01 (inclusive of leap years 2016, 2020, 2024).
_LAUNCH_OFFSET_MAX = 3440

_BASE_COLUMNS: list[SparkColumn] = [
    SparkColumn("product_sk", "long", "Surrogate key", is_key=True, nullable=False),
    SparkColumn("sku", "string", "Business key (stock-keeping unit)", nullable=False),
    SparkColumn("gtin", "string", "Global trade item number (barcode)"),
    SparkColumn("model_number", "string", "Manufacturer model number"),
    SparkColumn("product_name", "string", "Product display name"),
    SparkColumn("product_description", "string", "Rich product description (for GenAI/search)"),
    SparkColumn("manufacturer", "string", "Manufacturer name"),
    SparkColumn("brand_id", "string", "Brand business key (slug of brand name)"),
    SparkColumn("brand_name", "string", "Brand name (hierarchy level 5)"),
    SparkColumn("division_id", "string", "Division business key (hierarchy level 1)"),
    SparkColumn("division_name", "string", "Division name"),
    SparkColumn("department_id", "string", "Department business key (level 2)"),
    SparkColumn("department_name", "string", "Department name"),
    SparkColumn("category_id", "string", "Category business key (level 3)"),
    SparkColumn("category_name", "string", "Category name"),
    SparkColumn("subcategory_id", "string", "Subcategory business key (level 4)"),
    SparkColumn("subcategory_name", "string", "Subcategory name"),
    SparkColumn("primary_vendor_sk", "long", "Primary vendor (FK to dim_vendor)"),
    SparkColumn("private_label_flag", "boolean", "Techmart private-label product"),
    SparkColumn("is_marketplace", "boolean", "Sold by a 3rd-party marketplace seller"),
    SparkColumn("marketplace_seller_id", "string", "Marketplace seller id; null if first-party"),
    SparkColumn("uom", "string", "Unit of measure"),
    SparkColumn("color", "string", "Primary color"),
    SparkColumn("spec_attributes", "string", "JSON of product specification attributes"),
    SparkColumn("weight_kg", "double", "Weight in kilograms"),
    SparkColumn("dimensions", "string", "Package dimensions LxWxH (cm)"),
    SparkColumn("msrp", "double", "Manufacturer suggested retail price"),
    SparkColumn("list_price", "double", "Current list price"),
    SparkColumn("standard_cost", "double", "Standard unit cost"),
    SparkColumn("lifecycle_status", "string", "Active/Clearance/Discontinued"),
    SparkColumn("launch_date", "date", "Product launch date"),
    SparkColumn("discontinue_date", "date", "Discontinuation date; null unless discontinued"),
]

DIM_PRODUCT_SPEC = SparkTableSpec(
    schema="core",
    name="dim_product",
    grain="one current row per SKU (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)

# Schema for the paths-with-brand lookup DataFrame.
_LOOKUP_SCHEMA = StructType([
    StructField("path_brand_idx", IntegerType(), False),
    StructField("division_id", StringType(), True),
    StructField("division_name", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("department_name", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("subcategory_id", StringType(), True),
    StructField("subcategory_name", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("brand_id", StringType(), True),
])


def _build_lookup_rows() -> list[tuple]:
    """Expand subcategory_paths() × each category's brands into flat rows."""
    rows: list[tuple] = []
    idx = 0
    for div, dep, cat, sub in subcategory_paths():
        for brand_name in cat.brands:
            brand_id = brand_name.upper().replace(" ", "")
            rows.append((
                idx,
                div.id, div.name,
                dep.id, dep.name,
                cat.id, cat.name,
                sub.id, sub.name,
                brand_name, brand_id,
            ))
            idx += 1
    return rows


def build_dim_product(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    """Generate dim_product rows with dbldatagen; join taxonomy lookup; mark SCD2 current."""
    n = config.scale_profile.num_skus
    num_vendors = config.scale_profile.num_vendors
    end_iso = config.end_date.isoformat()

    # --- paths-with-brand lookup (one row per (subcategory_path, brand) combination) ---
    lookup_rows = _build_lookup_rows()
    num_path_brands = len(lookup_rows)
    lookup_df = F.broadcast(spark.createDataFrame(lookup_rows, _LOOKUP_SCHEMA))

    # --- base product DataFrame via dbldatagen ---
    base_df = (
        dg.DataGenerator(
            spark,
            name="dim_product",
            rows=n,
            partitions=max(1, min(64, n // 100_000)),
            randomSeed=config.seed,
            randomSeedMethod="fixed",
        )
        .withIdOutput()
        # --- surrogate / business keys ---
        .withColumn("product_sk", "long", expr="id + 1", baseColumn="id")
        .withColumn(
            "sku", "string",
            expr="concat('SKU', lpad(cast(id + 1 as string), 8, '0'))",
            baseColumn="id",
        )
        .withColumn(
            "gtin_raw", "long",
            minValue=100_000_000_000, maxValue=999_999_999_999,
            random=True, omit=True,
        )
        .withColumn("gtin", "string", expr="cast(gtin_raw as string)", baseColumn="gtin_raw")
        .withColumn(
            "model_number", "string",
            expr="concat('MDL', lpad(cast(id + 1 as string), 8, '0'))",
            baseColumn="id",
        )
        # --- taxonomy join key (0-based index into paths-with-brand lookup) ---
        .withColumn(
            "path_brand_idx", "int",
            minValue=0, maxValue=num_path_brands - 1,
            random=True,
        )
        # --- vendor FK ---
        .withColumn("primary_vendor_sk", "long", minValue=1, maxValue=num_vendors, random=True)
        # --- physical attributes ---
        .withColumn("color", "string", values=COLORS, random=True)
        .withColumn("weight_raw", "double", minValue=0.1, maxValue=20.0, random=True, omit=True)
        .withColumn("weight_kg", "double", expr="round(weight_raw, 2)", baseColumn="weight_raw")
        .withColumn("dim_len", "int", minValue=5, maxValue=60, random=True, omit=True)
        .withColumn("dim_wid", "int", minValue=5, maxValue=40, random=True, omit=True)
        .withColumn("dim_hgt", "int", minValue=1, maxValue=30, random=True, omit=True)
        .withColumn(
            "dimensions", "string",
            expr="concat(dim_len, 'x', dim_wid, 'x', dim_hgt)",
            baseColumn=["dim_len", "dim_wid", "dim_hgt"],
        )
        # --- pricing: msrp first, then list_price and standard_cost derived from it ---
        .withColumn("msrp_raw", "double", minValue=9.99, maxValue=2999.99, random=True, omit=True)
        .withColumn("msrp", "double", expr="round(msrp_raw, 2)", baseColumn="msrp_raw")
        .withColumn("disc_pct", "double", minValue=0.0, maxValue=0.15, random=True, omit=True)
        .withColumn(
            "list_price", "double",
            expr="round(msrp * (1.0 - disc_pct), 2)",
            baseColumn=["msrp", "disc_pct"],
        )
        .withColumn("cost_pct", "double", minValue=0.5, maxValue=0.8, random=True, omit=True)
        .withColumn(
            "standard_cost", "double",
            expr="round(msrp * cost_pct, 2)",
            baseColumn=["msrp", "cost_pct"],
        )
        # --- unit of measure ---
        .withColumn("uom", "string", values=_UOMS, random=True)
        # --- marketplace flags ---
        .withColumn(
            "is_marketplace", "boolean",
            expr="pmod(abs(hash(id,'mkt')),100) < 15",
            baseColumn="id",
        )
        .withColumn(
            "marketplace_seller_id", "string",
            expr=(
                "case when is_marketplace"
                " then concat('SELLER', lpad(cast(pmod(abs(hash(id,'sel')),200)+1 as string),4,'0'))"
                " else cast(null as string) end"
            ),
            baseColumn=["id", "is_marketplace"],
        )
        # --- private label ---
        .withColumn(
            "private_label_flag", "boolean",
            expr="pmod(abs(hash(id,'pl')),100) < 10",
            baseColumn="id",
        )
        # --- lifecycle ---
        .withColumn("lifecycle_status", "string", values=_LIFECYCLE, random=True)
        .withColumn("launch_off", "int", minValue=0, maxValue=_LAUNCH_OFFSET_MAX, random=True, omit=True)
        .withColumn(
            "launch_date", "date",
            expr="date_add(to_date('2015-01-01'), launch_off)",
            baseColumn="launch_off",
        )
        .withColumn("disc_off", "int", minValue=30, maxValue=1000, random=True, omit=True)
        .withColumn(
            "discontinue_date", "date",
            expr=(
                "case when lifecycle_status = 'Discontinued'"
                f" then least(date_add(launch_date, disc_off), to_date('{end_iso}'))"
                " else cast(null as date) end"
            ),
            baseColumn=["lifecycle_status", "launch_date", "disc_off"],
        )
        .build()
        .drop("id")
    )

    # --- join base to lookup on path_brand_idx to populate hierarchy + brand columns ---
    df = base_df.join(lookup_df, on="path_brand_idx", how="left")

    # --- derive columns that depend on joined brand/subcategory data ---
    df = (
        df
        .withColumn("manufacturer", F.col("brand_name"))
        .withColumn(
            "product_name",
            F.expr("concat(brand_name, ' ', subcategory_name, ' ', model_number)"),
        )
        .withColumn(
            "product_description",
            F.expr(
                "concat(brand_name, ' ', subcategory_name,"
                " ' (', color, '), model ', model_number, '.')"
            ),
        )
        .withColumn(
            "spec_attributes",
            F.expr(
                "to_json(named_struct('color', color, 'weight_kg', weight_kg, 'brand', brand_name))"
            ),
        )
    )

    df = with_scd2_current(df, config.start_date)
    return DIM_PRODUCT_SPEC.select_ordered(df)
