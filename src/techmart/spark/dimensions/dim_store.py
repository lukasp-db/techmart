"""Spark dim_store builder using dbldatagen + SCD2."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ...reference.pools import CITIES, US_STATES
from ..dim_builder import build_scd2_dim, sql_array
from ..framework import SparkColumn, SparkTableSpec
from ..scd2 import scd2_columns

_REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
_FORMATS = ["Flagship", "Standard", "Outlet", "Online-only"]

_BASE_COLUMNS: list[SparkColumn] = [
    SparkColumn("store_sk", "long", "Surrogate key", is_key=True, nullable=False),
    SparkColumn("store_id", "string", "Business key", nullable=False),
    SparkColumn("store_name", "string", "Store display name"),
    SparkColumn("store_format", "string", "Flagship/Standard/Outlet/Online-only"),
    SparkColumn("region_id", "string", "Region business key"),
    SparkColumn("region_name", "string", "Region name"),
    SparkColumn("district_id", "string", "District business key"),
    SparkColumn("district_name", "string", "District name"),
    SparkColumn("city", "string", "City"),
    SparkColumn("state", "string", "US state code"),
    SparkColumn("postal_code", "string", "Postal code"),
    SparkColumn("country", "string", "Country code"),
    SparkColumn("latitude", "double", "Latitude"),
    SparkColumn("longitude", "double", "Longitude"),
    SparkColumn("square_footage", "long", "Store square footage"),
    SparkColumn("open_date", "date", "Store opening date"),
    SparkColumn("status", "string", "Operating status"),
    SparkColumn("is_ship_from_store", "boolean", "Fulfills online orders"),
    SparkColumn("is_bopis_enabled", "boolean", "Supports buy-online-pickup-in-store"),
    SparkColumn("cost_center_id", "string", "Finance cost center identifier"),
]

DIM_STORE_SPEC = SparkTableSpec(
    schema="core",
    name="dim_store",
    grain="one current row per store (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)

# Pre-build SQL array literals for element_at expressions.
_REGIONS_ARR = sql_array(_REGIONS)


def build_dim_store(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    """Generate dim_store rows with dbldatagen; mark all rows as SCD2 current."""

    def add_columns(gen):
        return (
            gen
            # --- surrogate / business keys ---
            .withColumn("store_sk", "long", expr="id + 1", baseColumn="id")
            .withColumn(
                "store_id", "string",
                expr="concat('STORE', lpad(cast(id + 1 as string), 5, '0'))",
                baseColumn="id",
            )
            .withColumn(
                "store_name", "string",
                expr="concat('Techmart STORE', lpad(cast(id + 1 as string), 5, '0'))",
                baseColumn="id",
            )
            # --- store format ---
            .withColumn("store_format", "string", values=_FORMATS, random=True)
            # --- region (paired id + name from index) ---
            .withColumn("region_num", "int", minValue=1, maxValue=5, random=True, omit=True)
            .withColumn(
                "region_name", "string",
                expr=f"element_at({_REGIONS_ARR}, region_num)",
                baseColumn="region_num",
            )
            .withColumn(
                "region_id", "string",
                expr="concat('RGN', cast(region_num as string))",
                baseColumn="region_num",
            )
            # --- district (1-20) ---
            .withColumn("district_num", "int", minValue=1, maxValue=20, random=True, omit=True)
            .withColumn(
                "district_id", "string",
                expr="concat('DST', lpad(cast(district_num as string), 2, '0'))",
                baseColumn="district_num",
            )
            .withColumn(
                "district_name", "string",
                expr="concat('District ', lpad(cast(district_num as string), 2, '0'))",
                baseColumn="district_num",
            )
            # --- geo pools ---
            .withColumn("city", "string", values=CITIES, random=True)
            .withColumn("state", "string", values=US_STATES, random=True)
            # --- postal code as 5-digit string ---
            .withColumn("postal_int", "int", minValue=10000, maxValue=99999, random=True, omit=True)
            .withColumn(
                "postal_code", "string",
                expr="cast(postal_int as string)",
                baseColumn="postal_int",
            )
            .withColumn("country", "string", expr="'US'")
            # --- coordinates ---
            .withColumn("latitude", "double", minValue=25.0, maxValue=49.0, random=True)
            .withColumn("longitude", "double", minValue=-124.0, maxValue=-67.0, random=True)
            # --- square footage ---
            .withColumn("square_footage", "long", minValue=15000, maxValue=45000, random=True)
            # --- open date: 2005-01-01 + 0..5113 days (up to ~2019-01-01) ---
            .withColumn("open_off", "int", minValue=0, maxValue=5113, random=True, omit=True)
            .withColumn(
                "open_date", "date",
                expr="date_add(to_date('2005-01-01'), open_off)",
                baseColumn="open_off",
            )
            # --- operational flags ---
            .withColumn("status", "string", expr="'Active'")
            .withColumn(
                "is_ship_from_store", "boolean",
                expr="pmod(abs(hash(id, 'sfs')), 2) = 0",
                baseColumn="id",
            )
            .withColumn(
                "is_bopis_enabled", "boolean",
                expr="pmod(abs(hash(id, 'bopis')), 2) = 0",
                baseColumn="id",
            )
            .withColumn(
                "cost_center_id", "string",
                expr="concat('CC', lpad(cast(id + 1 as string), 5, '0'))",
                baseColumn="id",
            )
        )

    return build_scd2_dim(spark, config, DIM_STORE_SPEC, config.scale_profile.num_stores, add_columns)
