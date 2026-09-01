"""Spark dim_promotion builder using dbldatagen + SCD2."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ..dim_builder import build_scd2_dim
from ..framework import SparkColumn, SparkTableSpec
from ..scd2 import scd2_columns

_PROMO_TYPES = ["Markdown", "BOGO", "Bundle", "Coupon", "Vendor-Funded"]
_DISCOUNT_METHODS = ["PercentOff", "AmountOff", "BuyOneGetOne"]
_CHANNEL_SCOPES = ["All", "In-Store", "Online"]
_FUNDING = ["Retailer", "Vendor"]

_BASE_COLUMNS: list[SparkColumn] = [
    SparkColumn("promotion_sk", "long", "Surrogate key", is_key=True, nullable=False),
    SparkColumn("promotion_id", "string", "Business key", nullable=False),
    SparkColumn("promo_name", "string", "Promotion name"),
    SparkColumn("promo_type", "string", "Markdown/BOGO/Bundle/Coupon/Vendor-Funded"),
    SparkColumn("discount_method", "string", "Discount mechanism"),
    SparkColumn("discount_value", "double", "Discount value (percent or amount)"),
    SparkColumn("start_date", "date", "Promotion start date"),
    SparkColumn("end_date", "date", "Promotion end date"),
    SparkColumn("channel_scope", "string", "Channels the promotion applies to"),
    SparkColumn("funding_source", "string", "Retailer or Vendor funded"),
    SparkColumn("campaign_id", "string", "Parent campaign business key"),
    SparkColumn("campaign_name", "string", "Parent campaign name"),
]

DIM_PROMOTION_SPEC = SparkTableSpec(
    schema="core",
    name="dim_promotion",
    grain="one current row per promotion/offer (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_promotion(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    """Generate dim_promotion rows with dbldatagen; mark all rows as SCD2 current."""
    n = config.scale_profile.num_promotions
    span = (config.end_date - config.start_date).days
    start_iso = config.start_date.isoformat()
    end_iso = config.end_date.isoformat()

    # start_offset in [0, span-30): maxValue = span - 31 (dbldatagen inclusive)
    start_offset_max = max(span - 31, 0)
    # Number of distinct campaigns: n // 4 clamped to at least 2
    num_campaigns = max(n // 4, 2)

    def add_columns(gen):
        return (
            gen
            # --- surrogate / business keys ---
            .withColumn("promotion_sk", "long", expr="id + 1", baseColumn="id")
            .withColumn(
                "promotion_id", "string",
                expr="concat('PROMO', lpad(cast(id + 1 as string), 5, '0'))",
                baseColumn="id",
            )
            .withColumn(
                "promo_name", "string",
                expr="concat('Promo PROMO', lpad(cast(id + 1 as string), 5, '0'))",
                baseColumn="id",
            )
            # --- classification ---
            .withColumn("promo_type", "string", values=_PROMO_TYPES, random=True)
            .withColumn("discount_method", "string", values=_DISCOUNT_METHODS, random=True)
            .withColumn("discount_value_raw", "double", minValue=5.0, maxValue=50.0, random=True, omit=True)
            .withColumn("discount_value", "double", expr="round(discount_value_raw, 2)", baseColumn="discount_value_raw")
            # --- date window ---
            .withColumn("start_offset", "int", minValue=0, maxValue=start_offset_max, random=True, omit=True)
            .withColumn("duration", "int", minValue=3, maxValue=29, random=True, omit=True)
            .withColumn(
                "start_date", "date",
                expr=f"date_add(to_date('{start_iso}'), start_offset)",
                baseColumn="start_offset",
            )
            .withColumn(
                "end_date", "date",
                expr=f"least(date_add(start_date, duration), to_date('{end_iso}'))",
                baseColumn=["start_date", "duration"],
            )
            # --- scope / funding ---
            .withColumn("channel_scope", "string", values=_CHANNEL_SCOPES, random=True)
            .withColumn("funding_source", "string", values=_FUNDING, random=True)
            # --- campaign (grouped promotions) ---
            .withColumn("campaign_num", "int", minValue=1, maxValue=num_campaigns, random=True, omit=True)
            .withColumn(
                "campaign_id", "string",
                expr="concat('CAMP', lpad(cast(campaign_num as string), 4, '0'))",
                baseColumn="campaign_num",
            )
            .withColumn(
                "campaign_name", "string",
                expr="concat('Campaign ', lpad(cast(campaign_num as string), 4, '0'))",
                baseColumn="campaign_num",
            )
        )

    return build_scd2_dim(spark, config, DIM_PROMOTION_SPEC, n, add_columns)
