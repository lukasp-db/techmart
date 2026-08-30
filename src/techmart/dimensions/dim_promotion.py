from __future__ import annotations

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_PROMO_TYPES = ["Markdown", "BOGO", "Bundle", "Coupon", "Vendor-Funded"]
_DISCOUNT_METHODS = ["PercentOff", "AmountOff", "BuyOneGetOne"]
_CHANNEL_SCOPES = ["All", "In-Store", "Online"]
_FUNDING = ["Retailer", "Vendor"]

_BASE_COLUMNS = [
    Column("promotion_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("promotion_id", "Utf8", "Business key", nullable=False),
    Column("promo_name", "Utf8", "Promotion name"),
    Column("promo_type", "Utf8", "Markdown/BOGO/Bundle/Coupon/Vendor-Funded"),
    Column("discount_method", "Utf8", "Discount mechanism"),
    Column("discount_value", "Float64", "Discount value (percent or amount)"),
    Column("start_date", "Date", "Promotion start date"),
    Column("end_date", "Date", "Promotion end date"),
    Column("channel_scope", "Utf8", "Channels the promotion applies to"),
    Column("funding_source", "Utf8", "Retailer or Vendor funded"),
    Column("campaign_id", "Utf8", "Parent campaign business key"),
    Column("campaign_name", "Utf8", "Parent campaign name"),
]

DIM_PROMOTION_SPEC = TableSpec(
    schema="core",
    name="dim_promotion",
    grain="one current row per promotion/offer (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_promotion(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_promotions
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    span = (config.end_date - config.start_date).days
    durations = rng.stream("dim_promotion.dur").integers(3, 30, n)  # days
    start_offsets = rng.stream("dim_promotion.start").integers(0, max(span - 30, 1), n)  # offset in days; reserve 30d for max duration
    start = np.datetime64(config.start_date) + start_offsets.astype("timedelta64[D]")
    end = start + durations.astype("timedelta64[D]")
    # Clamp end_date to not exceed config.end_date
    end = np.minimum(end, np.datetime64(config.end_date))
    campaign_idx = rng.stream("dim_promotion.camp").integers(1, max(n // 4, 2), n)
    # Extract business_keys once to avoid duplication
    promotion_id = support.business_keys("PROMO", n, 5)
    data = {
        "promotion_sk": sk,
        "promotion_id": promotion_id,
        "promo_name": np.char.add("Promo ", promotion_id),
        "promo_type": support.sample(rng.stream("dim_promotion.type"), _PROMO_TYPES, n),
        "discount_method": support.sample(rng.stream("dim_promotion.method"), _DISCOUNT_METHODS, n),
        "discount_value": np.round(rng.stream("dim_promotion.val").uniform(5.0, 50.0, n), 2),
        "start_date": start,
        "end_date": end,
        "channel_scope": support.sample(rng.stream("dim_promotion.scope"), _CHANNEL_SCOPES, n),
        "funding_source": support.sample(rng.stream("dim_promotion.fund"), _FUNDING, n),
        "campaign_id": np.char.add("CAMP", np.char.zfill(campaign_idx.astype(str), 4)),
        "campaign_name": np.char.add("Campaign ", np.char.zfill(campaign_idx.astype(str), 4)),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_PROMOTION_SPEC.polars_schema()).select(DIM_PROMOTION_SPEC.column_names)
