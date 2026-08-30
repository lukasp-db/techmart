from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_VENDOR_TYPES = ["Manufacturer", "Distributor", "Marketplace-Seller"]
_CATEGORIES = ["Computing", "Consumer Electronics", "Appliances", "Networking & DIY", "Services"]
_PAYMENT_TERMS = ["NET30", "NET45", "NET60", "NET90"]
_VENDOR_NAME_STEMS = [
    "Apex", "Summit", "Vertex", "Pioneer", "Horizon", "Nimbus", "Quantum",
    "Beacon", "Cascade", "Meridian", "Atlas", "Catalyst", "Orbit", "Vanguard",
]
_VENDOR_NAME_TAILS = ["Electronics", "Supply", "Distribution", "Trading", "Technologies", "Brands"]


def build_dim_vendor(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_vendors
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    stems = support.sample(rng.stream("dim_vendor.stem"), _VENDOR_NAME_STEMS, n)
    tails = support.sample(rng.stream("dim_vendor.tail"), _VENDOR_NAME_TAILS, n)
    names = np.char.add(np.char.add(stems.astype(str), " "), tails.astype(str))
    data = {
        "vendor_sk": sk,
        "vendor_id": support.business_keys("VEND", n, 5),
        "vendor_name": names,
        "vendor_type": support.sample(rng.stream("dim_vendor.type"), _VENDOR_TYPES, n),
        "primary_category": support.sample(rng.stream("dim_vendor.cat"), _CATEGORIES, n),
        "country": np.full(n, "US", dtype=object),
        "relationship_start_date": support.random_dates(
            rng.stream("dim_vendor.rel"), date(2000, 1, 1), date(2020, 1, 1), n
        ),
        "preferred_flag": rng.stream("dim_vendor.pref").integers(0, 2, n).astype(bool),
        "vendor_scorecard_rating": rng.stream("dim_vendor.score").integers(1, 6, n),
        "avg_lead_time_days": rng.stream("dim_vendor.lead").integers(3, 45, n),
        "payment_terms": support.sample(rng.stream("dim_vendor.terms"), _PAYMENT_TERMS, n),
        "active_flag": np.full(n, True, dtype=bool),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_VENDOR_SPEC.polars_schema()).select(DIM_VENDOR_SPEC.column_names)


_BASE_COLUMNS = [
    Column("vendor_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("vendor_id", "Utf8", "Business key", nullable=False),
    Column("vendor_name", "Utf8", "Vendor name"),
    Column("vendor_type", "Utf8", "Manufacturer/Distributor/Marketplace-Seller"),
    Column("primary_category", "Utf8", "Primary product category supplied"),
    Column("country", "Utf8", "Country code"),
    Column("relationship_start_date", "Date", "Date the vendor relationship began"),
    Column("preferred_flag", "Boolean", "Preferred vendor"),
    Column("vendor_scorecard_rating", "Int64", "Scorecard rating 1-5"),
    Column("avg_lead_time_days", "Int64", "Average lead time in days"),
    Column("payment_terms", "Utf8", "Payment terms"),
    Column("active_flag", "Boolean", "Active vendor"),
]

DIM_VENDOR_SPEC = TableSpec(
    schema="core",
    name="dim_vendor",
    grain="one current row per vendor (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)
