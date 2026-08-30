from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_TYPES = ["Retail", "Commercial-B2B"]
_TIERS = ["None", "Bronze", "Silver", "Gold", "Platinum"]
_SEGMENTS = ["DIY-Pro", "Gamer", "Home-Office", "SMB", "Household", "Student"]
_ACQUISITION = ["In-Store", "Web", "Mobile-App", "Marketplace", "Referral"]

_BASE_COLUMNS = [
    Column("customer_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("customer_id", "Utf8", "Business key", nullable=False),
    Column("customer_type", "Utf8", "Retail or Commercial-B2B"),
    Column("first_name", "Utf8", "First name"),
    Column("last_name", "Utf8", "Last name"),
    Column("email", "Utf8", "Email address (synthetic)"),
    Column("city", "Utf8", "City"),
    Column("state", "Utf8", "US state code"),
    Column("postal_code", "Utf8", "Postal code"),
    Column("loyalty_member_flag", "Boolean", "Enrolled in loyalty program"),
    Column("loyalty_tier", "Utf8", "Loyalty tier"),
    Column("loyalty_enroll_date", "Date", "Loyalty enrollment date; null if not a member"),
    Column("acquisition_channel", "Utf8", "Channel that acquired the customer"),
    Column("segment", "Utf8", "Marketing/merch segment"),
    Column("email_opt_in", "Boolean", "Opted in to marketing email"),
]

DIM_CUSTOMER_SPEC = TableSpec(
    schema="core",
    name="dim_customer",
    grain="one current row per customer (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_customer(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_customers
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    customer_id = support.business_keys("CUST", n, 8)
    first = support.sample(rng.stream("dim_customer.first"), support.FIRST_NAMES, n).astype(str)
    last = support.sample(rng.stream("dim_customer.last"), support.LAST_NAMES, n).astype(str)
    email = np.char.add(
        np.char.add(np.char.add(np.char.lower(first), "."), np.char.lower(last)),
        np.char.add(np.char.add(".", customer_id.astype(str)), "@example.com"),
    )
    member = rng.stream("dim_customer.member").integers(0, 2, n).astype(bool)
    tier_idx = rng.stream("dim_customer.tier").integers(1, len(_TIERS), n)  # 1..4 (skip "None")
    tier = np.where(member, np.asarray(_TIERS, dtype=object)[tier_idx], "None")
    enroll = support.random_dates(rng.stream("dim_customer.enroll"), date(2015, 1, 1), date(2025, 1, 1), n)
    enroll = np.where(member, enroll, np.datetime64("NaT"))
    data = {
        "customer_sk": sk,
        "customer_id": customer_id,
        "customer_type": support.sample(rng.stream("dim_customer.type"), _TYPES, n),
        "first_name": first,
        "last_name": last,
        "email": email,
        "city": support.sample(rng.stream("dim_customer.city"), support.CITIES, n),
        "state": support.sample(rng.stream("dim_customer.state"), support.US_STATES, n),
        "postal_code": rng.stream("dim_customer.postal").integers(10000, 99999, n).astype(str),
        "loyalty_member_flag": member,
        "loyalty_tier": tier,
        "loyalty_enroll_date": enroll,
        "acquisition_channel": support.sample(rng.stream("dim_customer.acq"), _ACQUISITION, n),
        "segment": support.sample(rng.stream("dim_customer.seg"), _SEGMENTS, n),
        "email_opt_in": rng.stream("dim_customer.optin").integers(0, 2, n).astype(bool),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_CUSTOMER_SPEC.polars_schema()).select(DIM_CUSTOMER_SPEC.column_names)
