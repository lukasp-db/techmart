from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support

_REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
_FORMATS = ["Flagship", "Standard", "Outlet", "Online-only"]

_BASE_COLUMNS = [
    Column("store_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("store_id", "Utf8", "Business key", nullable=False),
    Column("store_name", "Utf8", "Store display name"),
    Column("store_format", "Utf8", "Flagship/Standard/Outlet/Online-only"),
    Column("region_id", "Utf8", "Region business key"),
    Column("region_name", "Utf8", "Region name"),
    Column("district_id", "Utf8", "District business key"),
    Column("district_name", "Utf8", "District name"),
    Column("city", "Utf8", "City"),
    Column("state", "Utf8", "US state code"),
    Column("postal_code", "Utf8", "Postal code"),
    Column("country", "Utf8", "Country code"),
    Column("latitude", "Float64", "Latitude"),
    Column("longitude", "Float64", "Longitude"),
    Column("square_footage", "Int64", "Store square footage"),
    Column("open_date", "Date", "Store opening date"),
    Column("status", "Utf8", "Operating status"),
    Column("is_ship_from_store", "Boolean", "Fulfills online orders"),
    Column("is_bopis_enabled", "Boolean", "Supports buy-online-pickup-in-store"),
    Column("cost_center_id", "Utf8", "Finance cost center identifier"),
]

DIM_STORE_SPEC = TableSpec(
    schema="core",
    name="dim_store",
    grain="one current row per store (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_store(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_stores
    rng = SeededRng(config.seed)
    sk = support.surrogate_keys(n)
    region_idx = rng.stream("dim_store.region").integers(0, len(_REGIONS), n)
    district_idx = rng.stream("dim_store.district").integers(1, 21, n)  # 20 districts
    postal = rng.stream("dim_store.postal").integers(10000, 99999, n)
    data = {
        "store_sk": sk,
        "store_id": support.business_keys("STORE", n, 5),
        "store_name": np.char.add("Techmart ", support.business_keys("STORE", n, 5)),
        "store_format": support.sample(rng.stream("dim_store.format"), _FORMATS, n),
        "region_id": np.char.add("RGN", (region_idx + 1).astype(str)),
        "region_name": np.asarray(_REGIONS, dtype=object)[region_idx],
        "district_id": np.char.add("DST", np.char.zfill(district_idx.astype(str), 2)),
        "district_name": np.char.add("District ", np.char.zfill(district_idx.astype(str), 2)),
        "city": support.sample(rng.stream("dim_store.city"), support.CITIES, n),
        "state": support.sample(rng.stream("dim_store.state"), support.US_STATES, n),
        "postal_code": postal.astype(str),
        "country": np.full(n, "US", dtype=object),
        "latitude": rng.stream("dim_store.lat").uniform(25.0, 49.0, n),
        "longitude": rng.stream("dim_store.lon").uniform(-124.0, -67.0, n),
        "square_footage": rng.stream("dim_store.sqft").integers(15000, 45000, n),
        "open_date": support.random_dates(rng.stream("dim_store.open"), date(2005, 1, 1), date(2019, 1, 1), n),
        "status": np.full(n, "Active", dtype=object),
        "is_ship_from_store": rng.stream("dim_store.sfs").integers(0, 2, n).astype(bool),
        "is_bopis_enabled": rng.stream("dim_store.bopis").integers(0, 2, n).astype(bool),
        "cost_center_id": np.char.add("CC", np.char.zfill(sk.astype(str), 5)),
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_STORE_SPEC.polars_schema()).select(DIM_STORE_SPEC.column_names)
