from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from ..config import TechmartConfig
from ..framework.schema import Column, TableSpec
from ..framework.scd2 import scd2_columns, with_scd2_current
from ..rng import SeededRng
from . import support
from .product_support import COLORS, assign_taxonomy

_UOMS = ["EA", "EA", "EA", "PK", "BX"]  # weighted toward "each"
_LIFECYCLE = ["Active", "Active", "Active", "Active", "Clearance", "Discontinued"]

_BASE_COLUMNS = [
    Column("product_sk", "Int64", "Surrogate key", is_key=True, nullable=False),
    Column("sku", "Utf8", "Business key (stock-keeping unit)", nullable=False),
    Column("gtin", "Utf8", "Global trade item number (barcode)"),
    Column("model_number", "Utf8", "Manufacturer model number"),
    Column("product_name", "Utf8", "Product display name"),
    Column("product_description", "Utf8", "Rich product description (for GenAI/search)"),
    Column("manufacturer", "Utf8", "Manufacturer name"),
    Column("brand_id", "Utf8", "Brand business key (slug of brand name)"),
    Column("brand_name", "Utf8", "Brand name (hierarchy level 5)"),
    Column("division_id", "Utf8", "Division business key (hierarchy level 1)"),
    Column("division_name", "Utf8", "Division name"),
    Column("department_id", "Utf8", "Department business key (level 2)"),
    Column("department_name", "Utf8", "Department name"),
    Column("category_id", "Utf8", "Category business key (level 3)"),
    Column("category_name", "Utf8", "Category name"),
    Column("subcategory_id", "Utf8", "Subcategory business key (level 4)"),
    Column("subcategory_name", "Utf8", "Subcategory name"),
    Column("primary_vendor_sk", "Int64", "Primary vendor (FK to dim_vendor)"),
    Column("private_label_flag", "Boolean", "Techmart private-label product"),
    Column("is_marketplace", "Boolean", "Sold by a 3rd-party marketplace seller"),
    Column("marketplace_seller_id", "Utf8", "Marketplace seller id; null if first-party"),
    Column("uom", "Utf8", "Unit of measure"),
    Column("color", "Utf8", "Primary color"),
    Column("spec_attributes", "Utf8", "JSON of product specification attributes"),
    Column("weight_kg", "Float64", "Weight in kilograms"),
    Column("dimensions", "Utf8", "Package dimensions LxWxH (cm)"),
    Column("msrp", "Float64", "Manufacturer suggested retail price"),
    Column("list_price", "Float64", "Current list price"),
    Column("standard_cost", "Float64", "Standard unit cost"),
    Column("lifecycle_status", "Utf8", "Active/Clearance/Discontinued"),
    Column("launch_date", "Date", "Product launch date"),
    Column("discontinue_date", "Date", "Discontinuation date; null unless discontinued"),
]

DIM_PRODUCT_SPEC = TableSpec(
    schema="core",
    name="dim_product",
    grain="one current row per SKU (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_product(config: TechmartConfig) -> pl.DataFrame:
    n = config.scale_profile.num_skus
    num_vendors = config.scale_profile.num_vendors
    rng = SeededRng(config.seed)

    tax = assign_taxonomy(rng.stream("dim_product.path"), rng.stream("dim_product.brand"), n)
    brand = tax["brand_name"].astype(str)
    subcat = tax["subcategory_name"].astype(str)

    sku = support.business_keys("SKU", n, 8)
    model_number = support.business_keys("MDL", n, 8)
    brand_id = np.char.upper(np.char.replace(brand, " ", ""))
    color = support.sample(rng.stream("dim_product.color"), COLORS, n).astype(str)

    weight = np.round(rng.stream("dim_product.weight").uniform(0.1, 20.0, n), 2)
    msrp = np.round(rng.stream("dim_product.msrp").uniform(9.99, 2999.99, n), 2)
    list_price = np.round(msrp * (1.0 - rng.stream("dim_product.disc").uniform(0.0, 0.15, n)), 2)
    standard_cost = np.round(msrp * rng.stream("dim_product.cost").uniform(0.5, 0.8, n), 2)

    length = rng.stream("dim_product.len").integers(5, 60, n)
    width = rng.stream("dim_product.wid").integers(5, 40, n)
    height = rng.stream("dim_product.hgt").integers(1, 30, n)
    dims = np.char.add(np.char.add(np.char.add(np.char.add(
        length.astype(str), "x"), width.astype(str)), "x"), height.astype(str))

    product_name = np.char.add(np.char.add(np.char.add(np.char.add(
        brand, " "), subcat), " "), model_number)
    product_description = np.char.add(np.char.add(np.char.add(np.char.add(np.char.add(
        brand, " "), subcat), np.char.add(" (", np.char.add(color, "), model "))), model_number), ".")

    weight_str = np.char.mod("%.2f", weight)
    spec_attributes = np.char.add(np.char.add(np.char.add(np.char.add(np.char.add(
        '{"color":"', color), '","weight_kg":'), weight_str), ',"brand":"'),
        np.char.add(brand, '"}'))

    status = support.sample(rng.stream("dim_product.status"), _LIFECYCLE, n).astype(str)
    launch = support.random_dates(rng.stream("dim_product.launch"), date(2015, 1, 1), date(2024, 6, 1), n)
    disc_days = rng.stream("dim_product.disc_days").integers(30, 1000, n).astype("timedelta64[D]")
    discontinue = np.where(status == "Discontinued", launch + disc_days, np.datetime64("NaT"))

    is_marketplace = rng.stream("dim_product.mkt").random(n) < 0.15
    seller_num = rng.stream("dim_product.seller").integers(1, 200, n)
    seller_id = np.char.add("SELLER", np.char.zfill(seller_num.astype(str), 4))
    marketplace_seller_id = np.where(is_marketplace, seller_id, None).tolist()

    data = {
        "product_sk": support.surrogate_keys(n),
        "sku": sku,
        "gtin": rng.stream("dim_product.gtin").integers(100000000000, 1000000000000, n).astype(str),
        "model_number": model_number,
        "product_name": product_name,
        "product_description": product_description,
        "manufacturer": brand,
        "brand_id": brand_id,
        "brand_name": brand,
        "division_id": tax["division_id"],
        "division_name": tax["division_name"],
        "department_id": tax["department_id"],
        "department_name": tax["department_name"],
        "category_id": tax["category_id"],
        "category_name": tax["category_name"],
        "subcategory_id": tax["subcategory_id"],
        "subcategory_name": tax["subcategory_name"],
        "primary_vendor_sk": rng.stream("dim_product.vendor").integers(1, num_vendors + 1, n),
        "private_label_flag": rng.stream("dim_product.pl").random(n) < 0.1,
        "is_marketplace": is_marketplace,
        "marketplace_seller_id": marketplace_seller_id,
        "uom": support.sample(rng.stream("dim_product.uom"), _UOMS, n),
        "color": color,
        "spec_attributes": spec_attributes,
        "weight_kg": weight,
        "dimensions": dims,
        "msrp": msrp,
        "list_price": list_price,
        "standard_cost": standard_cost,
        "lifecycle_status": status,
        "launch_date": launch,
        "discontinue_date": discontinue,
    }
    df = pl.DataFrame(data)
    df = with_scd2_current(df, config.start_date)
    return df.cast(DIM_PRODUCT_SPEC.polars_schema()).select(DIM_PRODUCT_SPEC.column_names)
