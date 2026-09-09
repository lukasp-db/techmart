"""Category-level price bands and unit-share weights for realistic sales economics.

Bands and weights are authored by category *name* (readable) and resolved to
category *id* against the taxonomy. `dim_product` draws msrp log-uniform within a
product's category band; `fact_sales_line` picks a category by unit-share weight
(cheap categories dominate unit volume), then a product within it. Weights are
tuned so the blended average line lands in the realistic electronics band and TTM
net revenue at showcase scale is ~$25B.
"""
from __future__ import annotations

import math

from .taxonomy import TAXONOMY

# (low, high) msrp band per category, USD. Log-uniform within the band.
CATEGORY_PRICE_BANDS: dict[str, tuple[float, float]] = {
    "Gaming Laptops": (900.0, 3000.0),
    "Ultrabooks": (700.0, 2200.0),
    "Business Laptops": (500.0, 1800.0),
    "Gaming Desktops": (800.0, 3500.0),
    "All-in-Ones": (600.0, 2200.0),
    "Graphics Cards": (200.0, 2000.0),
    "Storage Drives": (40.0, 500.0),
    "Memory": (30.0, 300.0),
    "Mirrorless Cameras": (500.0, 4000.0),
    "Action Cameras": (150.0, 700.0),
    "Smartphones": (250.0, 1500.0),
    "Tablets": (150.0, 1300.0),
    "Inkjet Printers": (40.0, 400.0),
    "Laser Printers": (120.0, 700.0),
    "Refrigerators": (600.0, 3500.0),
    "Laundry": (400.0, 2200.0),
    "Kitchen": (25.0, 500.0),
    "Home": (60.0, 900.0),
    "Routers": (40.0, 700.0),
    "Switches": (25.0, 500.0),
    "Ethernet Cabling": (5.0, 60.0),
    "Connectors & Tools": (5.0, 90.0),
    "Protection Plans": (30.0, 400.0),
    "Installation": (60.0, 350.0),
}

# Relative unit-share weight per category. Cheap, high-turn categories carry the
# large majority of unit volume; big-ticket categories are rare but high-dollar.
CATEGORY_UNIT_WEIGHTS: dict[str, float] = {
    "Gaming Laptops": 2.0,
    "Ultrabooks": 2.0,
    "Business Laptops": 2.0,
    "Gaming Desktops": 1.0,
    "All-in-Ones": 1.0,
    "Graphics Cards": 3.0,
    "Storage Drives": 30.0,
    "Memory": 70.0,
    "Mirrorless Cameras": 1.0,
    "Action Cameras": 5.0,
    "Smartphones": 6.0,
    "Tablets": 4.0,
    "Inkjet Printers": 15.0,
    "Laser Printers": 5.0,
    "Refrigerators": 1.0,
    "Laundry": 2.0,
    "Kitchen": 40.0,
    "Home": 8.0,
    "Routers": 12.0,
    "Switches": 20.0,
    "Ethernet Cabling": 380.0,
    "Connectors & Tools": 320.0,
    "Protection Plans": 50.0,
    "Installation": 15.0,
}


def _name_to_id() -> dict[str, str]:
    return {
        cat.name: cat.id
        for div in TAXONOMY
        for dep in div.departments
        for cat in dep.categories
    }


def price_bands_by_id() -> dict[str, tuple[float, float]]:
    n2i = _name_to_id()
    return {n2i[name]: band for name, band in CATEGORY_PRICE_BANDS.items()}


def unit_weights_by_id() -> dict[str, float]:
    n2i = _name_to_id()
    return {n2i[name]: w for name, w in CATEGORY_UNIT_WEIGHTS.items()}


def category_cdf() -> list[tuple[str, float]]:
    """Ordered (category_id, cumulative-upper) over all categories, normalized to 1.0."""
    weights = unit_weights_by_id()
    ordered = sorted(weights.items())  # deterministic order by category_id
    total = sum(w for _, w in ordered)
    cdf: list[tuple[str, float]] = []
    acc = 0.0
    for cid, w in ordered:
        acc += w / total
        cdf.append((cid, acc))
    return cdf


def loguniform_mean(low: float, high: float) -> float:
    """Mean of a log-uniform draw on [low, high): (high-low)/ln(high/low)."""
    return (high - low) / math.log(high / low)


def expected_msrp_blended() -> float:
    """Unit-share-weighted mean msrp per sales line (uniform product pick within category)."""
    total_w = sum(CATEGORY_UNIT_WEIGHTS.values())
    return sum(
        (CATEGORY_UNIT_WEIGHTS[name] / total_w) * loguniform_mean(lo, hi)
        for name, (lo, hi) in CATEGORY_PRICE_BANDS.items()
    )


def expected_ttm_net(lines_ttm: float, qty_mean: float,
                     list_factor: float, promo_factor: float) -> float:
    """Analytic TTM net = lines * qty * (list_factor*msrp_blended) * promo_factor."""
    return lines_ttm * qty_mean * (list_factor * expected_msrp_blended()) * promo_factor
