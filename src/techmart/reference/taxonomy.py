"""Curated electronics merchandise taxonomy for Techmart.

Authored as raw nested content (names only); ids are assigned deterministically
by enumeration order at import time. Brands are defined per department and shared
by that department's categories.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

__all__ = ["Subcategory", "Category", "Department", "Division", "TAXONOMY", "subcategory_paths"]


@dataclass(frozen=True)
class Subcategory:
    id: str
    name: str


@dataclass(frozen=True)
class Category:
    id: str
    name: str
    subcategories: tuple[Subcategory, ...]
    brands: tuple[str, ...]  # department-scoped: every sibling category in the department shares this brand list


@dataclass(frozen=True)
class Department:
    id: str
    name: str
    categories: tuple[Category, ...]


@dataclass(frozen=True)
class Division:
    id: str
    name: str
    departments: tuple[Department, ...]


class _RawDepartment(TypedDict):
    brands: list[str]
    categories: dict[str, list[str]]


# Raw authored content. Structure:
#   division -> department -> {"brands": [...], "categories": {category: [subcategories]}}
_RAW: dict[str, dict[str, _RawDepartment]] = {
    "Computing": {
        "Laptops": {
            "brands": ["Dell", "ASUS", "Lenovo", "HP", "Acer", "Apple"],
            "categories": {
                "Gaming Laptops": ["15\" Gaming Laptops", "17\" Gaming Laptops"],
                "Ultrabooks": ["13\" Ultrabooks", "14\" Ultrabooks"],
                "Business Laptops": ["Standard Business Laptops", "Convertible Business Laptops"],
            },
        },
        "Desktops": {
            "brands": ["Dell", "HP", "Lenovo", "CyberPowerPC", "Apple"],
            "categories": {
                "Gaming Desktops": ["Mid-Tower Gaming Desktops", "Compact Gaming Desktops"],
                "All-in-Ones": ["24\" All-in-Ones", "27\" All-in-Ones"],
            },
        },
        "PC Components": {
            "brands": ["NVIDIA", "AMD", "Intel", "Corsair", "Samsung", "Western Digital"],
            "categories": {
                "Graphics Cards": ["NVIDIA GPUs", "AMD GPUs"],
                "Storage Drives": ["NVMe SSDs", "SATA SSDs", "Hard Disk Drives"],
                "Memory": ["DDR4 Memory", "DDR5 Memory"],
            },
        },
    },
    "Consumer Electronics": {
        "Cameras": {
            "brands": ["Canon", "Nikon", "Sony", "Fujifilm"],
            "categories": {
                "Mirrorless Cameras": ["Full-Frame Mirrorless", "APS-C Mirrorless"],
                "Action Cameras": ["Standard Action Cameras", "360 Action Cameras"],
            },
        },
        "Mobile": {
            "brands": ["Apple", "Samsung", "Google", "Motorola"],
            "categories": {
                "Smartphones": ["Flagship Smartphones", "Mid-Range Smartphones"],
                "Tablets": ["Standard Tablets", "Pro Tablets"],
            },
        },
        "Printers": {
            "brands": ["HP", "Canon", "Epson", "Brother"],
            "categories": {
                "Inkjet Printers": ["All-in-One Inkjet", "Photo Inkjet"],
                "Laser Printers": ["Monochrome Laser", "Color Laser"],
            },
        },
    },
    "Appliances": {
        "Major Appliances": {
            "brands": ["Whirlpool", "LG", "Samsung", "GE"],
            "categories": {
                "Refrigerators": ["French-Door Refrigerators", "Top-Freezer Refrigerators"],
                "Laundry": ["Front-Load Washers", "Electric Dryers"],
            },
        },
        "Small Appliances": {
            "brands": ["Ninja", "Cuisinart", "Keurig", "Dyson"],
            "categories": {
                "Kitchen": ["Blenders", "Coffee Makers"],
                "Home": ["Vacuum Cleaners", "Air Purifiers"],
            },
        },
    },
    "Networking & DIY": {
        "Networking": {
            "brands": ["Ubiquiti", "Netgear", "TP-Link", "ASUS"],
            "categories": {
                "Routers": ["Wi-Fi 6 Routers", "Mesh Routers"],
                "Switches": ["Unmanaged Switches", "Managed Switches"],
            },
        },
        "Cabling & Parts": {
            "brands": ["Monoprice", "Cable Matters", "StarTech", "Belkin"],
            "categories": {
                "Ethernet Cabling": ["Cat6 Ethernet Cable", "Cat6a Ethernet Cable"],
                "Connectors & Tools": ["RJ45 Connectors", "Crimping Tools"],
            },
        },
    },
    "Services": {
        "Support Services": {
            "brands": ["Techmart Care"],
            "categories": {
                "Protection Plans": ["Laptop Protection Plans", "Appliance Protection Plans"],
                "Installation": ["Home Networking Installation", "Appliance Installation"],
            },
        },
    },
}


def _build() -> tuple[Division, ...]:
    divisions: list[Division] = []
    for d_idx, (div_name, deps) in enumerate(_RAW.items(), start=1):
        departments: list[Department] = []
        for p_idx, (dep_name, dep_body) in enumerate(deps.items(), start=1):
            brands = tuple(dep_body["brands"])
            categories: list[Category] = []
            for c_idx, (cat_name, subs) in enumerate(dep_body["categories"].items(), start=1):
                subcategories = tuple(
                    Subcategory(
                        id=f"SUB{d_idx:02d}{p_idx:02d}{c_idx:02d}{s_idx:02d}",
                        name=sub_name,
                    )
                    for s_idx, sub_name in enumerate(subs, start=1)
                )
                categories.append(
                    Category(
                        id=f"CAT{d_idx:02d}{p_idx:02d}{c_idx:02d}",
                        name=cat_name,
                        subcategories=subcategories,
                        brands=brands,
                    )
                )
            departments.append(
                Department(id=f"DEP{d_idx:02d}{p_idx:02d}", name=dep_name, categories=tuple(categories))
            )
        divisions.append(Division(id=f"DIV{d_idx:02d}", name=div_name, departments=tuple(departments)))
    return tuple(divisions)


TAXONOMY: tuple[Division, ...] = _build()


def subcategory_paths() -> list[tuple[Division, Department, Category, Subcategory]]:
    """Every root-to-leaf path through the taxonomy."""
    return [
        (div, dep, cat, sub)
        for div in TAXONOMY
        for dep in div.departments
        for cat in dep.categories
        for sub in cat.subcategories
    ]
