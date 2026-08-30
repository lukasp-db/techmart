from __future__ import annotations

import numpy as np

from ..reference.taxonomy import subcategory_paths

COLORS = [
    "Black", "Silver", "White", "Space Gray", "Blue",
    "Red", "Graphite", "Rose Gold", "Green", "Titanium",
]

# Precompute taxonomy paths and brand matrix at module level (runs once at import).
_PATHS = subcategory_paths()
_NUM_PATHS = len(_PATHS)

# Per-path attribute lookup arrays (length = _NUM_PATHS).
_DIV_ID = np.array([p[0].id for p in _PATHS], dtype=object)
_DIV_NAME = np.array([p[0].name for p in _PATHS], dtype=object)
_DEP_ID = np.array([p[1].id for p in _PATHS], dtype=object)
_DEP_NAME = np.array([p[1].name for p in _PATHS], dtype=object)
_CAT_ID = np.array([p[2].id for p in _PATHS], dtype=object)
_CAT_NAME = np.array([p[2].name for p in _PATHS], dtype=object)
_SUB_ID = np.array([p[3].id for p in _PATHS], dtype=object)
_SUB_NAME = np.array([p[3].name for p in _PATHS], dtype=object)

# Brand matrix metadata and padding.
_NUM_BRANDS = np.array([len(p[2].brands) for p in _PATHS], dtype=np.int64)
_MAX_BRANDS = int(_NUM_BRANDS.max())

# Padded brand matrix so brands can be gathered by 2D fancy indexing.
_BRAND_MATRIX = np.empty((_NUM_PATHS, _MAX_BRANDS), dtype=object)
for _i, _p in enumerate(_PATHS):
    _brands = _p[2].brands
    _BRAND_MATRIX[_i, : len(_brands)] = _brands
    _BRAND_MATRIX[_i, len(_brands) :] = _brands[0]


def assign_taxonomy(
    rng_path: np.random.Generator,
    rng_brand: np.random.Generator,
    n: int,
) -> dict[str, np.ndarray]:
    """Assign each of n SKUs to a taxonomy path and a department-scoped brand.

    Vectorized over n: the only loops are over the fixed set of taxonomy paths
    (~49) and the per-path brand lists (~6), never over the row count.
    """
    path_idx = rng_path.integers(0, _NUM_PATHS, n)
    brand_idx = rng_brand.integers(0, _NUM_BRANDS[path_idx])

    return {
        "division_id": _DIV_ID[path_idx],
        "division_name": _DIV_NAME[path_idx],
        "department_id": _DEP_ID[path_idx],
        "department_name": _DEP_NAME[path_idx],
        "category_id": _CAT_ID[path_idx],
        "category_name": _CAT_NAME[path_idx],
        "subcategory_id": _SUB_ID[path_idx],
        "subcategory_name": _SUB_NAME[path_idx],
        "brand_name": _BRAND_MATRIX[path_idx, brand_idx],
    }
