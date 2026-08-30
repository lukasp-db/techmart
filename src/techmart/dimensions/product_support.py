from __future__ import annotations

import numpy as np

from ..reference.taxonomy import subcategory_paths

COLORS = [
    "Black", "Silver", "White", "Space Gray", "Blue",
    "Red", "Graphite", "Rose Gold", "Green", "Titanium",
]


def assign_taxonomy(
    rng_path: np.random.Generator,
    rng_brand: np.random.Generator,
    n: int,
) -> dict[str, np.ndarray]:
    """Assign each of n SKUs to a taxonomy path and a department-scoped brand.

    Vectorized over n: the only loops are over the fixed set of taxonomy paths
    (~49) and the per-path brand lists (~6), never over the row count.
    """
    paths = subcategory_paths()
    num_paths = len(paths)

    # Per-path attribute lookup arrays (length = num_paths).
    div_id = np.array([p[0].id for p in paths], dtype=object)
    div_name = np.array([p[0].name for p in paths], dtype=object)
    dep_id = np.array([p[1].id for p in paths], dtype=object)
    dep_name = np.array([p[1].name for p in paths], dtype=object)
    cat_id = np.array([p[2].id for p in paths], dtype=object)
    cat_name = np.array([p[2].name for p in paths], dtype=object)
    sub_id = np.array([p[3].id for p in paths], dtype=object)
    sub_name = np.array([p[3].name for p in paths], dtype=object)

    # Padded brand matrix so brands can be gathered by 2D fancy indexing.
    brand_lists = [p[2].brands for p in paths]
    num_brands = np.array([len(b) for b in brand_lists], dtype=np.int64)
    max_brands = int(num_brands.max())
    brand_matrix = np.empty((num_paths, max_brands), dtype=object)
    for i, brands in enumerate(brand_lists):
        for j in range(max_brands):
            brand_matrix[i, j] = brands[j] if j < len(brands) else brands[0]

    path_idx = rng_path.integers(0, num_paths, n)
    # Per-element upper bound: brand index stays within the path's brand count.
    brand_idx = rng_brand.integers(0, num_brands[path_idx])
    brand = brand_matrix[path_idx, brand_idx]

    return {
        "division_id": div_id[path_idx],
        "division_name": div_name[path_idx],
        "department_id": dep_id[path_idx],
        "department_name": dep_name[path_idx],
        "category_id": cat_id[path_idx],
        "category_name": cat_name[path_idx],
        "subcategory_id": sub_id[path_idx],
        "subcategory_name": sub_name[path_idx],
        "brand_name": brand,
    }
