import numpy as np

from techmart.dimensions.product_support import COLORS, assign_taxonomy
from techmart.reference.taxonomy import subcategory_paths


def test_assign_returns_nine_arrays_of_length_n():
    out = assign_taxonomy(np.random.default_rng(1), np.random.default_rng(2), 100)
    expected = {
        "division_id", "division_name", "department_id", "department_name",
        "category_id", "category_name", "subcategory_id", "subcategory_name",
        "brand_name",
    }
    assert set(out) == expected
    assert all(len(v) == 100 for v in out.values())


def test_assign_is_deterministic():
    a = assign_taxonomy(np.random.default_rng(1), np.random.default_rng(2), 50)
    b = assign_taxonomy(np.random.default_rng(1), np.random.default_rng(2), 50)
    assert all(np.array_equal(a[k], b[k]) for k in a)


def test_hierarchy_ids_are_internally_consistent():
    # SUB id = "SUB"+dd+pp+cc+ss ; CAT id = "CAT"+dd+pp+cc — so the category's
    # digits must be the prefix of the subcategory's digits.
    out = assign_taxonomy(np.random.default_rng(3), np.random.default_rng(4), 200)
    for cat_id, sub_id in zip(out["category_id"], out["subcategory_id"]):
        assert sub_id[3:9] == cat_id[3:9]


def test_brand_belongs_to_assigned_category():
    # Build the valid (category_id -> brands) map from the taxonomy and verify
    # every row's brand is legal for its category.
    valid = {}
    for _div, _dep, cat, _sub in subcategory_paths():
        valid[cat.id] = set(cat.brands)
    out = assign_taxonomy(np.random.default_rng(5), np.random.default_rng(6), 300)
    for cat_id, brand in zip(out["category_id"], out["brand_name"]):
        assert brand in valid[cat_id]


def test_colors_present():
    assert len(COLORS) >= 8
