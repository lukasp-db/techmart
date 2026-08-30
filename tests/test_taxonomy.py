from techmart.reference.taxonomy import (
    TAXONOMY,
    Category,
    Division,
    subcategory_paths,
)


def _all_ids():
    ids = []
    for div in TAXONOMY:
        ids.append(div.id)
        for dep in div.departments:
            ids.append(dep.id)
            for cat in dep.categories:
                ids.append(cat.id)
                for sub in cat.subcategories:
                    ids.append(sub.id)
    return ids


def test_taxonomy_is_nonempty_and_typed():
    assert len(TAXONOMY) >= 5
    assert all(isinstance(d, Division) for d in TAXONOMY)


def test_all_ids_unique():
    ids = _all_ids()
    assert len(ids) == len(set(ids))


def test_id_prefixes_by_level():
    div = TAXONOMY[0]
    assert div.id.startswith("DIV")
    assert div.departments[0].id.startswith("DEP")
    assert div.departments[0].categories[0].id.startswith("CAT")
    assert div.departments[0].categories[0].subcategories[0].id.startswith("SUB")


def test_every_category_has_subcategories_and_brands():
    for div in TAXONOMY:
        for dep in div.departments:
            for cat in dep.categories:
                assert isinstance(cat, Category)
                assert len(cat.subcategories) >= 1
                assert len(cat.brands) >= 1


def test_expected_divisions_present():
    names = {d.name for d in TAXONOMY}
    assert {"Computing", "Consumer Electronics", "Appliances", "Networking & DIY"} <= names


def test_subcategory_paths_cover_every_leaf():
    paths = subcategory_paths()
    leaf_count = sum(
        len(cat.subcategories)
        for div in TAXONOMY
        for dep in div.departments
        for cat in dep.categories
    )
    assert len(paths) == leaf_count
    div, dep, cat, sub = paths[0]
    assert isinstance(div, Division) and sub in cat.subcategories
