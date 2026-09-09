from techmart.reference.taxonomy import TAXONOMY
from techmart.reference import pricing


def _all_category_names():
    return [cat.name for div in TAXONOMY for dep in div.departments for cat in dep.categories]


def test_every_category_has_band_and_weight():
    names = _all_category_names()
    assert len(names) == 24
    for n in names:
        assert n in pricing.CATEGORY_PRICE_BANDS, f"missing band: {n}"
        assert n in pricing.CATEGORY_UNIT_WEIGHTS, f"missing weight: {n}"
    # no stray keys
    assert set(pricing.CATEGORY_PRICE_BANDS) == set(names)
    assert set(pricing.CATEGORY_UNIT_WEIGHTS) == set(names)


def test_bands_valid_and_weights_positive():
    for n, (lo, hi) in pricing.CATEGORY_PRICE_BANDS.items():
        assert 0 < lo < hi, f"bad band for {n}: {lo},{hi}"
    for n, w in pricing.CATEGORY_UNIT_WEIGHTS.items():
        assert w > 0, f"non-positive weight for {n}"


def test_by_id_maps_resolve_to_taxonomy_ids():
    bands = pricing.price_bands_by_id()
    weights = pricing.unit_weights_by_id()
    ids = [cat.id for div in TAXONOMY for dep in div.departments for cat in dep.categories]
    assert set(bands) == set(ids)
    assert set(weights) == set(ids)


def test_category_cdf_is_normalized_and_ordered():
    cdf = pricing.category_cdf()
    assert len(cdf) == 24
    uppers = [u for _, u in cdf]
    assert uppers == sorted(uppers)
    assert abs(uppers[-1] - 1.0) < 1e-9


def test_loguniform_mean_between_bounds():
    m = pricing.loguniform_mean(5.0, 60.0)
    assert 5.0 < m < 60.0
    assert abs(m - (55.0 / __import__("math").log(12.0))) < 1e-6


def test_calibration_hits_25B_band():
    # msrp-blended average line must sit in the realistic electronics band.
    blended = pricing.expected_msrp_blended()
    assert 80.0 <= blended <= 100.0, f"blended msrp/line = {blended:.1f}"
    # implied trailing-12-month net at showcase (~270M TTM lines, qty ~1.16,
    # list = 0.925*msrp, promo factor 0.9736) lands near $25B.
    ttm = pricing.expected_ttm_net(lines_ttm=270_000_000, qty_mean=1.16,
                                   list_factor=0.925, promo_factor=0.9736)
    assert 22e9 <= ttm <= 28e9, f"implied TTM net = ${ttm/1e9:.1f}B"
