from datetime import date

import numpy as np

from techmart.dimensions import support


def test_surrogate_keys_are_sequential_int64():
    sk = support.surrogate_keys(5)
    assert sk.tolist() == [1, 2, 3, 4, 5]
    assert sk.dtype == np.int64


def test_business_keys_zero_padded():
    keys = support.business_keys("STORE", 3, width=4)
    assert keys.tolist() == ["STORE0001", "STORE0002", "STORE0003"]


def test_sample_is_deterministic_and_in_range():
    rng1 = np.random.default_rng(7)
    rng2 = np.random.default_rng(7)
    a = support.sample(rng1, ["x", "y", "z"], 10)
    b = support.sample(rng2, ["x", "y", "z"], 10)
    assert a.tolist() == b.tolist()
    assert set(a.tolist()) <= {"x", "y", "z"}
    assert len(a) == 10


def test_random_dates_within_bounds():
    rng = np.random.default_rng(1)
    d = support.random_dates(rng, date(2020, 1, 1), date(2020, 12, 31), 100)
    lo = np.datetime64(date(2020, 1, 1))
    hi = np.datetime64(date(2020, 12, 31))
    assert d.min() >= lo and d.max() < hi
    assert len(d) == 100


def test_reference_lists_present():
    assert len(support.US_STATES) >= 12
    assert len(support.CITIES) >= 12
    assert len(support.FIRST_NAMES) >= 15
    assert len(support.LAST_NAMES) >= 15
