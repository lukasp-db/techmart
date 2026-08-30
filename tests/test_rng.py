import numpy as np

from techmart.rng import SeededRng


def test_same_seed_and_name_is_reproducible():
    a = SeededRng(42).stream("dim_date").integers(0, 1_000_000, size=50)
    b = SeededRng(42).stream("dim_date").integers(0, 1_000_000, size=50)
    assert np.array_equal(a, b)


def test_different_names_are_independent():
    a = SeededRng(42).stream("dim_store").integers(0, 1_000_000, size=50)
    b = SeededRng(42).stream("dim_product").integers(0, 1_000_000, size=50)
    assert not np.array_equal(a, b)


def test_different_base_seeds_diverge():
    a = SeededRng(1).stream("dim_date").integers(0, 1_000_000, size=50)
    b = SeededRng(2).stream("dim_date").integers(0, 1_000_000, size=50)
    assert not np.array_equal(a, b)


def test_stream_returns_numpy_generator():
    assert isinstance(SeededRng(7).stream("x"), np.random.Generator)
