from __future__ import annotations

from datetime import date

import numpy as np

US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA",
    "NC", "MI", "WA", "AZ", "MA", "CO", "OR",
]
CITIES = [
    "Springfield", "Riverside", "Franklin", "Greenville", "Bristol",
    "Fairview", "Salem", "Georgetown", "Madison", "Clinton",
    "Arlington", "Ashland", "Dover", "Auburn", "Hudson",
]
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard",
    "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Carlos", "Maria",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Lee",
]


def surrogate_keys(n: int) -> np.ndarray:
    return np.arange(1, n + 1, dtype=np.int64)


def business_keys(prefix: str, n: int, width: int = 6) -> np.ndarray:
    nums = np.arange(1, n + 1)
    return np.char.add(prefix, np.char.zfill(nums.astype(str), width))


def sample(rng: np.random.Generator, values: list, n: int) -> np.ndarray:
    arr = np.asarray(values, dtype=object)
    return arr[rng.integers(0, len(values), size=n)]


def random_dates(rng: np.random.Generator, start: date, end: date, n: int) -> np.ndarray:
    span = (end - start).days
    offsets = rng.integers(0, span, size=n).astype("timedelta64[D]")
    return np.datetime64(start) + offsets
