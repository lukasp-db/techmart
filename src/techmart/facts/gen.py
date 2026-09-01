"""Deterministic, partition-independent column helpers for fact generation."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

_UNIFORM_MOD = 1_000_000


def uniform_hash(*keys: Column, salt: str) -> Column:
    """Uniform pseudo-random double in [0, 1) keyed on the given columns + salt.

    Pure hash of stable keys, so the value is independent of partitioning and
    identical across runs. Never use ``rand()`` for fact attributes.
    """
    return F.pmod(F.hash(*keys, F.lit(salt)), F.lit(_UNIFORM_MOD)) / F.lit(float(_UNIFORM_MOD))


def bounded_int(*keys: Column, salt: str, lo: int, hi: int) -> Column:
    """Deterministic integer in the inclusive range [lo, hi]."""
    span = hi - lo + 1
    return (F.pmod(F.hash(*keys, F.lit(salt)), F.lit(span)) + F.lit(lo)).cast("int")


def shifted_date_sk(date_col: Column, lag_days: Column, max_date) -> Column:
    """date_sk (yyyymmdd long) for ``date_col + lag_days``, clamped to <= max_date.

    Clamping guarantees the result exists in dim_date (referential integrity),
    since dim_date is contiguous daily up to max_date.
    """
    shifted = F.least(F.date_add(date_col, lag_days), F.lit(max_date))
    return (
        F.year(shifted) * F.lit(10000) + F.month(shifted) * F.lit(100) + F.dayofmonth(shifted)
    ).cast("long")
