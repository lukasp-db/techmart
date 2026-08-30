from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from ..framework.schema import Column, TableSpec

DIM_DATE_SPEC = TableSpec(
    schema="core",
    name="dim_date",
    grain="one row per calendar day",
    columns=[
        Column("date_sk", "Int64", "Surrogate key in yyyymmdd form", is_key=True, nullable=False),
        Column("date", "Date", "Calendar date", nullable=False),
        Column("day_of_week", "Int64", "ISO day of week (1=Mon..7=Sun)"),
        Column("day_name", "Utf8", "Full day name (e.g. 'Monday')"),
        Column("week", "Int64", "ISO week number"),
        Column("month", "Int64", "Calendar month (1-12)"),
        Column("month_name", "Utf8", "Full month name (e.g. 'January')"),
        Column("quarter", "Int64", "Calendar quarter (1-4)"),
        Column("year", "Int64", "Calendar year"),
        Column("fiscal_year", "Int64", "Retail 4-5-4 fiscal year"),
        Column("fiscal_week", "Int64", "Retail fiscal week (1-53)"),
        Column("fiscal_period", "Int64", "Retail fiscal period (1-12)"),
        Column("fiscal_quarter", "Int64", "Retail fiscal quarter (1-4)"),
        Column("is_weekend", "Boolean", "True on Saturday or Sunday"),
        Column("is_holiday", "Boolean", "True on a recognized US holiday"),
        Column("holiday_name", "Utf8", "Holiday name, else null"),
        Column("selling_season", "Utf8", "Retail selling-season label"),
    ],
)

_DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
_MONTH_SEASON = {
    1: "Post-Holiday", 2: "Post-Holiday", 3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Back-to-School", 8: "Back-to-School", 9: "Back-to-School",
    10: "Fall", 11: "Holiday", 12: "Holiday",
}
# 4-5-4 weeks-per-period pattern (12 periods, 4 quarters of 4-5-4).
_PERIOD_WEEKS = [4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5, 4]


def _first_sunday_of_february(year: int) -> date:
    d = date(year, 2, 1)
    offset = (6 - d.weekday()) % 7  # weekday(): Mon=0..Sun=6
    return d + timedelta(days=offset)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """nth (1-based) `weekday` (Mon=0..Sun=6) of the month."""
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    last = nxt - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def fiscal_attrs(d: date) -> tuple[int, int, int, int]:
    """Return (fiscal_year, fiscal_week, fiscal_period, fiscal_quarter)."""
    fy_start = _first_sunday_of_february(d.year)
    if d < fy_start:
        fiscal_year = d.year - 1
        fy_start = _first_sunday_of_february(d.year - 1)
    else:
        fiscal_year = d.year
    fiscal_week = (d - fy_start).days // 7 + 1
    cumulative = 0
    fiscal_period = len(_PERIOD_WEEKS)  # default to last period for 53-week overflow
    for idx, weeks in enumerate(_PERIOD_WEEKS, start=1):
        cumulative += weeks
        if fiscal_week <= cumulative:
            fiscal_period = idx
            break
    fiscal_quarter = (fiscal_period - 1) // 3 + 1
    return fiscal_year, fiscal_week, fiscal_period, fiscal_quarter


def holiday_name(d: date) -> str | None:
    if (d.month, d.day) == (1, 1):
        return "New Year's Day"
    if (d.month, d.day) == (7, 4):
        return "Independence Day"
    if (d.month, d.day) == (12, 25):
        return "Christmas Day"
    thanksgiving = _nth_weekday(d.year, 11, 3, 4)  # 4th Thursday of November
    if d == thanksgiving:
        return "Thanksgiving"
    if d == thanksgiving + timedelta(days=1):
        return "Black Friday"
    if d == _last_weekday(d.year, 5, 0):  # last Monday of May
        return "Memorial Day"
    if d == _nth_weekday(d.year, 9, 0, 1):  # first Monday of September
        return "Labor Day"
    return None


def build_dim_date(start: date, end: date) -> pl.DataFrame:
    rows: list[dict] = []
    d = start
    while d <= end:
        fy, fw, fp, fq = fiscal_attrs(d)
        hn = holiday_name(d)
        rows.append(
            {
                "date_sk": d.year * 10000 + d.month * 100 + d.day,
                "date": d,
                "day_of_week": d.isoweekday(),
                "day_name": _DAY_NAMES[d.weekday()],
                "week": d.isocalendar()[1],
                "month": d.month,
                "month_name": _MONTH_NAMES[d.month - 1],
                "quarter": (d.month - 1) // 3 + 1,
                "year": d.year,
                "fiscal_year": fy,
                "fiscal_week": fw,
                "fiscal_period": fp,
                "fiscal_quarter": fq,
                "is_weekend": d.weekday() >= 5,
                "is_holiday": hn is not None,
                "holiday_name": hn,
                "selling_season": _MONTH_SEASON[d.month],
            }
        )
        d += timedelta(days=1)
    return pl.DataFrame(rows).select(DIM_DATE_SPEC.column_names)
