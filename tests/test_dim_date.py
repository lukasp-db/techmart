from datetime import date

from techmart.dimensions.dim_date import (
    DIM_DATE_SPEC,
    build_dim_date,
    fiscal_attrs,
    holiday_name,
)
from techmart.framework.writer import validate_schema


def test_row_count_is_inclusive_day_count():
    df = build_dim_date(date(2024, 1, 1), date(2024, 12, 31))
    assert df.height == 366  # 2024 is a leap year


def test_conforms_to_spec():
    df = build_dim_date(date(2024, 1, 1), date(2024, 1, 31))
    validate_schema(df, DIM_DATE_SPEC)  # no raise


def test_date_sk_is_yyyymmdd_and_unique():
    df = build_dim_date(date(2024, 1, 1), date(2024, 1, 3))
    assert df["date_sk"].to_list() == [20240101, 20240102, 20240103]
    assert df["date_sk"].n_unique() == df.height


def test_weekend_flag():
    df = build_dim_date(date(2024, 1, 6), date(2024, 1, 8))  # Sat, Sun, Mon
    assert df["is_weekend"].to_list() == [True, True, False]


def test_known_holidays():
    df = build_dim_date(date(2024, 12, 24), date(2024, 12, 26))
    names = dict(zip(df["date_sk"].to_list(), df["holiday_name"].to_list()))
    assert names[20241225] == "Christmas Day"
    assert names[20241224] is None
    # Thanksgiving 2024 = Nov 28; Black Friday = Nov 29.
    assert holiday_name(date(2024, 11, 28)) == "Thanksgiving"
    assert holiday_name(date(2024, 11, 29)) == "Black Friday"


def test_fiscal_week_one_starts_first_sunday_of_february():
    # First Sunday of Feb 2024 is Feb 4 -> fiscal week 1, period 1, quarter 1.
    fy, fw, fp, fq = fiscal_attrs(date(2024, 2, 4))
    assert (fy, fw, fp, fq) == (2024, 1, 1, 1)


def test_fiscal_period_pattern_454():
    # Week 5 falls in period 2 (4-5-4: period 1 = weeks 1-4, period 2 = weeks 5-9).
    _, _, period_wk5, _ = fiscal_attrs(date(2024, 3, 3))  # 4 weeks after Feb 4
    assert period_wk5 == 2


def test_fiscal_year_rollback_for_january():
    # Jan 15 2024 precedes the first Sunday of Feb 2024, so it belongs to FY2023.
    assert fiscal_attrs(date(2024, 1, 15))[0] == 2023


def test_fiftythree_week_fiscal_year_overflow():
    # FY2015 is a 53-week retail year; its final week must map to period 12, quarter 4.
    fy, fw, fp, fq = fiscal_attrs(date(2016, 1, 31))
    assert (fy, fw, fp, fq) == (2015, 53, 12, 4)
    assert fw == 53
