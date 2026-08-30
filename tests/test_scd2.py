from datetime import date, datetime

import polars as pl

from techmart.framework.scd2 import scd2_columns, with_scd2_current


def test_scd2_columns_names_and_order():
    assert [c.name for c in scd2_columns()] == [
        "effective_start_ts",
        "effective_end_ts",
        "is_current",
        "version",
    ]


def test_scd2_columns_carry_comments():
    assert all(c.comment for c in scd2_columns())


def test_with_scd2_current_appends_current_version():
    df = pl.DataFrame({"product_sk": [1, 2, 3]})
    out = with_scd2_current(df, date(2023, 1, 31))
    assert out.columns == [
        "product_sk",
        "effective_start_ts",
        "effective_end_ts",
        "is_current",
        "version",
    ]
    assert out["is_current"].to_list() == [True, True, True]
    assert out["version"].to_list() == [1, 1, 1]
    assert out["effective_end_ts"].null_count() == 3
    assert out["effective_start_ts"].to_list() == [datetime(2023, 1, 31)] * 3


def test_with_scd2_current_dtypes():
    out = with_scd2_current(pl.DataFrame({"x": [1]}), date(2023, 1, 31))
    assert out.schema["effective_start_ts"] == pl.Datetime
    assert out.schema["effective_end_ts"] == pl.Datetime
    assert out.schema["is_current"] == pl.Boolean
    assert out.schema["version"] == pl.Int64
