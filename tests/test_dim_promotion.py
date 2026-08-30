from datetime import date
from pathlib import Path

from techmart.config import ScaleProfile, TechmartConfig
from techmart.dimensions.dim_promotion import DIM_PROMOTION_SPEC, build_dim_promotion
from techmart.framework.writer import validate_schema


def _cfg(history_years: int) -> TechmartConfig:
    # num_promotions is derived as 60 * history_years.
    profile = ScaleProfile("t", 5, 10, history_years, 1, 8, 4)
    return TechmartConfig(profile, 11, Path("data"), "techmart", "techmart_", date(2026, 1, 31))


def test_promotion_rows_match_derived_count_and_schema():
    cfg = _cfg(2)  # 60 * 2 = 120
    df = build_dim_promotion(cfg)
    assert df.height == 120
    validate_schema(df, DIM_PROMOTION_SPEC)


def test_promotion_dates_within_history_and_ordered():
    cfg = _cfg(2)
    df = build_dim_promotion(cfg)
    assert df["start_date"].min() >= cfg.start_date
    assert (df["end_date"] >= df["start_date"]).all()
    assert df["end_date"].max() <= cfg.end_date


def test_promotion_types_valid():
    cfg = _cfg(1)
    df = build_dim_promotion(cfg)
    assert set(df["promo_type"].to_list()) <= {
        "Markdown", "BOGO", "Bundle", "Coupon", "Vendor-Funded",
    }
