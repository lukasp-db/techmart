from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from techmart.config import ScaleProfile, TechmartConfig
from techmart.finance.dim_gl_account import DIM_GL_ACCOUNT_SPEC, build_dim_gl_account
from techmart.reference.gl_accounts import GL_ACCOUNTS

_CFG = TechmartConfig(
    scale_profile=ScaleProfile("t", 5, 40, 1, 3000, 200, 20), seed=42,
    output_dir=Path("data"), catalog="c", schema_prefix="techmart_", end_date=date(2026, 1, 31),
)


def test_schema_and_count(spark):
    df = build_dim_gl_account(spark, _CFG)
    assert df.columns == DIM_GL_ACCOUNT_SPEC.column_names
    assert DIM_GL_ACCOUNT_SPEC.schema == "finance"
    assert df.count() == len(GL_ACCOUNTS)


def test_unique_sk_and_number(spark):
    df = build_dim_gl_account(spark, _CFG)
    assert df.select("gl_account_sk").distinct().count() == df.count()
    assert df.select("account_number").distinct().count() == df.count()
    # sequential 1..N
    sks = sorted(r["gl_account_sk"] for r in df.collect())
    assert sks == list(range(1, len(GL_ACCOUNTS) + 1))


def test_contra_boolean_and_required(spark):
    df = build_dim_gl_account(spark, _CFG)
    by = {r["account_number"]: r for r in df.collect()}
    assert by["4100"]["is_contra"] is True
    assert by["4000"]["account_type"] == "Revenue"
    assert by["1400"]["statement"] == "Balance-Sheet"
