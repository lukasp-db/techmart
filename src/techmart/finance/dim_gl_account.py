"""dim_gl_account: chart of accounts (techmart_finance)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig
from ..reference.gl_accounts import GL_ACCOUNTS
from ..spark.framework import SparkColumn, SparkTableSpec

DIM_GL_ACCOUNT_SPEC = SparkTableSpec(
    schema="finance",
    name="dim_gl_account",
    grain="one row per general-ledger account",
    columns=[
        SparkColumn("gl_account_sk", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("account_number", "string", "Account business key", nullable=False),
        SparkColumn("account_name", "string", "Account name"),
        SparkColumn("account_type", "string", "Revenue/COGS/Opex/Asset"),
        SparkColumn("statement", "string", "P&L or Balance-Sheet"),
        SparkColumn("statement_section", "string", "Rollup level 1"),
        SparkColumn("account_category", "string", "Rollup level 2"),
        SparkColumn("normal_balance", "string", "Debit or Credit"),
        SparkColumn("is_contra", "boolean", "True for contra accounts", nullable=False),
    ],
)


def build_dim_gl_account(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    rows = [
        (
            i, a["account_number"], a["account_name"], a["account_type"],
            a["statement"], a["statement_section"], a["account_category"],
            a["normal_balance"], a["is_contra"],
        )
        for i, a in enumerate(GL_ACCOUNTS, start=1)
    ]
    return spark.createDataFrame(rows, schema=DIM_GL_ACCOUNT_SPEC.struct_type())
