"""Spark dim_customer builder using dbldatagen + SCD2."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ...config import TechmartConfig
from ...reference.pools import CITIES, FIRST_NAMES, LAST_NAMES, US_STATES
from ..dim_builder import build_scd2_dim, sql_array
from ..framework import SparkColumn, SparkTableSpec
from ..scd2 import scd2_columns

_TYPES = ["Retail", "Commercial-B2B"]
_SEGMENTS = ["DIY-Pro", "Gamer", "Home-Office", "SMB", "Household", "Student"]
_ACQUISITION = ["In-Store", "Web", "Mobile-App", "Marketplace", "Referral"]

# loyalty enroll offset: 2015-01-01 to 2025-01-01 = 3652 days
_ENROLL_OFFSET_MAX = 3652

# Pre-build SQL array literals for element_at expressions.
_FIRST_NAMES_ARR = sql_array(FIRST_NAMES)
_LAST_NAMES_ARR = sql_array(LAST_NAMES)
_CITIES_ARR = sql_array(CITIES)
_STATES_ARR = sql_array(US_STATES)

_BASE_COLUMNS: list[SparkColumn] = [
    SparkColumn("customer_sk", "long", "Surrogate key", is_key=True, nullable=False),
    SparkColumn("customer_id", "string", "Business key", nullable=False),
    SparkColumn("customer_type", "string", "Retail or Commercial-B2B"),
    SparkColumn("first_name", "string", "First name"),
    SparkColumn("last_name", "string", "Last name"),
    SparkColumn("email", "string", "Email address (synthetic)"),
    SparkColumn("city", "string", "City"),
    SparkColumn("state", "string", "US state code"),
    SparkColumn("postal_code", "string", "Postal code"),
    SparkColumn("loyalty_member_flag", "boolean", "Enrolled in loyalty program"),
    SparkColumn("loyalty_tier", "string", "Loyalty tier"),
    SparkColumn("loyalty_enroll_date", "date", "Loyalty enrollment date; null if not a member"),
    SparkColumn("acquisition_channel", "string", "Channel that acquired the customer"),
    SparkColumn("segment", "string", "Marketing/merch segment"),
    SparkColumn("email_opt_in", "boolean", "Opted in to marketing email"),
]

DIM_CUSTOMER_SPEC = SparkTableSpec(
    schema="core",
    name="dim_customer",
    grain="one current row per customer (SCD2 scaffolding)",
    columns=_BASE_COLUMNS + scd2_columns(),
)


def build_dim_customer(spark: SparkSession, config: TechmartConfig) -> DataFrame:
    """Generate dim_customer rows with dbldatagen; mark all rows as SCD2 current."""
    n = config.scale_profile.num_customers

    def add_columns(gen):
        return (
            gen
            # --- surrogate / business keys ---
            .withColumn("customer_sk", "long", expr="id + 1", baseColumn="id")
            .withColumn(
                "customer_id", "string",
                expr="concat('CUST', lpad(cast(id + 1 as string), 8, '0'))",
                baseColumn="id",
            )
            # --- customer type ---
            .withColumn("customer_type", "string", values=_TYPES, random=True)
            # --- name pools (1-based element_at) ---
            .withColumn(
                "first_name_idx", "int",
                minValue=1, maxValue=len(FIRST_NAMES), random=True, omit=True,
            )
            .withColumn(
                "first_name", "string",
                expr=f"element_at({_FIRST_NAMES_ARR}, first_name_idx)",
                baseColumn="first_name_idx",
            )
            .withColumn(
                "last_name_idx", "int",
                minValue=1, maxValue=len(LAST_NAMES), random=True, omit=True,
            )
            .withColumn(
                "last_name", "string",
                expr=f"element_at({_LAST_NAMES_ARR}, last_name_idx)",
                baseColumn="last_name_idx",
            )
            # --- email (derived from first_name, last_name, customer_id) ---
            .withColumn(
                "email", "string",
                expr="concat(lower(first_name), '.', lower(last_name), '.', customer_id, '@example.com')",
                baseColumn=["first_name", "last_name", "customer_id"],
            )
            # --- geo pools ---
            .withColumn(
                "city_idx", "int",
                minValue=1, maxValue=len(CITIES), random=True, omit=True,
            )
            .withColumn(
                "city", "string",
                expr=f"element_at({_CITIES_ARR}, city_idx)",
                baseColumn="city_idx",
            )
            .withColumn(
                "state_idx", "int",
                minValue=1, maxValue=len(US_STATES), random=True, omit=True,
            )
            .withColumn(
                "state", "string",
                expr=f"element_at({_STATES_ARR}, state_idx)",
                baseColumn="state_idx",
            )
            # --- postal code as 5-digit string ---
            .withColumn("postal_int", "int", minValue=10000, maxValue=99999, random=True, omit=True)
            .withColumn(
                "postal_code", "string",
                expr="cast(postal_int as string)",
                baseColumn="postal_int",
            )
            # --- loyalty member flag ---
            .withColumn(
                "loyalty_member_flag", "boolean",
                expr="pmod(abs(hash(id, 'm')), 2) = 0",
                baseColumn="id",
            )
            # --- loyalty tier (members get Bronze/Silver/Gold/Platinum; non-members 'None') ---
            .withColumn(
                "tier_num", "int",
                minValue=1, maxValue=4, random=True, omit=True,
            )
            .withColumn(
                "loyalty_tier", "string",
                expr="case when loyalty_member_flag then element_at(array('Bronze', 'Silver', 'Gold', 'Platinum'), tier_num) else 'None' end",
                baseColumn=["loyalty_member_flag", "tier_num"],
            )
            # --- loyalty enroll date (members get a date; non-members get null) ---
            .withColumn(
                "enroll_off", "int",
                minValue=0, maxValue=_ENROLL_OFFSET_MAX, random=True, omit=True,
            )
            .withColumn(
                "loyalty_enroll_date", "date",
                expr="case when loyalty_member_flag then date_add(to_date('2015-01-01'), enroll_off) else cast(null as date) end",
                baseColumn=["loyalty_member_flag", "enroll_off"],
            )
            # --- acquisition channel and segment ---
            .withColumn("acquisition_channel", "string", values=_ACQUISITION, random=True)
            .withColumn("segment", "string", values=_SEGMENTS, random=True)
            # --- email opt-in ---
            .withColumn(
                "email_opt_in", "boolean",
                expr="pmod(abs(hash(id, 'optin')), 2) = 0",
                baseColumn="id",
            )
        )

    return build_scd2_dim(spark, config, DIM_CUSTOMER_SPEC, n, add_columns)
