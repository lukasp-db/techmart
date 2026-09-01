"""PK/FK constraint registry for the gold tables (applied NOT ENFORCED RELY).

PKs are the grain keys (verified unique by construction); FKs are the
surrogate-key columns referencing the conformed-dimension PKs (RI by
construction). fact_inventory_valuation.category_id is NOT an FK (not a unique
dim key).
"""
from __future__ import annotations

from .constraints import ForeignKey, TableConstraints


def _fk(col: str, schema: str, table: str, ref: str) -> ForeignKey:
    return ForeignKey(columns=(col,), ref_schema=schema, ref_table=table, ref_columns=(ref,))


# Conformed-dimension FKs (all live in core except gl_account/department in finance).
_DATE = _fk("date_sk", "core", "dim_date", "date_sk")
_PRODUCT = _fk("product_sk", "core", "dim_product", "product_sk")
_STORE = _fk("store_sk", "core", "dim_store", "store_sk")
_CUSTOMER = _fk("customer_sk", "core", "dim_customer", "customer_sk")
_EMPLOYEE = _fk("employee_sk", "core", "dim_employee", "employee_sk")
_VENDOR = _fk("vendor_sk", "core", "dim_vendor", "vendor_sk")
_PROMO = _fk("promotion_sk", "core", "dim_promotion", "promotion_sk")
_CHANNEL = _fk("channel_sk", "core", "dim_channel", "channel_sk")
_GL = _fk("gl_account_sk", "finance", "dim_gl_account", "gl_account_sk")
_DEPT = _fk("department_sk", "finance", "dim_department", "department_sk")


def _dim(schema: str, table: str, pk: str) -> TableConstraints:
    return TableConstraints(schema=schema, table=table, primary_key=(pk,))


TABLE_CONSTRAINTS: tuple[TableConstraints, ...] = (
    # --- dimensions (PK on the surrogate key) ---
    _dim("core", "dim_date", "date_sk"),
    _dim("core", "dim_product", "product_sk"),
    _dim("core", "dim_store", "store_sk"),
    _dim("core", "dim_customer", "customer_sk"),
    _dim("core", "dim_employee", "employee_sk"),
    _dim("core", "dim_vendor", "vendor_sk"),
    _dim("core", "dim_promotion", "promotion_sk"),
    _dim("core", "dim_channel", "channel_sk"),
    _dim("finance", "dim_gl_account", "gl_account_sk"),
    _dim("finance", "dim_department", "department_sk"),
    # --- core facts ---
    TableConstraints("core", "fact_sales_line", ("transaction_id", "line_number"),
                     (_DATE, _PRODUCT, _STORE, _CUSTOMER, _EMPLOYEE, _PROMO, _CHANNEL)),
    TableConstraints("core", "fact_inventory_snapshot", ("date_sk", "store_sk", "product_sk"),
                     (_DATE, _STORE, _PRODUCT)),
    TableConstraints("core", "fact_inventory_movement", ("movement_id",),
                     (_DATE, _PRODUCT, _STORE, _VENDOR)),
    TableConstraints("core", "fact_returns", ("rma_id",),
                     (_DATE, _PRODUCT, _STORE, _CUSTOMER, _EMPLOYEE, _CHANNEL)),
    TableConstraints("core", "fact_fulfillment", ("order_id",),
                     (_DATE, _PRODUCT, _STORE, _CUSTOMER, _CHANNEL)),
    TableConstraints("core", "fact_loyalty_activity", ("loyalty_event_id",),
                     (_DATE, _CUSTOMER, _STORE, _CHANNEL)),
    TableConstraints("core", "fact_web_events", ("session_id", "event_number"),
                     (_DATE, _CUSTOMER, _PRODUCT, _CHANNEL)),
    # --- finance facts ---
    TableConstraints("finance", "fact_gl_actuals",
                     ("date_sk", "gl_account_sk", "store_sk", "department_sk"),
                     (_DATE, _GL, _STORE, _DEPT)),
    TableConstraints("finance", "fact_budget_plan",
                     ("date_sk", "gl_account_sk", "store_sk", "department_sk", "plan_version"),
                     (_DATE, _GL, _STORE, _DEPT)),
    TableConstraints("finance", "fact_inventory_valuation",
                     ("date_sk", "store_sk", "category_id"),
                     (_DATE, _STORE)),
    # --- ai facts ---
    TableConstraints("ai", "fact_sales_forecast",
                     ("date_sk", "product_sk", "store_sk", "forecast_version"),
                     (_DATE, _PRODUCT, _STORE)),
)
