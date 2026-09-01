"""The six subject-area metric views over the gold star schema.

Each view is single-source (one fact) joined star-style to the conformed dims.
Cross-fact metrics (sell-through, weeks-of-supply, forecast MAPE, budget
attainment) are deferred (spec §Out of scope).
"""
from __future__ import annotations

from .metric_view import (
    MaterializedView, Materialization, MetricField, MetricJoin, MetricViewSpec,
)

_CUR = {"type": "currency"}
_PCT = {"type": "percentage"}


def _j(name: str, schema: str, fk: str, pk: str) -> MetricJoin:
    """Star join: source.<fk> = <name>.<pk>."""
    return MetricJoin(name=name, schema=schema, table=name,
                      on=f"source.{fk} = {name}.{pk}")


# --- shared conformed-dimension join sets ------------------------------------
_JOIN_DATE = _j("dim_date", "core", "date_sk", "date_sk")
_JOIN_PRODUCT = _j("dim_product", "core", "product_sk", "product_sk")
_JOIN_STORE = _j("dim_store", "core", "store_sk", "store_sk")
_JOIN_CUSTOMER = _j("dim_customer", "core", "customer_sk", "customer_sk")
_JOIN_CHANNEL = _j("dim_channel", "core", "channel_sk", "channel_sk")
_JOIN_PROMO = _j("dim_promotion", "core", "promotion_sk", "promotion_sk")
_JOIN_GL = _j("dim_gl_account", "finance", "gl_account_sk", "gl_account_sk")
_JOIN_DEPT = _j("dim_department", "finance", "department_sk", "department_sk")


def _date_dims() -> tuple[MetricField, ...]:
    return (
        MetricField("date", "dim_date.date", "Calendar date", "Date"),
        MetricField("fiscal_year", "dim_date.fiscal_year", "Retail fiscal year",
                    "Fiscal Year", ("FY",)),
        MetricField("fiscal_period", "dim_date.fiscal_period",
                    "Retail fiscal period (1-12)", "Fiscal Period"),
        MetricField("fiscal_quarter", "dim_date.fiscal_quarter",
                    "Retail fiscal quarter", "Fiscal Quarter"),
        MetricField("selling_season", "dim_date.selling_season",
                    "Selling season (Holiday, Back-to-School, ...)", "Selling Season"),
    )


def _product_dims() -> tuple[MetricField, ...]:
    return (
        MetricField("division", "dim_product.division_name", "Product division", "Division"),
        MetricField("department", "dim_product.department_name",
                    "Product department", "Department", ("merch department",)),
        MetricField("category", "dim_product.category_name", "Product category", "Category"),
        MetricField("subcategory", "dim_product.subcategory_name",
                    "Product subcategory", "Subcategory"),
        MetricField("brand", "dim_product.brand_name", "Product brand", "Brand"),
        MetricField("product", "dim_product.product_name", "Product name", "Product"),
    )


def _store_dims() -> tuple[MetricField, ...]:
    return (
        MetricField("region", "dim_store.region_name", "Store region", "Region"),
        MetricField("district", "dim_store.district_name", "Store district", "District"),
        MetricField("store_format", "dim_store.store_format",
                    "Store format (Flagship/Standard/Outlet/Online-only)", "Store Format"),
        MetricField("store", "dim_store.store_name", "Store name", "Store"),
        MetricField("state", "dim_store.state", "Store state", "State"),
    )


# =============================================================================
# mv_sales  (source: core.fact_sales_line) — MATERIALIZED
# =============================================================================
MV_SALES = MetricViewSpec(
    name="mv_sales",
    source_schema="core", source_table="fact_sales_line",
    comment="Sales performance metrics at transaction-line grain.",
    joins=(_JOIN_DATE, _JOIN_PRODUCT, _JOIN_STORE, _JOIN_CUSTOMER, _JOIN_CHANNEL, _JOIN_PROMO),
    dimensions=(
        *_date_dims(),
        MetricField("is_weekend", "dim_date.is_weekend", "Weekend day flag", "Is Weekend"),
        MetricField("is_holiday", "dim_date.is_holiday", "Holiday flag", "Is Holiday"),
        *_product_dims(), *_store_dims(),
        MetricField("channel", "dim_channel.channel_name", "Sales channel", "Channel"),
        MetricField("channel_type", "dim_channel.channel_type",
                    "Channel type (Physical/Digital)", "Channel Type"),
        MetricField("customer_segment", "dim_customer.segment",
                    "Customer segment", "Customer Segment"),
        MetricField("loyalty_tier", "dim_customer.loyalty_tier",
                    "Loyalty tier", "Loyalty Tier"),
        MetricField("customer_type", "dim_customer.customer_type",
                    "Retail or Commercial-B2B", "Customer Type"),
        MetricField("promo_type", "dim_promotion.promo_type",
                    "Promotion type", "Promotion Type"),
        MetricField("funding_source", "dim_promotion.funding_source",
                    "Promotion funding source (Retailer/Vendor)", "Funding Source"),
    ),
    measures=(
        MetricField("gross_sales", "SUM(source.gross_sales_amount)",
                    "Gross sales amount", "Gross Sales", ("revenue",), _CUR),
        MetricField("net_sales", "SUM(source.net_sales_amount)",
                    "Net sales amount (gross - discount)", "Net Sales", ("sales",), _CUR),
        MetricField("discount", "SUM(source.discount_amount)",
                    "Promotional discount", "Discount", (), _CUR),
        MetricField("discount_rate",
                    "SUM(source.discount_amount)/NULLIF(SUM(source.gross_sales_amount),0)",
                    "Discount as a fraction of gross sales", "Discount Rate", (), _PCT),
        MetricField("cogs", "SUM(source.cogs_amount)", "Cost of goods sold", "COGS", (), _CUR),
        MetricField("gross_margin", "SUM(source.gross_margin_amount)",
                    "Gross margin amount (net sales - COGS)", "Gross Margin", ("margin",), _CUR),
        MetricField("gross_margin_pct",
                    "SUM(source.gross_margin_amount)/NULLIF(SUM(source.net_sales_amount),0)",
                    "Gross margin as a fraction of net sales", "Gross Margin %",
                    ("margin percent", "GM%"), _PCT),
        MetricField("units", "SUM(source.quantity)", "Units sold", "Units"),
        MetricField("line_count", "COUNT(1)", "Sales line count", "Line Count"),
        MetricField("transaction_count", "COUNT(DISTINCT source.transaction_id)",
                    "Distinct transaction (receipt) count", "Transactions", ("orders",)),
        MetricField("avg_order_value",
                    "SUM(source.net_sales_amount)/NULLIF(COUNT(DISTINCT source.transaction_id),0)",
                    "Average net sales per transaction", "Avg Order Value", ("AOV",), _CUR),
        MetricField("avg_basket_units",
                    "SUM(source.quantity)/NULLIF(COUNT(DISTINCT source.transaction_id),0)",
                    "Average units per transaction", "Avg Basket Units"),
        MetricField("avg_unit_price",
                    "SUM(source.gross_sales_amount)/NULLIF(SUM(source.quantity),0)",
                    "Average selling price per unit", "Avg Unit Price", (), _CUR),
    ),
    materialization=Materialization(
        schedule="EVERY 24 HOURS", mode="relaxed",
        materialized_views=(
            MaterializedView(
                name="mv_sales_daily",
                dimensions=("date", "region", "department"),
                measures=("net_sales", "gross_margin", "units", "transaction_count"),
            ),
        ),
    ),
)

# =============================================================================
# mv_inventory  (source: core.fact_inventory_snapshot) — MATERIALIZED
# =============================================================================
MV_INVENTORY = MetricViewSpec(
    name="mv_inventory",
    source_schema="core", source_table="fact_inventory_snapshot",
    comment="Inventory stock-position metrics at store x SKU x day grain.",
    joins=(_JOIN_DATE, _JOIN_PRODUCT, _JOIN_STORE),
    dimensions=(*_date_dims(), *_product_dims(), *_store_dims()),
    measures=(
        MetricField("on_hand_qty", "SUM(source.on_hand_qty)", "Units on hand", "On-Hand Units"),
        MetricField("available_qty", "SUM(source.available_qty)",
                    "Available units (on hand - reserved)", "Available Units"),
        MetricField("on_order_qty", "SUM(source.on_order_qty)",
                    "Units on open purchase orders", "On-Order Units"),
        MetricField("reserved_qty", "SUM(source.reserved_qty)", "Reserved units", "Reserved Units"),
        MetricField("on_hand_cost_value", "SUM(source.on_hand_cost_value)",
                    "Inventory at cost", "On-Hand Cost Value", (), _CUR),
        MetricField("on_hand_retail_value", "SUM(source.on_hand_retail_value)",
                    "Inventory at retail", "On-Hand Retail Value", (), _CUR),
        MetricField("avg_days_of_supply", "AVG(source.days_of_supply)",
                    "Average days of supply", "Avg Days of Supply", ("DOS",)),
        MetricField("out_of_stock_rate",
                    "AVG(CASE WHEN source.is_out_of_stock THEN 1 ELSE 0 END)",
                    "Fraction of store x SKU x day cells out of stock", "Out-of-Stock Rate",
                    ("OOS rate",), _PCT),
        MetricField("sku_count", "COUNT(DISTINCT source.product_sk)",
                    "Distinct SKU count", "SKU Count"),
        MetricField("stocked_store_count", "COUNT(DISTINCT source.store_sk)",
                    "Distinct store count", "Store Count"),
    ),
    materialization=Materialization(
        schedule="EVERY 24 HOURS", mode="relaxed",
        materialized_views=(
            MaterializedView(
                name="mv_inventory_daily",
                dimensions=("date", "region", "department"),
                measures=("on_hand_qty", "on_hand_cost_value", "out_of_stock_rate"),
            ),
        ),
    ),
)

# =============================================================================
# mv_inventory_valuation  (source: finance.fact_inventory_valuation)
#   NOTE: no vendor_sk on this fact; category_id/category_name are degenerate.
# =============================================================================
MV_INVENTORY_VALUATION = MetricViewSpec(
    name="mv_inventory_valuation",
    source_schema="finance", source_table="fact_inventory_valuation",
    comment="Finance view of inventory value at store x category x fiscal period.",
    joins=(_JOIN_DATE, _JOIN_STORE),
    dimensions=(
        *_date_dims(), *_store_dims(),
        MetricField("category_id", "source.category_id", "Product category id", "Category Id"),
        MetricField("category_name", "source.category_name",
                    "Product category name", "Category"),
    ),
    measures=(
        MetricField("on_hand_cost_value", "SUM(source.on_hand_cost_value)",
                    "Period-end inventory at cost", "On-Hand Cost Value", (), _CUR),
        MetricField("on_hand_retail_value", "SUM(source.on_hand_retail_value)",
                    "Period-end inventory at retail", "On-Hand Retail Value", (), _CUR),
        MetricField("cogs", "SUM(source.cogs_amount)", "Category COGS", "COGS", (), _CUR),
        MetricField("markdown", "SUM(source.markdown_amount)", "Markdown value", "Markdown", (), _CUR),
        MetricField("shrink", "SUM(source.shrink_amount)", "Shrink value", "Shrink", (), _CUR),
        MetricField("avg_gmroi", "AVG(source.gmroi)",
                    "Gross-margin return on inventory investment", "Avg GMROI", ("GMROI",)),
    ),
)

# =============================================================================
# mv_forecast  (source: ai.fact_sales_forecast)
# =============================================================================
MV_FORECAST = MetricViewSpec(
    name="mv_forecast",
    source_schema="ai", source_table="fact_sales_forecast",
    comment="AI demand-forecast levels at product x store x fiscal week x version.",
    joins=(_JOIN_DATE, _JOIN_PRODUCT, _JOIN_STORE),
    dimensions=(
        *_date_dims(), *_product_dims(), *_store_dims(),
        MetricField("forecast_version", "source.forecast_version",
                    "Forecast model version (baseline/improved)", "Forecast Version"),
        MetricField("model_name", "source.model_name", "Forecast model name", "Model"),
        MetricField("fiscal_week", "source.fiscal_week", "Retail fiscal week", "Fiscal Week"),
    ),
    measures=(
        MetricField("forecast_qty", "SUM(source.forecast_qty)",
                    "Projected units", "Forecast Units"),
        MetricField("forecast_amount", "SUM(source.forecast_amount)",
                    "Projected net sales amount", "Forecast Amount", (), _CUR),
        MetricField("lower_bound", "SUM(source.lower_bound)",
                    "Lower prediction bound (qty)", "Lower Bound"),
        MetricField("upper_bound", "SUM(source.upper_bound)",
                    "Upper prediction bound (qty)", "Upper Bound"),
        MetricField("interval_width", "SUM(source.upper_bound - source.lower_bound)",
                    "Prediction interval width (qty)", "Interval Width"),
    ),
)

# =============================================================================
# mv_gl_actuals  (source: finance.fact_gl_actuals)
# =============================================================================
MV_GL_ACTUALS = MetricViewSpec(
    name="mv_gl_actuals",
    source_schema="finance", source_table="fact_gl_actuals",
    comment="GL actual amounts at account x store x department x fiscal period.",
    joins=(_JOIN_DATE, _JOIN_STORE, _JOIN_GL, _JOIN_DEPT),
    dimensions=(
        *_date_dims(), *_store_dims(),
        MetricField("account_type", "dim_gl_account.account_type",
                    "Revenue/COGS/Opex/Asset", "Account Type"),
        MetricField("statement", "dim_gl_account.statement",
                    "P&L or Balance-Sheet", "Statement"),
        MetricField("account", "dim_gl_account.account_name", "GL account name", "Account"),
        MetricField("account_category", "dim_gl_account.account_category",
                    "Account rollup category", "Account Category"),
        MetricField("department", "dim_department.department_name",
                    "Cost-center department", "Department"),
        MetricField("department_group", "dim_department.department_group",
                    "COGS-bearing or Opex grouping", "Department Group"),
    ),
    measures=(
        MetricField("actual_amount", "SUM(source.actual_amount)",
                    "Actual amount (contra revenue negative)", "Actual Amount", (), _CUR),
    ),
)

# =============================================================================
# mv_budget_plan  (source: finance.fact_budget_plan)
# =============================================================================
MV_BUDGET_PLAN = MetricViewSpec(
    name="mv_budget_plan",
    source_schema="finance", source_table="fact_budget_plan",
    comment="Budget/forecast plan amounts at account x store x department x period x version.",
    joins=(_JOIN_DATE, _JOIN_STORE, _JOIN_GL, _JOIN_DEPT),
    dimensions=(
        *_date_dims(), *_store_dims(),
        MetricField("plan_version", "source.plan_version",
                    "Budget/Forecast/Latest-Estimate", "Plan Version"),
        MetricField("scenario", "source.scenario", "Planning scenario", "Scenario"),
        MetricField("account_type", "dim_gl_account.account_type",
                    "Revenue/COGS/Opex/Asset", "Account Type"),
        MetricField("account", "dim_gl_account.account_name", "GL account name", "Account"),
        MetricField("department", "dim_department.department_name",
                    "Cost-center department", "Department"),
    ),
    measures=(
        MetricField("plan_amount", "SUM(source.plan_amount)", "Planned amount",
                    "Plan Amount", (), _CUR),
        MetricField("plan_units", "SUM(source.plan_units)", "Planned units (proxy)", "Plan Units"),
    ),
)

METRIC_VIEW_SPECS: tuple[MetricViewSpec, ...] = (
    MV_SALES, MV_INVENTORY, MV_INVENTORY_VALUATION, MV_FORECAST, MV_GL_ACTUALS, MV_BUDGET_PLAN,
)
