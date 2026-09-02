"""Dataset queryLines for the merch exec dashboard.

All queries use UNQUALIFIED metric-view names (mv_sales, mv_inventory); the DAB
`dashboards` resource sets dataset_catalog/dataset_schema so they resolve to
${catalog}.${schema_prefix}semantic at deploy. Metric views are queried with
MEASURE(); every ratio is NULLIF-guarded. Inventory stock is the latest
snapshot (current position), not summed across days.
"""
from __future__ import annotations

from collections import namedtuple

Dataset = namedtuple("Dataset", "name display_name query_lines")

_WEEKS_PER_PERIOD = 4.333  # retail 4-4-5 fiscal period ≈ 4.33 weeks


def sales_querylines() -> list[str]:
    return [
        "-- Sales flow by category x region x fiscal period",
        "SELECT fiscal_year, fiscal_quarter, fiscal_period,",
        "       division, department, category, region, channel_type,",
        "       MEASURE(net_sales)        AS net_sales,",
        "       MEASURE(gross_sales)      AS gross_sales,",
        "       MEASURE(gross_margin)     AS gross_margin,",
        "       MEASURE(gross_margin_pct) AS gross_margin_pct,",
        "       MEASURE(cogs)             AS cogs,",
        "       MEASURE(units)            AS units,",
        "       MEASURE(transaction_count) AS transaction_count,",
        "       MEASURE(discount_rate)    AS discount_rate",
        "FROM mv_sales",
        "GROUP BY ALL",
    ]


def inventory_querylines() -> list[str]:
    return [
        "-- Current inventory position: latest snapshot only (stock, not summed over days)",
        "SELECT division, department, category, region,",
        "       MEASURE(on_hand_qty)          AS on_hand_qty,",
        "       MEASURE(on_hand_cost_value)   AS on_hand_cost_value,",
        "       MEASURE(on_hand_retail_value) AS on_hand_retail_value,",
        "       MEASURE(out_of_stock_rate)    AS out_of_stock_rate,",
        "       MEASURE(avg_days_of_supply)   AS avg_days_of_supply,",
        "       MEASURE(sku_count)            AS sku_count",
        "FROM mv_inventory",
        "WHERE date = (SELECT MAX(date) FROM mv_inventory)",
        "GROUP BY ALL",
    ]


def bridge_querylines() -> list[str]:
    return [
        "-- Cross-fact bridge: period sales flow ⋈ current inventory position.",
        "-- Sell-through/WOS/GMROI/turns require terms from both facts (NULLIF-guarded).",
        "WITH s AS (",
        "  SELECT fiscal_year, fiscal_quarter, fiscal_period, division, department, category, region,",
        "         MEASURE(net_sales) AS net_sales, MEASURE(gross_margin) AS gross_margin,",
        "         MEASURE(gross_margin_pct) AS gross_margin_pct, MEASURE(units) AS units,",
        "         MEASURE(cogs) AS cogs",
        "  FROM mv_sales GROUP BY ALL",
        "),",
        "i AS (",
        "  SELECT division, department, category, region,",
        "         MEASURE(on_hand_qty) AS on_hand_qty, MEASURE(on_hand_cost_value) AS on_hand_cost_value,",
        "         MEASURE(out_of_stock_rate) AS out_of_stock_rate",
        "  FROM mv_inventory",
        "  WHERE date = (SELECT MAX(date) FROM mv_inventory) GROUP BY ALL",
        ")",
        "SELECT s.fiscal_year, s.fiscal_quarter, s.fiscal_period,",
        "       s.division, s.department, s.category, s.region,",
        "       s.net_sales, s.gross_margin, s.gross_margin_pct, s.units, s.cogs,",
        "       i.on_hand_qty, i.on_hand_cost_value, i.out_of_stock_rate,",
        "       s.units / NULLIF(s.units + i.on_hand_qty, 0)                    AS sell_through_pct,",
        f"       (i.on_hand_qty * {_WEEKS_PER_PERIOD}) / NULLIF(s.units, 0)      AS weeks_of_supply,",
        "       s.gross_margin / NULLIF(i.on_hand_cost_value, 0)                AS gmroi,",
        "       s.cogs / NULLIF(i.on_hand_cost_value, 0)                        AS inventory_turns",
        "FROM s LEFT JOIN i USING (division, department, category, region)",
    ]


def lost_sales_querylines() -> list[str]:
    return [
        "-- Lost-sales risk: high out-of-stock AND high velocity, latest period.",
        "WITH s AS (",
        "  SELECT division, department, category, MEASURE(units) AS units, MEASURE(net_sales) AS net_sales",
        "  FROM mv_sales WHERE fiscal_period = (SELECT MAX(fiscal_period) FROM mv_sales) GROUP BY ALL",
        "),",
        "i AS (",
        "  SELECT division, department, category, MEASURE(out_of_stock_rate) AS out_of_stock_rate",
        "  FROM mv_inventory WHERE date = (SELECT MAX(date) FROM mv_inventory) GROUP BY ALL",
        ")",
        "SELECT s.division, s.department, s.category, s.units, s.net_sales, i.out_of_stock_rate",
        "FROM s JOIN i USING (division, department, category)",
        "WHERE i.out_of_stock_rate > 0.05",
        "ORDER BY (i.out_of_stock_rate * s.units) DESC",
        "LIMIT 25",
    ]


def ai_takeaways_querylines() -> list[str]:
    # ai_query over the current-fiscal-year headline aggregates. :llm_endpoint is a
    # dashboard parameter bound to ${var.llm_endpoint} at deploy.
    return [
        "-- AI Key Takeaways: LLM summary of the headline metrics (current fiscal year).",
        "WITH agg AS (",
        "  SELECT MEASURE(net_sales) AS net_sales, MEASURE(gross_margin_pct) AS gm_pct, MEASURE(units) AS units",
        "  FROM mv_sales WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM mv_sales)",
        "  GROUP BY ALL",
        ")",
        "SELECT ai_query(",
        "  :llm_endpoint,",
        "  CONCAT(",
        "    'You are a retail merchandising analyst. In 3 short bullet points, summarize ',",
        "    'these current-fiscal-year metrics for a VP of Merchandising and flag one risk. ',",
        "    'Net sales: ', CAST(ROUND(net_sales) AS STRING),",
        "    '. Gross margin %: ', CAST(ROUND(gm_pct*100,1) AS STRING),",
        "    '. Units: ', CAST(ROUND(units) AS STRING), '.'",
        "  )",
        ") AS takeaways",
        "FROM agg",
    ]


DATASETS: tuple[Dataset, ...] = (
    Dataset("sales", "Sales performance", sales_querylines()),
    Dataset("inventory", "Inventory position", inventory_querylines()),
    Dataset("bridge", "Sales ⋈ inventory bridge", bridge_querylines()),
    Dataset("lost_sales", "Lost-sales risk", lost_sales_querylines()),
    Dataset("ai_takeaways", "AI key takeaways", ai_takeaways_querylines()),
)
