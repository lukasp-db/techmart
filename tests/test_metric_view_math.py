# tests/test_metric_view_math.py
import math

import pytest

from techmart.semantic.metric_views import (
    MV_SALES, MV_INVENTORY, MV_FORECAST, MV_GL_ACTUALS, MV_BUDGET_PLAN,
    MV_INVENTORY_VALUATION,
)


def _measure(spark, sample_rows, schema, expr):
    df = spark.createDataFrame(sample_rows, schema=schema)
    df.createOrReplaceTempView("source")
    # backtick the base alias so `source` is treated as an identifier
    sql = expr.replace("source.", "`source`.")
    return spark.sql(f"SELECT {sql} AS m FROM `source`").collect()[0]["m"]


def _by_name(spec):
    return {m.name: m.expr for m in spec.measures}


def test_sales_measures(spark):
    # two lines, one transaction: gross 100+50, discount 10+0, net 90+50,
    # cogs 60+30, margin 30+20, qty 2+1
    schema = ("transaction_id long, line_number int, quantity int, "
              "gross_sales_amount double, discount_amount double, net_sales_amount double, "
              "cogs_amount double, gross_margin_amount double")
    rows = [(1, 1, 2, 100.0, 10.0, 90.0, 60.0, 30.0),
            (1, 2, 1, 50.0, 0.0, 50.0, 30.0, 20.0)]
    m = _by_name(MV_SALES)
    assert _measure(spark, rows, schema, m["gross_sales"]) == 150.0
    assert _measure(spark, rows, schema, m["net_sales"]) == 140.0
    assert _measure(spark, rows, schema, m["discount"]) == 10.0
    assert _measure(spark, rows, schema, m["cogs"]) == 90.0
    assert _measure(spark, rows, schema, m["gross_margin"]) == 50.0
    assert _measure(spark, rows, schema, m["units"]) == 3
    assert _measure(spark, rows, schema, m["line_count"]) == 2
    assert _measure(spark, rows, schema, m["transaction_count"]) == 1
    assert math.isclose(_measure(spark, rows, schema, m["gross_margin_pct"]), 50.0 / 140.0)
    assert math.isclose(_measure(spark, rows, schema, m["discount_rate"]), 10.0 / 150.0)
    assert math.isclose(_measure(spark, rows, schema, m["avg_order_value"]), 140.0)
    assert math.isclose(_measure(spark, rows, schema, m["avg_basket_units"]), 3.0)
    assert math.isclose(_measure(spark, rows, schema, m["avg_unit_price"]), 150.0 / 3.0)


def test_inventory_measures(spark):
    schema = ("store_sk long, product_sk long, on_hand_qty int, available_qty int, "
              "on_hand_cost_value double, days_of_supply double, is_out_of_stock boolean")
    rows = [(1, 10, 4, 3, 40.0, 8.0, False),
            (1, 11, 0, 0, 0.0, 0.0, True),
            (2, 10, 6, 6, 60.0, 4.0, False)]
    m = _by_name(MV_INVENTORY)
    assert _measure(spark, rows, schema, m["on_hand_qty"]) == 10
    assert _measure(spark, rows, schema, m["on_hand_cost_value"]) == 100.0
    assert math.isclose(_measure(spark, rows, schema, m["avg_days_of_supply"]), 4.0)
    assert math.isclose(_measure(spark, rows, schema, m["out_of_stock_rate"]), 1.0 / 3.0)
    assert _measure(spark, rows, schema, m["sku_count"]) == 2
    assert _measure(spark, rows, schema, m["stocked_store_count"]) == 2


def test_valuation_measures(spark):
    schema = "gmroi double, on_hand_cost_value double, markdown_amount double"
    rows = [(2.0, 10.0, 1.0), (4.0, 20.0, 2.0), (6.0, 30.0, 3.0)]
    m = _by_name(MV_INVENTORY_VALUATION)
    assert math.isclose(_measure(spark, rows, schema, m["avg_gmroi"]), 4.0)
    assert _measure(spark, rows, schema, m["on_hand_cost_value"]) == 60.0
    assert _measure(spark, rows, schema, m["markdown"]) == 6.0


def test_forecast_measures(spark):
    schema = "forecast_qty double, forecast_amount double, lower_bound double, upper_bound double"
    rows = [(10.0, 100.0, 8.0, 12.0), (20.0, 200.0, 15.0, 25.0)]
    m = _by_name(MV_FORECAST)
    assert _measure(spark, rows, schema, m["forecast_qty"]) == 30.0
    assert _measure(spark, rows, schema, m["forecast_amount"]) == 300.0
    assert _measure(spark, rows, schema, m["interval_width"]) == (12.0 - 8.0) + (25.0 - 15.0)


def test_finance_measures(spark):
    m = _by_name(MV_GL_ACTUALS)
    assert _measure(spark, [(100.0,), (-25.0,)], "actual_amount double", m["actual_amount"]) == 75.0
    b = _by_name(MV_BUDGET_PLAN)
    schema = "plan_amount double, plan_units long"
    rows = [(100.0, 5), (50.0, 3)]
    assert _measure(spark, rows, schema, b["plan_amount"]) == 150.0
    assert _measure(spark, rows, schema, b["plan_units"]) == 8
