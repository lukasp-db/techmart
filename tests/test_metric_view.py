import yaml

from techmart.semantic.metric_view import (
    MaterializedView, Materialization, MetricField, MetricJoin, MetricViewSpec,
    metric_view_ddl,
)

_SPEC = MetricViewSpec(
    name="mv_demo",
    source_schema="core",
    source_table="fact_sales_line",
    comment="Demo sales metrics",
    joins=(
        MetricJoin(name="dim_date", schema="core", table="dim_date",
                   on="source.date_sk = dim_date.date_sk"),
    ),
    dimensions=(
        MetricField(name="fiscal_year", expr="dim_date.fiscal_year",
                    comment="Retail fiscal year", display_name="Fiscal Year",
                    synonyms=("FY",)),
    ),
    measures=(
        MetricField(name="net_sales", expr="SUM(source.net_sales_amount)",
                    comment="Net sales", display_name="Net Sales",
                    format={"type": "currency"}),
        MetricField(name="gross_margin_pct",
                    expr="SUM(source.gross_margin_amount)/NULLIF(SUM(source.net_sales_amount),0)",
                    comment="Gross margin percent", display_name="Gross Margin %",
                    format={"type": "percentage"}),
    ),
    materialization=Materialization(
        schedule="EVERY 24 HOURS", mode="relaxed",
        materialized_views=(
            MaterializedView(name="mv_demo_daily",
                             dimensions=("fiscal_year",), measures=("net_sales",)),
        ),
    ),
)


def test_ddl_header_and_wrapper():
    ddl = metric_view_ddl(_SPEC, catalog="cat", schema_prefix="tm_")
    assert ddl.startswith(
        "CREATE OR REPLACE VIEW cat.tm_semantic.mv_demo WITH METRICS LANGUAGE YAML AS $$"
    )
    assert ddl.rstrip().endswith("$$")


def test_inner_yaml_round_trips_with_quoted_on_key():
    ddl = metric_view_ddl(_SPEC, catalog="cat", schema_prefix="tm_")
    inner = ddl.split("$$")[1]
    doc = yaml.safe_load(inner)
    assert doc["version"] == 1.1
    assert doc["source"] == "cat.tm_core.fact_sales_line"
    assert doc["joins"][0]["name"] == "dim_date"
    assert doc["joins"][0]["source"] == "cat.tm_core.dim_date"
    # The join key MUST survive as the string "on", not boolean True.
    assert doc["joins"][0]["on"] == "source.date_sk = dim_date.date_sk"
    assert True not in doc["joins"][0]  # no bool key from an unquoted `on`


def test_dimensions_and_measures_present():
    doc = yaml.safe_load(metric_view_ddl(_SPEC, catalog="c", schema_prefix="tm_").split("$$")[1])
    dim = doc["dimensions"][0]
    assert dim == {"name": "fiscal_year", "expr": "dim_date.fiscal_year",
                   "display_name": "Fiscal Year", "comment": "Retail fiscal year",
                   "synonyms": ["FY"]}
    names = {m["name"]: m for m in doc["measures"]}
    assert names["net_sales"]["expr"] == "SUM(source.net_sales_amount)"
    assert names["net_sales"]["format"] == {"type": "currency"}
    assert names["gross_margin_pct"]["format"] == {"type": "percentage"}


def test_materialization_block():
    doc = yaml.safe_load(metric_view_ddl(_SPEC, catalog="c", schema_prefix="tm_").split("$$")[1])
    mat = doc["materialization"]
    assert mat["schedule"] == "EVERY 24 HOURS"
    assert mat["mode"] == "relaxed"
    mv = mat["materialized_views"][0]
    assert mv == {"name": "mv_demo_daily", "type": "aggregated",
                  "dimensions": ["fiscal_year"], "measures": ["net_sales"]}


def test_no_materialization_omits_block():
    spec = MetricViewSpec(name="mv_x", source_schema="ai", source_table="fact_sales_forecast",
                          comment="c", dimensions=(), measures=(
                              MetricField(name="q", expr="SUM(source.forecast_qty)", comment="q"),))
    doc = yaml.safe_load(metric_view_ddl(spec, catalog="c", schema_prefix="tm_").split("$$")[1])
    assert "materialization" not in doc
    assert "joins" not in doc  # empty joins tuple omits the key
