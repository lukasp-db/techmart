# tests/test_dashboard_build.py
import json, pathlib
from techmart.dashboards.build import build_dashboard, write_dashboard
from techmart.dashboards.datasets import DATASETS
from techmart.dashboards.theme import ui_theme

def test_dashboard_has_all_datasets_and_one_page():
    d = build_dashboard()
    ds_names = {x["name"] for x in d["datasets"]}
    assert {ds.name for ds in DATASETS} <= ds_names
    assert len(d["pages"]) == 1
    assert d["uiSettings"]["theme"] == ui_theme()

def test_every_widget_binds_a_defined_dataset():
    d = build_dashboard()
    defined = {x["name"] for x in d["datasets"]}
    for item in d["pages"][0]["layout"]:
        for q in item["widget"].get("queries", []):
            assert q["query"]["datasetName"] in defined

def test_six_kpi_counters_present():
    d = build_dashboard()
    counters = [it for it in d["pages"][0]["layout"]
                if it["widget"].get("spec", {}).get("widgetType") == "counter"]
    assert len(counters) == 6
    titles = {c["widget"]["spec"]["frame"]["title"] for c in counters}
    assert {"Net Sales", "Gross Margin %", "Units", "Sell-Through %",
            "Weeks of Supply", "GMROI"} <= titles

def test_category_pivot_and_filters_bind_bridge():
    d = build_dashboard()
    layout = d["pages"][0]["layout"]
    pivots = [it for it in layout if it["widget"]["spec"].get("widgetType") == "pivot"]
    assert len(pivots) == 1
    assert pivots[0]["widget"]["queries"][0]["query"]["datasetName"] == "bridge"

def test_positions_do_not_overlap_and_fit_12_cols():
    d = build_dashboard()
    for it in d["pages"][0]["layout"]:
        p = it["position"]
        assert 0 <= p["x"] and p["x"] + p["width"] <= 12

def test_write_dashboard_roundtrips(tmp_path):
    out = tmp_path / "merch_exec.lvdash.json"
    write_dashboard(str(out))
    reloaded = json.loads(out.read_text())
    assert reloaded == build_dashboard()

def test_committed_json_matches_builder():
    # the committed artifact must equal build_dashboard() output (no drift)
    repo = pathlib.Path(__file__).resolve().parents[1]
    committed = json.loads((repo / "dashboards" / "merch_exec.lvdash.json").read_text())
    assert committed == build_dashboard()
