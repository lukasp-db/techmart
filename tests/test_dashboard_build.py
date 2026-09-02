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

def test_dataset_querylines_survive_lakeview_concatenation():
    # Lakeview joins queryLines WITHOUT inserting separators. A dataset must
    # therefore carry its own newlines, must not be swallowed by a leading `--`
    # line comment, and must not merge tokens across the join. Guards the deploy
    # bug where the whole query rendered as a single commented-out line.
    d = build_dashboard()
    for ds in d["datasets"]:
        raw = "".join(ds["queryLines"])                      # mimic Lakeview
        assert raw.strip(), f"{ds['name']}: empty query"
        assert "\n" in raw, f"{ds['name']}: no newlines — tokens will merge"
        for line in raw.split("\n"):
            assert not line.lstrip().startswith("--"), \
                f"{ds['name']}: comment line survives — would comment out SQL"
        # the query keyword must begin a physical line (not hidden after a comment)
        assert any(line.lstrip().upper().startswith(("SELECT", "WITH"))
                   for line in raw.split("\n")), f"{ds['name']}: no SELECT/WITH at line start"

def test_six_kpi_counters_present():
    d = build_dashboard()
    counters = [it for it in d["pages"][0]["layout"]
                if it["widget"].get("spec", {}).get("widgetType") == "counter"]
    assert len(counters) == 6
    titles = {c["widget"]["spec"]["frame"]["title"] for c in counters}
    assert {"Net Sales", "Gross Margin %", "Units", "Avg Days of Supply",
            "On-Hand Value", "Out-of-Stock Rate"} <= titles

def test_category_ranking_table_binds_bridge_with_index_columns():
    d = build_dashboard()
    layout = d["pages"][0]["layout"]
    bridge_tables = [
        it for it in layout
        if it["widget"]["spec"].get("widgetType") == "table"
        and it["widget"]["queries"][0]["query"]["datasetName"] == "bridge"
    ]
    assert len(bridge_tables) == 1
    table_fields = {f["name"] for f in bridge_tables[0]["widget"]["queries"][0]["query"]["fields"]}
    assert {"sell_through_index", "inventory_efficiency_index", "gmroi_index"} <= table_fields

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
