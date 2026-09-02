import pathlib, yaml

def test_dashboard_resource_wires_file_and_dataset_defaults():
    repo = pathlib.Path(__file__).resolve().parents[1]
    doc = yaml.safe_load((repo / "resources" / "dashboards.yml").read_text())
    dash = doc["resources"]["dashboards"]["merch_exec"]
    assert dash["file_path"].endswith("dashboards/merch_exec.lvdash.json")
    assert dash["dataset_catalog"] == "${var.catalog}"
    assert dash["dataset_schema"] == "${var.schema_prefix}semantic"
    assert dash["warehouse_id"] == "${resources.sql_warehouses.techmart_warehouse.id}"
    assert dash["display_name"] == "Techmart — Merchandising Executive"
