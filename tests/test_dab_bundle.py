from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[1]


def test_databricks_yml_structure():
    bundle = yaml.safe_load((_ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "techmart"
    # Parameterized, secret-free: catalog/schema/scale are variables.
    assert set(bundle["variables"]) >= {"catalog", "schema_prefix", "scale_profile"}
    assert "dev" in bundle["targets"]
    # Secret-free: no committed workspace host. Inspect each target's host
    # value directly (a literal URL fails; a ${var...} substitution is fine)
    # rather than scanning the whole dump, which a stray ${...} elsewhere
    # could mask.
    for target in bundle["targets"].values():
        if isinstance(target, dict) and "host" in target:
            assert "${" in str(target["host"]), "committed workspace host in target"


def test_generate_facts_job_is_serverless_notebooks():
    import yaml
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    job = yaml.safe_load((root / "resources" / "generate_facts_job.yml").read_text())["resources"]["jobs"]["generate_facts"]
    assert "job_clusters" not in job
    keys = {t["task_key"] for t in job["tasks"]}
    assert {"generate_dims", "generate_facts"} <= keys
    for t in job["tasks"]:
        assert "notebook_task" in t
    facts = next(t for t in job["tasks"] if t["task_key"] == "generate_facts")
    assert any(d["task_key"] == "generate_dims" for d in facts.get("depends_on", []))


def test_generate_finance_task_wired():
    import yaml
    import pathlib
    y = yaml.safe_load((pathlib.Path(__file__).parent.parent / "resources" / "generate_facts_job.yml").read_text())
    tasks = y["resources"]["jobs"]["generate_facts"]["tasks"]
    by_key = {t["task_key"]: t for t in tasks}
    assert "generate_finance" in by_key
    deps = {d["task_key"] for d in by_key["generate_finance"].get("depends_on", [])}
    assert "generate_facts" in deps
    assert by_key["generate_finance"]["notebook_task"]["notebook_path"].endswith("generate_finance.py")


def test_smoke_profile_exists():
    import yaml
    from pathlib import Path
    profiles = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "scale_profiles.yaml").read_text())["profiles"]
    assert "smoke" in profiles
    assert profiles["smoke"]["num_stores"] <= 10
