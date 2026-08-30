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


def test_generate_facts_job_is_serverless():
    job_doc = yaml.safe_load((_ROOT / "resources" / "generate_facts_job.yml").read_text())
    job = job_doc["resources"]["jobs"]["generate_facts"]
    task = job["tasks"][0]
    # Serverless: no classic job_clusters; an environment or serverless task.
    assert "job_clusters" not in job
    assert "python_wheel_task" in task or "spark_python_task" in task
