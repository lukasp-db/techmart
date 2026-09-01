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
        # sql_task tasks (e.g. generate_ai_text) are the only non-notebook tasks
        assert "notebook_task" in t or "sql_task" in t
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


def test_ai_bundle_variables_present():
    import yaml
    bundle = yaml.safe_load((_ROOT / "databricks.yml").read_text())
    assert {"warehouse_id", "llm_endpoint"} <= set(bundle["variables"])
    # warehouse_id must have NO committed default (supplied per-deploy, like host)
    wid = bundle["variables"]["warehouse_id"]
    assert "default" not in wid or wid.get("default") in (None, "")


def test_ai_tasks_wired_with_fanout():
    import yaml
    job = yaml.safe_load((_ROOT / "resources" / "generate_facts_job.yml").read_text())["resources"]["jobs"]["generate_facts"]
    by_key = {t["task_key"]: t for t in job["tasks"]}
    # generate_ai is a notebook task depending only on generate_facts (parallel to finance)
    assert "generate_ai" in by_key
    ai_deps = {d["task_key"] for d in by_key["generate_ai"].get("depends_on", [])}
    assert ai_deps == {"generate_facts"}
    assert "notebook_task" in by_key["generate_ai"]
    # finance still depends only on generate_facts (not chained behind AI)
    fin_deps = {d["task_key"] for d in by_key["generate_finance"].get("depends_on", [])}
    assert fin_deps == {"generate_facts"}
    # generate_ai_text is a SQL task on the warehouse, depending on generate_ai
    assert "generate_ai_text" in by_key
    txt = by_key["generate_ai_text"]
    assert "sql_task" in txt
    assert "${var.warehouse_id}" in str(txt["sql_task"].get("warehouse_id", ""))
    assert {d["task_key"] for d in txt.get("depends_on", [])} == {"generate_ai"}


def test_ai_text_sql_applies_comments_to_final_tables():
    sql = (_ROOT / "resources" / "generate_ai_text.sql").read_text()
    # Both final tables are created with explicit column definitions + comments.
    assert "ai.product_review'" in sql and "ai.service_case'" in sql
    # Comments are present (Genie contract on the marquee tables).
    assert sql.count("COMMENT") >= 18  # 9 + 9 column comments (+ 2 table comments)
    assert "ai_query(:llm_endpoint" in sql


def test_smoke_profile_exists():
    import yaml
    from pathlib import Path
    profiles = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "scale_profiles.yaml").read_text())["profiles"]
    assert "smoke" in profiles
    assert profiles["smoke"]["num_stores"] <= 10
