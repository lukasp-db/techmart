import ast
from pathlib import Path

_NB = Path(__file__).parent.parent / "notebooks" / "generate_finance.py"
_JOB = Path(__file__).parent.parent / "src" / "techmart" / "jobs" / "generate_finance.py"


def test_notebook_parses_and_covers_all_tables():
    src = _NB.read_text()
    ast.parse(src)
    for name in ("dim_department", "dim_gl_account", "fact_gl_actuals",
                 "fact_budget_plan", "fact_inventory_valuation"):
        assert name in src


def test_job_module_parses_and_has_main():
    src = _JOB.read_text()
    tree = ast.parse(src)
    assert any(isinstance(n, ast.FunctionDef) and n.name == "main" for n in tree.body)
    assert "dbutils" not in src
