from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _read(name):
    return (_ROOT / "notebooks" / name).read_text()


def test_notebooks_are_databricks_sources():
    for name in ["generate_dims.py", "generate_facts.py"]:
        text = _read(name)
        assert text.splitlines()[0] == "# Databricks notebook source"
        assert "%pip install dbldatagen" in text
        assert "dbutils.widgets" in text
        assert "write_table_uc" in text


def test_dims_notebook_covers_all_dims():
    text = _read("generate_dims.py")
    for b in ["build_dim_date", "build_dim_channel", "build_dim_store", "build_dim_vendor",
              "build_dim_promotion", "build_dim_employee", "build_dim_customer", "build_dim_product"]:
        assert b in text
