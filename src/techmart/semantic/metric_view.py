"""Metric-view specs + a pure YAML/DDL emitter.

`metric_view_ddl` produces a `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE
YAML AS $$...$$` statement (Databricks metric-view spec v1.1). Pure and locally
testable; the actual execution (metric-view engine) is workspace-only.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# YAML 1.1 boolean-like keys that MUST be quoted so they round-trip as strings
# (an unquoted `on:` key parses back as boolean True).
_QUOTE_KEYS = {"on", "off", "yes", "no", "true", "false", "null"}


@dataclass(frozen=True)
class MetricField:
    name: str
    expr: str
    comment: str
    display_name: str | None = None
    synonyms: tuple[str, ...] = ()
    format: dict | None = None


@dataclass(frozen=True)
class MetricJoin:
    name: str
    schema: str
    table: str
    on: str


@dataclass(frozen=True)
class MaterializedView:
    name: str
    dimensions: tuple[str, ...]
    measures: tuple[str, ...]
    type: str = "aggregated"


@dataclass(frozen=True)
class Materialization:
    schedule: str
    mode: str
    materialized_views: tuple[MaterializedView, ...]


@dataclass(frozen=True)
class MetricViewSpec:
    name: str
    source_schema: str
    source_table: str
    comment: str
    dimensions: tuple[MetricField, ...]
    measures: tuple[MetricField, ...]
    joins: tuple[MetricJoin, ...] = ()
    materialization: Materialization | None = None


def _qualify(catalog: str, schema_prefix: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema_prefix}{schema}.{table}"


def _yaml_scalar(value) -> str:
    """Render a scalar. Strings are double-quoted (escaping \\ and "); numbers
    and bools are emitted raw so they round-trip as YAML numbers/bools."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    s = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def _yaml(obj, indent: int = 0) -> list[str]:
    """Deterministic YAML renderer for dict/list/scalar with 2-space indent.
    Dict keys that are YAML boolean-like are quoted (see _QUOTE_KEYS)."""
    pad = "  " * indent
    lines: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f'"{k}"' if str(k).lower() in _QUOTE_KEYS else str(k)
            if isinstance(v, dict):
                lines.append(f"{pad}{key}:")
                lines += _yaml(v, indent + 1)
            elif isinstance(v, list):
                lines.append(f"{pad}{key}:")
                lines += _yaml(v, indent)  # list items align under the key
            else:
                lines.append(f"{pad}{key}: {_yaml_scalar(v)}")
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                rendered = _yaml(item, indent + 1)
                # hang the first key off the "- " marker
                first = rendered[0].lstrip()
                lines.append(f"{pad}- {first}")
                lines += rendered[1:]
            else:
                lines.append(f"{pad}- {_yaml_scalar(item)}")
    else:  # pragma: no cover - scalars handled inline above
        lines.append(f"{pad}{_yaml_scalar(obj)}")
    return lines


def _field_dict(f: MetricField) -> dict:
    d: dict = {"name": f.name, "expr": f.expr}
    if f.display_name is not None:
        d["display_name"] = f.display_name
    d["comment"] = f.comment
    if f.synonyms:
        d["synonyms"] = list(f.synonyms)
    if f.format is not None:
        d["format"] = f.format
    return d


def metric_view_ddl(spec: MetricViewSpec, *, catalog: str, schema_prefix: str) -> str:
    view = _qualify(catalog, schema_prefix, "semantic", spec.name)
    inner: dict = {
        "version": 1.1,
        "comment": spec.comment,
        "source": _qualify(catalog, schema_prefix, spec.source_schema, spec.source_table),
    }
    if spec.joins:
        inner["joins"] = [
            {"name": j.name,
             "source": _qualify(catalog, schema_prefix, j.schema, j.table),
             "on": j.on}
            for j in spec.joins
        ]
    inner["dimensions"] = [_field_dict(d) for d in spec.dimensions]
    inner["measures"] = [_field_dict(m) for m in spec.measures]
    if spec.materialization is not None:
        m = spec.materialization
        inner["materialization"] = {
            "schedule": m.schedule,
            "mode": m.mode,
            "materialized_views": [
                {"name": mv.name, "type": mv.type,
                 "dimensions": list(mv.dimensions), "measures": list(mv.measures)}
                for mv in m.materialized_views
            ],
        }
    body = "\n".join(_yaml(inner))
    return f"CREATE OR REPLACE VIEW {view} WITH METRICS LANGUAGE YAML AS $$\n{body}\n$$"
