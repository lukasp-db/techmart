"""Postgres (Lakebase) table spec + DDL emitter + workspace write path.

`pg_type` / `pg_ddl` / `PgTableSpec` are pure and locally testable. `write_pg`
and `get_pg_connection` import psycopg + databricks-sdk lazily and run only on
the workspace against a live Lakebase instance (proven-green gate).
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..spark.framework import SparkColumn

_PG_TYPES: dict[str, str] = {
    "long": "bigint",
    "int": "integer",
    "double": "double precision",
    "string": "text",
    "boolean": "boolean",
    "timestamp": "timestamptz",
    "date": "date",
}


def pg_type(dtype: str) -> str:
    """Map a framework dtype to its Postgres column type."""
    return _PG_TYPES[dtype]


@dataclass(frozen=True)
class PgTableSpec:
    schema: str  # target schema group, e.g. "ops"
    name: str
    grain: str
    columns: list[SparkColumn]
    primary_key: tuple[str, ...]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def select_ordered(self, df: DataFrame) -> DataFrame:
        """Project df to the spec's columns, in order (comment as field metadata)."""
        return df.select(
            *[F.col(c.name).alias(c.name, metadata={"comment": c.comment}) for c in self.columns]
        )


def _qualified(schema: str, name: str) -> str:
    return f"{schema}.{name}"


def _sql_str(s: str) -> str:
    """Single-quote a SQL string literal, escaping embedded single quotes."""
    return "'" + s.replace("'", "''") + "'"


def pg_ddl(spec: PgTableSpec, schema: str) -> list[str]:
    """Emit CREATE TABLE (PK) + table/column COMMENT statements.

    FKs to lakehouse dims are advisory (documented in column comments), not PG
    constraints: the referenced dims live in Delta/UC, not Postgres.
    """
    table = _qualified(schema, spec.name)
    col_lines = [
        f"  {c.name} {pg_type(c.dtype)}{'' if c.nullable else ' NOT NULL'}"
        for c in spec.columns
    ]
    col_lines.append(f"  PRIMARY KEY ({', '.join(spec.primary_key)})")
    create = f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(col_lines) + "\n);"
    stmts = [create, f"COMMENT ON TABLE {table} IS {_sql_str(spec.grain)};"]
    stmts += [
        f"COMMENT ON COLUMN {table}.{c.name} IS {_sql_str(c.comment)};" for c in spec.columns
    ]
    return stmts


def get_pg_connection(instance_name: str, database: str):
    """Open a psycopg connection to a Lakebase instance via workspace OAuth.

    Workspace-only. Exact SDK credential idiom is validated on the workspace
    (proven-green gate); adjust here if the SDK surface differs.
    """
    import psycopg  # lazy: not a local test/runtime dependency
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    instance = w.database.get_database_instance(name=instance_name)
    cred = w.database.generate_database_credential(
        request_id=instance_name, instance_names=[instance_name]
    )
    return psycopg.connect(
        host=instance.read_write_dns,
        dbname=database,
        user=w.current_user.me().user_name,
        password=cred.token,
        sslmode="require",
    )


def write_pg(df: DataFrame, spec: PgTableSpec, *, conn, schema: str) -> int:
    """Create-if-needed + idempotently reseed a Postgres table from a Spark DF.

    Truncate + insert gives a deterministic baseline on every regeneration.
    Workspace-only. Returns the number of rows written.
    """
    rows = [tuple(r[c] for c in spec.column_names) for r in df.collect()]
    cols = ", ".join(spec.column_names)
    placeholders = ", ".join(["%s"] * len(spec.column_names))
    table = _qualified(schema, spec.name)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        for stmt in pg_ddl(spec, schema):
            cur.execute(stmt)
        cur.execute(f"TRUNCATE TABLE {table};")
        if rows:
            cur.executemany(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", rows)
    conn.commit()
    return len(rows)
