"""Informational PK/FK constraint specs + pure DDL emitters.

Every constraint is `NOT ENFORCED RELY`: RELY lets the optimizer trust the
constraint (join / group-by elimination). Safe only because generation
guarantees key uniqueness (PK) and referential integrity (FK) by construction.
Pure string generation; the actual ALTER TABLE runs workspace-only.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    ref_schema: str
    ref_table: str
    ref_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableConstraints:
    schema: str
    table: str
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()


def _qualify(catalog: str, schema_prefix: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema_prefix}{schema}.{table}"


def _pk_name(tc: TableConstraints) -> str:
    return f"{tc.table}_pk"


def _fk_name(tc: TableConstraints, fk: ForeignKey) -> str:
    return f"{tc.table}_{'_'.join(fk.columns)}_fk"


def set_not_null_ddls(tc: TableConstraints, *, catalog: str, schema_prefix: str) -> list[str]:
    """Emit `ALTER COLUMN ... SET NOT NULL` for each PK column.

    A PRIMARY KEY constraint requires its columns to be NOT NULL at the table
    level. Delta columns default to nullable even when the generating DataFrame
    marks them non-nullable, so this must run before `pk_ddl`. Safe because PK
    columns are non-null by construction (they are the grain keys).
    """
    table = _qualify(catalog, schema_prefix, tc.schema, tc.table)
    return [f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL;" for col in tc.primary_key]


def pk_ddl(tc: TableConstraints, *, catalog: str, schema_prefix: str) -> str:
    table = _qualify(catalog, schema_prefix, tc.schema, tc.table)
    cols = ", ".join(tc.primary_key)
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {_pk_name(tc)} "
        f"PRIMARY KEY ({cols}) NOT ENFORCED RELY;"
    )


def drop_pk_ddl(tc: TableConstraints, *, catalog: str, schema_prefix: str) -> str:
    # CASCADE so a re-run can drop a PK that fact FKs already reference (the FKs are
    # re-added in pass 2). Harmless on a first run where no constraint exists yet.
    table = _qualify(catalog, schema_prefix, tc.schema, tc.table)
    return f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_pk_name(tc)} CASCADE;"


def fk_ddl(tc: TableConstraints, fk: ForeignKey, *, catalog: str, schema_prefix: str) -> str:
    table = _qualify(catalog, schema_prefix, tc.schema, tc.table)
    ref = _qualify(catalog, schema_prefix, fk.ref_schema, fk.ref_table)
    cols = ", ".join(fk.columns)
    ref_cols = ", ".join(fk.ref_columns)
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {_fk_name(tc, fk)} "
        f"FOREIGN KEY ({cols}) REFERENCES {ref} ({ref_cols}) NOT ENFORCED RELY;"
    )
