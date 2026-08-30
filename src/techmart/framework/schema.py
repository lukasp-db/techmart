from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str  # Polars dtype name, e.g. "Int64", "Utf8", "Date", "Boolean"
    comment: str
    is_key: bool = False
    nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    schema: str  # target schema group: "core", "finance", "ai", "ops", "semantic"
    name: str  # table name, e.g. "dim_date"
    grain: str  # one-line description of the row grain
    columns: list[Column]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]
