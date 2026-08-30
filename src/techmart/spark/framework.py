from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

_SPARK_TYPES: dict[str, DataType] = {
    "long": LongType(),
    "int": IntegerType(),
    "double": DoubleType(),
    "string": StringType(),
    "boolean": BooleanType(),
}


class FactSchemaMismatchError(ValueError):
    """Raised when a DataFrame's columns/types do not match its FactSpec."""


@dataclass(frozen=True)
class FactColumn:
    name: str
    dtype: str  # one of _SPARK_TYPES
    comment: str
    is_key: bool = False
    nullable: bool = True


@dataclass(frozen=True)
class FactSpec:
    schema: str  # target schema group, e.g. "core"
    name: str  # table name, e.g. "fact_sales_line"
    grain: str  # one-line description of the row grain
    columns: list[FactColumn]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def struct_type(self) -> StructType:
        return StructType(
            [
                StructField(
                    c.name,
                    _SPARK_TYPES[c.dtype],
                    c.nullable,
                    metadata={"comment": c.comment},
                )
                for c in self.columns
            ]
        )

    def select_ordered(self, df: DataFrame) -> DataFrame:
        """Project df to the spec's columns, in order, attaching each column's
        comment as field metadata. Spark's saveAsTable propagates the
        ``comment`` metadata key to Delta column comments (fuels Genie)."""
        return df.select(
            *[
                F.col(c.name).alias(c.name, metadata={"comment": c.comment})
                for c in self.columns
            ]
        )


def validate_fact_schema(df: DataFrame, spec: FactSpec) -> None:
    expected = {c.name: _SPARK_TYPES[c.dtype] for c in spec.columns}
    actual = dict(df.dtypes)  # name -> simpleString type

    missing = [n for n in expected if n not in actual]
    extra = [n for n in actual if n not in expected]
    if missing or extra:
        raise FactSchemaMismatchError(
            f"{spec.name}: column mismatch (missing={missing}, extra={extra})"
        )

    mismatches = [
        (name, actual[name], dtype.simpleString())
        for name, dtype in expected.items()
        if actual[name] != dtype.simpleString()
    ]
    if mismatches:
        raise FactSchemaMismatchError(
            f"{spec.name}: dtype mismatch (name, actual, expected): {mismatches}"
        )
