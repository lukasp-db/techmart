"""Shared dbldatagen scaffolding for SCD2 dimensions."""
from __future__ import annotations

from typing import Callable

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession

from ..config import TechmartConfig
from .framework import SparkTableSpec
from .scd2 import with_scd2_current


def sql_array(values: list[str]) -> str:
    """Build a Spark SQL ``array('a', 'b', ...)`` literal for element_at lookups."""
    return "array(" + ", ".join(f"'{v}'" for v in values) + ")"


def build_scd2_dim(
    spark: SparkSession,
    config: TechmartConfig,
    spec: SparkTableSpec,
    rows: int,
    add_columns: Callable[[dg.DataGenerator], dg.DataGenerator],
) -> DataFrame:
    """Run the shared dbldatagen + SCD2 pipeline for a dimension builder.

    ``add_columns`` receives a generator already configured with the fixed seed
    and ``.withIdOutput()`` and returns it with the dimension's ``withColumn``
    chain applied. The helper builds, drops the raw ``id``, appends SCD2 current
    columns, and projects to the spec (attaching column comments).
    """
    gen = dg.DataGenerator(
        spark,
        name=spec.name,
        rows=rows,
        partitions=max(1, min(64, rows // 100_000)),
        randomSeed=config.seed,
        randomSeedMethod="hash_fieldname",
    ).withIdOutput()
    df = add_columns(gen).build().drop("id")
    df = with_scd2_current(df, config.start_date)
    return spec.select_ordered(df)
