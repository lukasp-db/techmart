from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .framework import SparkTableSpec, validate_spark_schema


def target_table_name(spec: SparkTableSpec, catalog: str, schema_prefix: str) -> str:
    """Fully-qualified UC table name: <catalog>.<schema_prefix><group>.<name>."""
    return f"{catalog}.{schema_prefix}{spec.schema}.{spec.name}"


def write_table_uc(
    spark: SparkSession,
    df: DataFrame,
    spec: SparkTableSpec,
    catalog: str,
    schema_prefix: str,
) -> str:
    """Validate, attach comments, and write a Delta table to Unity Catalog (idempotent)."""
    validate_spark_schema(df, spec)
    schema_fqn = f"{catalog}.{schema_prefix}{spec.schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
    target = target_table_name(spec, catalog, schema_prefix)
    (
        spec.select_ordered(df)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    return target
