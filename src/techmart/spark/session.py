from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "techmart", *, local_partitions: int = 4) -> SparkSession:
    """Return a SparkSession suitable for both serverless and local use.

    On Databricks serverless a session already exists and is reused unchanged.
    Locally (tests / laptop) a small ``local[2]`` session is built. A stale
    ``SPARK_HOME`` pointing at a removed distribution would shadow pyspark's
    bundled runtime, so it is dropped before building.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    home = os.environ.get("SPARK_HOME")
    if home and not os.path.isdir(home):
        os.environ.pop("SPARK_HOME", None)

    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(local_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
