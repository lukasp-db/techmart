from datetime import date

from pyspark.sql import functions as F

from techmart.facts.gen import bounded_int, shifted_date_sk, uniform_hash


def test_uniform_hash_in_unit_interval_and_deterministic(spark):
    df = spark.range(0, 1000).withColumnRenamed("id", "k")
    out = df.withColumn("u", uniform_hash(F.col("k"), salt="s"))
    r = out.agg(F.min("u").alias("lo"), F.max("u").alias("hi")).first()
    assert 0.0 <= r["lo"] and r["hi"] < 1.0
    a = out.agg(F.sum("u")).first()[0]
    b = df.withColumn("u", uniform_hash(F.col("k"), salt="s")).agg(F.sum("u")).first()[0]
    assert a == b
    # different salt -> different stream
    c = df.withColumn("u", uniform_hash(F.col("k"), salt="other")).agg(F.sum("u")).first()[0]
    assert a != c


def test_bounded_int_inclusive_range(spark):
    df = spark.range(0, 2000).withColumnRenamed("id", "k")
    out = df.withColumn("v", bounded_int(F.col("k"), salt="q", lo=1, hi=5))
    r = out.agg(F.min("v").alias("lo"), F.max("v").alias("hi")).first()
    assert r["lo"] == 1 and r["hi"] == 5


def test_shifted_date_sk_clamps_to_max(spark):
    rows = [(date(2025, 12, 20),), (date(2025, 12, 31),)]
    df = spark.createDataFrame(rows, "d date")
    max_date = date(2025, 12, 31)
    out = df.withColumn("sk", shifted_date_sk(F.col("d"), F.lit(20), max_date))
    got = [r["sk"] for r in out.orderBy("d").collect()]
    # 2025-12-20 + 20d = 2026-01-09 -> clamped to 2025-12-31 -> 20251231
    # 2025-12-31 + 20d -> clamped to 20251231
    assert got == [20251231, 20251231]
    # a lag that stays in range is not clamped
    out2 = df.withColumn("sk", shifted_date_sk(F.col("d"), F.lit(5), max_date))
    assert out2.orderBy("d").first()["sk"] == 20251225
