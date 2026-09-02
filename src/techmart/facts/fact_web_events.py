"""fact_web_events: sessionized clickstream (standalone, high-volume)."""
from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession, functions as F

from ..config import TechmartConfig
from ..spark.framework import SparkColumn, SparkTableSpec
from .gen import uniform_hash
from .lookups import date_seasonality_weights

FACT_WEB_EVENTS_SPEC = SparkTableSpec(
    schema="core",
    name="fact_web_events",
    grain="one row per web clickstream event",
    columns=[
        SparkColumn("session_id", "long", "Degenerate session id", nullable=False),
        SparkColumn("event_number", "int", "Event sequence within the session", nullable=False),
        SparkColumn("date_sk", "long", "Event date FK (dim_date)", is_key=True, nullable=False),
        SparkColumn("customer_sk", "long", "Customer FK (dim_customer); null for anonymous", is_key=True),
        SparkColumn("product_sk", "long", "Product FK (dim_product); null for non-product events", is_key=True),
        SparkColumn("channel_sk", "long", "Channel FK (dim_channel)", is_key=True, nullable=False),
        SparkColumn("event_ts", "timestamp", "Event timestamp", nullable=False),
        SparkColumn("event_type", "string", "page_view/search/add_to_cart/checkout/purchase", nullable=False),
        SparkColumn("search_term", "string", "Search term for search events; null otherwise"),
        SparkColumn("device_type", "string", "Desktop/Mobile/Tablet"),
        SparkColumn("referrer", "string", "Traffic referrer"),
        SparkColumn("cart_value", "double", "Cart value for cart/checkout/purchase events; null otherwise"),
    ],
)

_AVG_EVENTS = 3.9  # weighted mean of the session-length distribution below
_EVENT_TYPES = ["page_view", "search", "add_to_cart", "checkout", "purchase"]
_EVENT_WEIGHTS = [55, 20, 15, 6, 4]
_DEVICES = ["Desktop", "Mobile", "Tablet"]
_REFERRERS = ["Organic", "Paid-Search", "Email", "Social", "Direct"]
_SEARCH_TERMS = ["laptop", "tv", "headphones", "gpu", "router", "monitor", "tablet", "camera"]


def build_fact_web_events(
    spark: SparkSession,
    config: TechmartConfig,
    *,
    dim_date: DataFrame,
    dim_counts: dict,
    rows: int | None = None,
    seed: int | None = None,
) -> DataFrame:
    target = rows if rows is not None else config.scale_profile.web_events_target
    seed = seed if seed is not None else config.seed
    num_sessions = max(1, round(target / _AVG_EVENTS))
    partitions = max(1, min(256, num_sessions // 1_000_000))

    date_sks, weights = date_seasonality_weights(dim_date)

    header = (
        dg.DataGenerator(
            spark, name="web_session", rows=num_sessions, partitions=partitions,
            randomSeed=seed, randomSeedMethod="hash_fieldname",
        )
        .withIdOutput()
        .withColumn("date_sk", "long", values=date_sks, weights=weights, random=True)
        .withColumn("channel_sk", "long", values=[2, 3], weights=[60, 40], random=True)
        # device_num / referrer_num are consumed in a post-build withColumn (F.col),
        # so they must NOT be omitted from the built frame; they are dropped below.
        .withColumn("device_num", "int", minValue=1, maxValue=3, random=True)
        .withColumn("referrer_num", "int", minValue=1, maxValue=5, random=True)
        .withColumn("num_events", "int", values=[1, 2, 3, 4, 5, 6, 7, 8], weights=[22, 22, 18, 14, 10, 7, 4, 3], random=True)
        .build()
    )
    header = header.withColumn("session_id", (F.col("id") + F.lit(1)).cast("long")).drop("id")

    devices_arr = F.array(*[F.lit(d) for d in _DEVICES])
    refs_arr = F.array(*[F.lit(r) for r in _REFERRERS])
    terms_arr = F.array(*[F.lit(t) for t in _SEARCH_TERMS])

    header = (
        header
        .withColumn("device_type", F.element_at(devices_arr, F.col("device_num")))
        .withColumn("referrer", F.element_at(refs_arr, F.col("referrer_num")))
        # ~35% of sessions are anonymous (null customer_sk), stable per session.
        .withColumn(
            "customer_sk",
            F.when(
                uniform_hash(F.col("session_id"), salt="anon") < F.lit(0.35), F.lit(None).cast("long"),
            ).otherwise((F.floor(uniform_hash(F.col("session_id"), salt="cust") * F.lit(dim_counts["customer"])) + F.lit(1)).cast("long")),
        )
        .drop("device_num", "referrer_num")
    )

    lines = header.withColumn("event_number", F.explode(F.sequence(F.lit(1), F.col("num_events")))).drop("num_events")

    keyed = (F.col("session_id"), F.col("event_number"))
    # Weighted event type via a cumulative-weight bucket over a uniform draw.
    # Thresholds are iterated in reverse so the smallest threshold becomes the
    # outermost when(), giving the correct mutually-exclusive bucket assignment.
    u_type = uniform_hash(*keyed, salt="etype")
    total = float(sum(_EVENT_WEIGHTS))
    cum = 0.0
    thresholds: list[float] = []
    for w in _EVENT_WEIGHTS[:-1]:
        cum += w / total
        thresholds.append(cum)
    type_col = F.lit(_EVENT_TYPES[-1])
    for thresh, name in zip(reversed(thresholds), reversed(_EVENT_TYPES[:-1])):
        type_col = F.when(u_type < F.lit(thresh), F.lit(name)).otherwise(type_col)

    date_col = F.to_date(F.col("date_sk").cast("string"), "yyyyMMdd")
    secs = (F.pmod(F.hash(*keyed, F.lit("ts")), F.lit(86400))).cast("long")

    df = (
        lines
        .withColumn("event_type", type_col)
        .withColumn("event_ts", F.timestamp_seconds(F.unix_timestamp(date_col) + secs))
        .withColumn(
            "product_sk",
            F.when(
                F.col("event_type").isin("add_to_cart", "checkout", "purchase"),
                (F.floor(uniform_hash(*keyed, salt="prod") * F.lit(dim_counts["product"])) + F.lit(1)).cast("long"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "search_term",
            F.when(F.col("event_type") == F.lit("search"), F.element_at(terms_arr, (F.pmod(F.hash(*keyed, F.lit("term")), F.lit(len(_SEARCH_TERMS))) + F.lit(1)))).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "cart_value",
            F.when(
                F.col("event_type").isin("add_to_cart", "checkout", "purchase"),
                F.round(F.lit(20.0) + uniform_hash(*keyed, salt="cart") * F.lit(1980.0), 2),
            ).otherwise(F.lit(None).cast("double")),
        )
    )
    return FACT_WEB_EVENTS_SPEC.select_ordered(df)
