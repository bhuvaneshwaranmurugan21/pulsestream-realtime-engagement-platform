"""Canonical Spark transformations; all constants come from versioned rules."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from pulsestream.rules import SessionRules


def gateway_contract_invalid(registered_sources: tuple[str, ...]) -> Any:
    from pyspark.sql import functions as function

    required = (
        "schema_version",
        "event_id",
        "source_id",
        "user_token",
        "event_type",
        "event_time",
        "arrival_time",
        "consent_state",
    )
    allowed = required + ("page_id", "campaign_id")
    unknown = (
        function.size(
            function.array_except(
                function.map_keys("raw_map"), function.array(*(function.lit(v) for v in allowed))
            )
        )
        > 0
    )
    missing = function.lit(False)
    for field in required:
        missing = missing | function.col(field).isNull()
    return (
        missing
        | function.col("raw_map").isNull()
        | unknown
        | (function.col("schema_version") != "1.0")
        | ~function.col("event_id").rlike(r"^[A-Za-z0-9._:-]{1,128}$")
        | ~function.col("source_id").isin(*registered_sources)
        | ~function.col("user_token").rlike(r"^[a-f0-9]{64}$")
        | ~function.col("event_type").isin(
            "page_view", "search", "product_view", "add_to_cart", "checkout"
        )
        | ~function.col("consent_state").isin("analytics_allowed", "essential_only")
        | ~function.col("event_time").rlike(r"(?:[zZ]|[+-][0-9]{2}:[0-9]{2})$")
        | ~function.col("arrival_time").rlike(r"(?:[zZ]|[+-][0-9]{2}:[0-9]{2})$")
    )


def classify_lateness(
    events: DataFrame, rules: SessionRules, prior_watermarks: DataFrame | None = None
) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as function

    window = (
        Window.partitionBy("kafka_partition")
        .orderBy("kafka_offset")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    prior = function.max("event_time").over(window)
    result = events
    if prior_watermarks is not None:
        result = result.join(prior_watermarks, "kafka_partition", "left")
        prior = function.greatest(prior, function.col("prior_max_event_time"))
    return result.withColumn(
        "beyond_watermark",
        function.when(
            prior.isNotNull()
            & (
                function.col("event_time").cast("long")
                < prior.cast("long") - rules.live_watermark_minutes * 60
            ),
            True,
        ).otherwise(False),
    ).drop("prior_max_event_time")


def identity_gate_batch(events: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    from pyspark.sql import Window
    from pyspark.sql import functions as function

    fields = (
        "schema_version",
        "event_id",
        "source_id",
        "user_token",
        "event_type",
        "event_time",
        "consent_state",
        "page_id",
        "campaign_id",
    )
    fingerprint = function.sha2(
        function.to_json(function.struct(*(function.col(v) for v in fields))), 256
    )
    order = Window.partitionBy("event_id").orderBy("kafka_partition", "kafka_offset")
    full = order.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    values = (
        events.withColumn("identity_sha256", fingerprint)
        .withColumn("first_identity_sha256", function.first("identity_sha256").over(full))
        .withColumn("identity_arrival_number", function.row_number().over(order))
    )
    accepted = values.where("identity_arrival_number = 1").drop(
        "first_identity_sha256", "identity_arrival_number"
    )
    duplicate = values.where(
        "identity_arrival_number > 1 AND identity_sha256 = first_identity_sha256"
    )
    conflict = values.where(
        "identity_arrival_number > 1 AND identity_sha256 <> first_identity_sha256"
    )
    return accepted, duplicate, conflict


def identity_gate_against_parent(
    events: DataFrame, parent_identities: DataFrame | None
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split first-in-batch events against identity state from the published parent."""
    from pyspark.sql import functions as function

    if parent_identities is None:
        return events, events.limit(0), events.limit(0)
    parent = parent_identities.select(
        "event_id", function.col("identity_sha256").alias("parent_identity_sha256")
    ).dropDuplicates(["event_id"])
    compared = events.join(parent, "event_id", "left")
    new = compared.where("parent_identity_sha256 IS NULL").drop("parent_identity_sha256")
    duplicate = compared.where("identity_sha256 = parent_identity_sha256").drop(
        "parent_identity_sha256"
    )
    conflict = compared.where(
        "parent_identity_sha256 IS NOT NULL AND identity_sha256 <> parent_identity_sha256"
    ).drop("parent_identity_sha256")
    return new, duplicate, conflict


def sessionize_batch(events: DataFrame, rules: SessionRules) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as function

    eligible = events.where("consent_state = 'analytics_allowed'")
    order = Window.partitionBy("user_token").orderBy("event_time", "event_id")
    previous = function.lag("event_time").over(order)
    starts = function.when(
        previous.isNull()
        | (
            function.col("event_time").cast("long") - previous.cast("long")
            > rules.session_gap_minutes * 60
        ),
        1,
    ).otherwise(0)
    numbered = eligible.withColumn("session_start", starts).withColumn(
        "session_number",
        function.sum("session_start").over(
            order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )
    grouped = numbered.groupBy("user_token", "session_number").agg(
        function.min("event_time").alias("started_at"),
        function.max("event_time").alias("ended_at"),
        function.count("event_id").alias("event_count"),
        function.min_by("event_id", function.struct("event_time", "event_id")).alias(
            "first_event_id"
        ),
        function.max_by("event_id", function.struct("event_time", "event_id")).alias(
            "last_event_id"
        ),
    )
    return grouped.withColumn(
        "session_id",
        function.sha2(
            function.concat_ws(
                ":", "user_token", function.col("started_at").cast("string"), "first_event_id"
            ),
            256,
        ),
    ).select(
        "session_id",
        "user_token",
        "started_at",
        "ended_at",
        "event_count",
        "first_event_id",
        "last_event_id",
    )


def engagement_aggregates(events: DataFrame, sessions: DataFrame) -> DataFrame:
    from pyspark.sql import functions as function

    eligible = events.where("consent_state = 'analytics_allowed'").withColumn(
        "event_date", function.to_date("event_time")
    )
    metrics = eligible.groupBy("event_date", "event_type").agg(
        function.count("event_id").alias("event_count"),
        function.count_distinct("user_token").alias("unique_users"),
    )
    session_metrics = sessions.groupBy(function.to_date("started_at").alias("event_date")).agg(
        function.count("session_id").alias("sessions_started_on_date")
    )
    return metrics.join(session_metrics, "event_date", "left").fillna(
        0, subset=["sessions_started_on_date"]
    )
