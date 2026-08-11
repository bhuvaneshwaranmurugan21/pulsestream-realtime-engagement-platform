from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")

from spark_jobs.transforms import (  # noqa: E402
    classify_lateness,
    engagement_aggregates,
    identity_gate_against_parent,
    identity_gate_batch,
    sessionize_batch,
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pulsestream-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_event_time_identity_session_and_aggregate_semantics(spark, rules):
    base = datetime(2026, 8, 11, 10, tzinfo=UTC)
    rows = [
        ("e1", 0, 0, "s1", "u1", "page_view", base, "analytics_allowed", "p1", "c1"),
        ("e1", 0, 1, "s1", "u1", "page_view", base, "analytics_allowed", "p1", "c1"),
        ("e1", 0, 2, "s1", "u1", "page_view", base, "analytics_allowed", "p2", "c1"),
        (
            "late",
            0,
            3,
            "s1",
            "u1",
            "checkout",
            base - timedelta(hours=2),
            "analytics_allowed",
            "p3",
            None,
        ),
        (
            "e2",
            0,
            4,
            "s1",
            "u1",
            "checkout",
            base + timedelta(minutes=35),
            "analytics_allowed",
            "p4",
            None,
        ),
    ]
    columns = [
        "event_id",
        "kafka_partition",
        "kafka_offset",
        "source_id",
        "user_token",
        "event_type",
        "event_time",
        "consent_state",
        "page_id",
        "campaign_id",
    ]
    events = spark.createDataFrame(rows, columns).withColumn(
        "schema_version", pyspark.sql.functions.lit("1.0")
    )
    classified = classify_lateness(events, rules)
    assert classified.where("beyond_watermark").select("event_id").first()[0] == "late"
    accepted, duplicates, conflicts = identity_gate_batch(classified)
    assert accepted.count() == 3
    assert duplicates.count() == 1
    assert conflicts.count() == 1
    new, previous_duplicate, previous_conflict = identity_gate_against_parent(
        accepted,
        accepted.where("event_id = 'e1'").select("event_id", "identity_sha256"),
    )
    assert new.count() == 2
    assert previous_duplicate.count() == 1
    assert previous_conflict.count() == 0
    sessions = sessionize_batch(accepted.where("NOT beyond_watermark"), rules)
    assert sessions.count() == 2
    aggregates = engagement_aggregates(accepted.where("NOT beyond_watermark"), sessions)
    assert aggregates.agg({"event_count": "sum"}).first()[0] == 2
