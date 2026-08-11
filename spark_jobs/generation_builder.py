"""Build and describe one reproducible, four-snapshot Iceberg generation."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pulsestream.contracts import (
    canonical_json,
    load_source_registry,
    read_json,
    sha256_bytes,
    sha256_text,
)
from pulsestream.publication import (
    OffsetRange,
    PublicationManifest,
    SnapshotRef,
    manifest_from_dict,
)
from pulsestream.rules import load_session_rules
from pulsestream.source_manifest import source_manifest_sha256, validate_source_manifest
from pulsestream.uri import read_json_uri, write_json_uri
from spark_jobs.branching import advance_frontier, branch_table, generation_branch_name
from spark_jobs.transforms import (
    classify_lateness,
    engagement_aggregates,
    identity_gate_against_parent,
    identity_gate_batch,
    sessionize_batch,
)


def select_ranges(events: Any, ranges: list[dict[str, int]]) -> Any:
    from functools import reduce
    from operator import or_

    from pyspark.sql import functions as function

    predicates = [
        (function.col("kafka_partition") == value["partition"])
        & function.col("kafka_offset").between(value["start_offset"], value["end_offset"])
        for value in ranges
    ]
    return events.where(reduce(or_, predicates))


def snapshot_frame(spark: Any, table: str, snapshot_id: str) -> Any:
    if not snapshot_id.isdigit():
        raise ValueError("production Iceberg snapshot IDs must be numeric")
    return spark.read.option("snapshot-id", snapshot_id).table(table)


def create_candidate_branch(
    spark: Any, table: str, branch: str, parent_snapshot_id: str | None
) -> None:
    # table and branch are validated by branch_table before entering SQL identifier context.
    branch_table(table, branch)
    suffix = "" if parent_snapshot_id is None else f" AS OF VERSION {int(parent_snapshot_id)}"
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS {branch}{suffix} RETAIN 7 DAYS")


def write_candidate(frame: Any, table: str, branch: str) -> None:
    from pyspark.sql import functions as function

    frame.writeTo(branch_table(table, branch)).overwrite(function.lit(True))


def branch_snapshot_id(spark: Any, table: str, branch: str) -> str:
    branch_table(table, branch)
    # Both identifier values passed strict allowlist regexes in branch_table.
    row = spark.sql(
        f"SELECT snapshot_id FROM {table}.refs WHERE name = '{branch}'"  # nosec B608
    ).first()
    if row is None:
        raise ValueError(f"candidate branch {branch} has no snapshot")
    return str(row["snapshot_id"])


def _snapshot_map(parent: PublicationManifest | None) -> dict[str, str]:
    return {} if parent is None else {value.table: value.snapshot_id for value in parent.snapshots}


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as function

    parser = argparse.ArgumentParser()
    parser.add_argument("--generation-id", required=True)
    parser.add_argument("--mode", choices=("LIVE", "REPLAY"), required=True)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--parent-publication-manifest", default="")
    parser.add_argument("--output-publication-manifest", required=True)
    parser.add_argument("--implementation-sha256", required=True)
    parser.add_argument("--rules", default="config/session_rules.json")
    parser.add_argument("--sources", default="config/sources.json")
    parser.add_argument("--contract", default="contracts/gateway_engagement_event_v1.json")
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--curated-table", required=True)
    parser.add_argument("--session-table", required=True)
    parser.add_argument("--aggregate-table", required=True)
    parser.add_argument("--correction-table", required=True)
    args = parser.parse_args()

    source = read_json_uri(args.source_manifest)
    ranges, bronze_digest, _, source_snapshots = validate_source_manifest(source)
    parent = (
        manifest_from_dict(read_json_uri(args.parent_publication_manifest))
        if args.parent_publication_manifest
        else None
    )
    if args.mode == "REPLAY" and parent is None:
        raise ValueError("REPLAY requires a published parent manifest")
    if parent is not None:
        parent.validate(load_session_rules(args.rules).required_snapshot_tables)
    rules = load_session_rules(args.rules)
    load_source_registry(args.sources)
    spark = SparkSession.builder.appName(f"pulsestream-{args.generation_id}").getOrCreate()

    bronze = snapshot_frame(spark, args.bronze_table, source_snapshots["bronze_event"])
    selected = select_ranges(bronze, ranges)
    parent_snapshots = _snapshot_map(parent)
    parent_curated = (
        snapshot_frame(spark, args.curated_table, parent_snapshots["curated_event"])
        if parent is not None
        else None
    )
    parent_corrections = (
        snapshot_frame(spark, args.correction_table, parent_snapshots["correction_exception"])
        if parent is not None
        else None
    )
    prior_watermarks = (
        parent_curated.groupBy("kafka_partition").agg(
            function.max("event_time").alias("prior_max_event_time")
        )
        if parent_curated is not None and args.mode == "LIVE"
        else None
    )
    classified = classify_lateness(selected, rules, prior_watermarks)
    first, duplicate_batch, conflict_batch = identity_gate_batch(classified)
    parent_identity = None
    if parent_curated is not None and parent_corrections is not None:
        correction_identity = parent_corrections.where("identity_sha256 IS NOT NULL")
        if args.mode == "REPLAY":
            # An OPEN late event is intentionally eligible for correction, not a duplicate.
            correction_identity = correction_identity.where(
                "NOT (status = 'OPEN' AND reason = 'beyond_live_watermark')"
            )
        parent_identity = parent_curated.select("event_id", "identity_sha256").unionByName(
            correction_identity.select("event_id", "identity_sha256")
        )
    accepted, duplicate_parent, conflict_parent = identity_gate_against_parent(
        first, parent_identity
    )
    duplicates = duplicate_batch.unionByName(duplicate_parent)
    conflicts = conflict_batch.unionByName(conflict_parent)
    new_curated = accepted if args.mode == "REPLAY" else accepted.where("NOT beyond_watermark")
    curated = new_curated if parent_curated is None else parent_curated.unionByName(new_curated)
    new_late = accepted.where("beyond_watermark")
    base_corrections = parent_corrections
    if parent_corrections is not None:
        if args.mode == "REPLAY":
            resolved_ids = (
                accepted.select("event_id")
                .distinct()
                .withColumn("resolved_by_replay", function.lit(True))
            )
            base_corrections = (
                parent_corrections.join(resolved_ids, "event_id", "left")
                .withColumn(
                    "status",
                    function.when(
                        function.col("resolved_by_replay")
                        & (function.col("status") == "OPEN")
                        & (function.col("reason") == "beyond_live_watermark"),
                        "RESOLVED",
                    ).otherwise(function.col("status")),
                )
                .drop("resolved_by_replay")
            )
        new_late = new_late.join(
            parent_corrections.select("event_id").distinct(), "event_id", "left_anti"
        )
    new_corrections = new_late.selectExpr(
        "event_id",
        "identity_sha256",
        "'beyond_live_watermark' AS reason",
        f"'{args.mode}' AS generation_mode",
        "CASE WHEN '" + args.mode + "' = 'REPLAY' THEN 'RESOLVED' ELSE 'OPEN' END AS status",
        "kafka_partition",
        "kafka_offset",
    ).unionByName(
        conflicts.selectExpr(
            "event_id",
            "identity_sha256",
            "'identity_payload_conflict' AS reason",
            f"'{args.mode}' AS generation_mode",
            "'OPEN' AS status",
            "kafka_partition",
            "kafka_offset",
        )
    )
    corrections = (
        new_corrections
        if base_corrections is None
        else base_corrections.unionByName(new_corrections)
    )
    sessions = sessionize_batch(curated, rules)
    aggregates = engagement_aggregates(curated, sessions)

    tables = {
        "curated_event": (args.curated_table, curated),
        "session_version": (args.session_table, sessions),
        "engagement_aggregate": (args.aggregate_table, aggregates),
        "correction_exception": (args.correction_table, corrections),
    }
    branch = generation_branch_name(args.generation_id)
    snapshots: list[SnapshotRef] = []
    for logical_name in rules.required_snapshot_tables:
        table, frame = tables[logical_name]
        create_candidate_branch(spark, table, branch, parent_snapshots.get(logical_name))
        write_candidate(frame, table, branch)
        snapshots.append(SnapshotRef(logical_name, branch_snapshot_id(spark, table, branch)))

    input_offsets = tuple(OffsetRange(**value) for value in ranges)
    parent_frontier = () if parent is None else parent.offsets
    frontier = advance_frontier(input_offsets, parent_frontier, args.mode)
    manifest = PublicationManifest(
        generation_id=args.generation_id,
        parent_generation_id=None if parent is None else parent.generation_id,
        mode=args.mode,
        implementation_sha256=args.implementation_sha256,
        contract_sha256=sha256_bytes(Path(args.contract).read_bytes()),
        source_registry_sha256=sha256_text(canonical_json(read_json(args.sources))),
        rule_sha256=rules.sha256,
        source_manifest_sha256=source_manifest_sha256(source),
        bronze_manifest_sha256=bronze_digest,
        input_offsets=input_offsets,
        offsets=frontier,
        snapshots=tuple(snapshots),
        counts={
            "curated_events": curated.count(),
            "sessions": sessions.count(),
            "aggregates": aggregates.count(),
            "corrections": corrections.count(),
            "duplicates": duplicates.count(),
        },
    )
    manifest.validate(rules.required_snapshot_tables)
    write_json_uri(args.output_publication_manifest, manifest.as_dict())


if __name__ == "__main__":
    main()
