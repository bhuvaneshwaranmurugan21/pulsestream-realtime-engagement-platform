"""Exact source-snapshot and Kafka-range manifest."""

from __future__ import annotations

import re
from typing import Any

from pulsestream.contracts import canonical_json, sha256_bytes

HEX64 = re.compile(r"^[a-f0-9]{64}$")
SNAPSHOT = re.compile(r"^(?:[a-f0-9]{64}|[1-9][0-9]{0,19})$")
TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
FORMAT = "PULSESTREAM_OFFSET_MANIFEST_V1"


def validate_source_manifest(
    raw: Any,
) -> tuple[list[dict[str, int]], str, str, dict[str, str]]:
    required = {"format", "topic", "bronze_manifest_sha256", "source_snapshots", "ranges"}
    if not isinstance(raw, dict) or set(raw) != required or raw["format"] != FORMAT:
        raise ValueError("invalid source manifest shape")
    topic, digest, snapshots = raw["topic"], raw["bronze_manifest_sha256"], raw["source_snapshots"]
    if not isinstance(topic, str) or TOPIC.fullmatch(topic) is None:
        raise ValueError("invalid topic")
    if not isinstance(digest, str) or HEX64.fullmatch(digest) is None:
        raise ValueError("invalid bronze checksum")
    if (
        not isinstance(snapshots, dict)
        or set(snapshots) != {"bronze_event", "contract_quarantine"}
        or any(not isinstance(v, str) or SNAPSHOT.fullmatch(v) is None for v in snapshots.values())
    ):
        raise ValueError("source snapshots are incomplete")
    if not isinstance(raw["ranges"], list) or not raw["ranges"]:
        raise ValueError("source ranges are empty")
    ranges: list[dict[str, int]] = []
    partitions: set[int] = set()
    for value in raw["ranges"]:
        if not isinstance(value, dict) or set(value) != {"partition", "start_offset", "end_offset"}:
            raise ValueError("invalid source range")
        if any(isinstance(v, bool) or not isinstance(v, int) for v in value.values()):
            raise ValueError("source offsets must be integers")
        partition, start, end = value["partition"], value["start_offset"], value["end_offset"]
        if partition < 0 or start < 0 or end < start or partition in partitions:
            raise ValueError("source offsets are invalid or repeated")
        partitions.add(partition)
        ranges.append(dict(value))
    return sorted(ranges, key=lambda v: v["partition"]), digest, topic, dict(snapshots)


def source_manifest_sha256(raw: dict[str, Any]) -> str:
    validate_source_manifest(raw)
    return sha256_bytes((canonical_json(raw) + "\n").encode())
