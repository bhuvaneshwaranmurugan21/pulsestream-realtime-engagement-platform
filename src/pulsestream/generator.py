"""Deterministic batched bronze fixture with explicit failure cases."""

from __future__ import annotations

import json
import random
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pulsestream.contracts import Source, canonical_json, sha256_bytes


def _event(number: int, source_ids: tuple[str, ...], base: datetime) -> dict[str, Any]:
    event_types = ("page_view", "search", "product_view", "add_to_cart", "checkout")
    event_time = base + timedelta(seconds=number * 7)
    return {
        "schema_version": "1.0",
        "event_id": f"event-{number:08d}",
        "source_id": source_ids[number % len(source_ids)],
        "user_id": f"user-{number % 2000:05d}",
        "event_type": event_types[number % len(event_types)],
        "event_time": event_time.isoformat(),
        "arrival_time": (event_time + timedelta(seconds=3)).isoformat(),
        "consent_state": "essential_only" if number % 37 == 0 else "analytics_allowed",
        "page_id": f"page-{number % 200}",
        "campaign_id": None if number % 5 else f"campaign-{number % 40}",
    }


def generate_bronze_segments(
    registry: dict[str, Source], output_dir: str | Path, minimum_valid_events: int = 5_000
) -> dict[str, Any]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=False)
    # A fixed seed is required for reproducible synthetic failure evidence.
    rng = random.Random(20260811)  # nosec B311
    sources = tuple(sorted(registry))
    base = datetime(2026, 6, 1, tzinfo=UTC)
    raw = [_event(number, sources, base) for number in range(minimum_valid_events)]
    duplicate = dict(raw[17])
    conflict = dict(raw[23]) | {"page_id": "conflicting-page"}
    missing = dict(raw[31])
    missing.pop("event_type")
    prohibited = dict(raw[41]) | {"context": {"cookie": "secret"}}
    unknown = dict(raw[51]) | {"source_id": "unknown-99"}
    late = _event(minimum_valid_events + 1, sources, base) | {
        "event_id": "late-event-0001",
        "event_time": (base - timedelta(days=2)).isoformat(),
        "arrival_time": (base + timedelta(days=1)).isoformat(),
    }
    raw.extend((duplicate, conflict, missing, prohibited, unknown, late))

    offsets: dict[int, int] = defaultdict(int)
    envelopes: list[dict[str, Any]] = []
    injection_partitions = {
        minimum_valid_events: 17,
        minimum_valid_events + 1: 23,
        minimum_valid_events + 2: 7,
        minimum_valid_events + 3: 17,
        minimum_valid_events + 4: 3,
        minimum_valid_events + 5: 0,
    }
    for number, payload in enumerate(raw):
        partition = injection_partitions.get(number, number % 24)
        envelope = {"partition": partition, "offset": offsets[partition], "payload": payload}
        offsets[partition] += 1
        envelopes.append(envelope)
    # Preserve per-partition ordering while varying segment composition deterministically.
    rng.shuffle(envelopes)
    envelopes.sort(key=lambda value: (value["partition"], value["offset"]))

    files: list[dict[str, Any]] = []
    segment_size = max(1, (len(envelopes) + 23) // 24)
    for index in range(0, len(envelopes), segment_size):
        values = envelopes[index : index + segment_size]
        body = "".join(canonical_json(value) + "\n" for value in values).encode()
        name = f"segment-{index // segment_size:04d}.jsonl"
        (root / name).write_bytes(body)
        files.append(
            {
                "path": name,
                "record_count": len(values),
                "sha256": sha256_bytes(body),
            }
        )
    manifest: dict[str, Any] = {
        "format": "PULSESTREAM_BRONZE_SEGMENTS_V1",
        "files": files,
        "records": len(envelopes),
    }
    manifest["manifest_sha256"] = sha256_bytes((canonical_json(manifest) + "\n").encode())
    (root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def load_envelopes(root: str | Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    base = Path(root)
    result: list[dict[str, Any]] = []
    for item in manifest["files"]:
        body = (base / item["path"]).read_bytes()
        if sha256_bytes(body) != item["sha256"]:
            raise ValueError("bronze segment checksum mismatch")
        result.extend(json.loads(line) for line in body.splitlines())
    return sorted(result, key=lambda value: (value["partition"], value["offset"]))
