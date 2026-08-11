from __future__ import annotations

import pytest

from pulsestream.publication import (
    OffsetRange,
    PublicationManifest,
    SnapshotRef,
    manifest_from_dict,
)

H = "a" * 64
TABLES = ("curated_event", "session_version", "engagement_aggregate", "correction_exception")


def manifest(parent=None):
    return PublicationManifest(
        "generation-1",
        parent,
        "LIVE",
        H,
        H,
        H,
        H,
        H,
        H,
        (OffsetRange(0, 0, 10),),
        (OffsetRange(0, 0, 10),),
        tuple(SnapshotRef(table, H) for table in TABLES),
        {"rows": 1},
    )


def test_manifest_round_trip_and_hash_is_deterministic():
    value = manifest()
    value.validate(TABLES)
    restored = manifest_from_dict(value.as_dict())
    assert restored == value
    assert restored.sha256 == value.sha256


def test_manifest_rejects_incomplete_bundle_and_unknown_fields():
    value = manifest()
    with pytest.raises(ValueError, match="snapshot set"):
        value.validate(TABLES + ("missing",))
    raw = value.as_dict() | {"unexpected": True}
    with pytest.raises(ValueError, match="missing or unexpected"):
        manifest_from_dict(raw)


def test_manifest_rejects_bad_generation_and_hash():
    value = manifest()
    with pytest.raises(ValueError, match="identifier"):
        (value.__class__(**(value.__dict__ | {"generation_id": "bad id"}))).validate(TABLES)
    with pytest.raises(ValueError, match="SHA-256"):
        (value.__class__(**(value.__dict__ | {"contract_sha256": "bad"}))).validate(TABLES)
