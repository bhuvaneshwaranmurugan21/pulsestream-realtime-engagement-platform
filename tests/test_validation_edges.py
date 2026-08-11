from __future__ import annotations

from pathlib import Path

import pytest

from pulsestream.aws_publication import publication_transaction, registration_item
from pulsestream.contracts import ContractError, load_source_registry, source_registry_from_dict
from pulsestream.generator import generate_bronze_segments, load_envelopes
from pulsestream.publication import (
    OffsetRange,
    PublicationManifest,
    SnapshotRef,
    manifest_from_dict,
)
from pulsestream.rules import session_rules_from_dict
from pulsestream.source_manifest import FORMAT, source_manifest_sha256, validate_source_manifest

H = "d" * 64
TABLES = ("curated_event", "session_version", "engagement_aggregate", "correction_exception")
ROOT = Path(__file__).resolve().parents[1]


def valid_source_manifest():
    return {
        "format": FORMAT,
        "topic": "engagement-events-v1",
        "bronze_manifest_sha256": H,
        "source_snapshots": {"bronze_event": H, "contract_quarantine": "123"},
        "ranges": [{"partition": 0, "start_offset": 0, "end_offset": 2}],
    }


def test_source_manifest_is_canonical_and_strict():
    raw = valid_source_manifest()
    ranges, digest, topic, snapshots = validate_source_manifest(raw)
    assert ranges[0]["end_offset"] == 2
    assert digest == H and topic == "engagement-events-v1"
    assert snapshots["bronze_event"] == H
    assert len(source_manifest_sha256(raw)) == 64
    for change in (
        {"topic": "bad topic"},
        {"bronze_manifest_sha256": "bad"},
        {"source_snapshots": {}},
        {"ranges": []},
        {"ranges": [{"partition": 0, "start_offset": 3, "end_offset": 2}]},
    ):
        with pytest.raises(ValueError):
            validate_source_manifest(raw | change)


def test_generator_rejects_tampered_segment(tmp_path: Path):
    registry = load_source_registry(ROOT / "config/sources.json")
    manifest = generate_bronze_segments(registry, tmp_path / "bronze", 60)
    target = tmp_path / "bronze" / manifest["files"][0]["path"]
    target.write_bytes(target.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="checksum"):
        load_envelopes(tmp_path / "bronze", manifest)


def test_rules_registry_and_publication_type_edges():
    with pytest.raises(ValueError, match="shape"):
        session_rules_from_dict({})
    with pytest.raises(ContractError, match="source_families"):
        source_registry_from_dict({"version": 1, "families": {}})
    with pytest.raises(ValueError, match="field types"):
        manifest_from_dict(
            {
                "generation_id": "g1",
                "parent_generation_id": None,
                "mode": "LIVE",
                "implementation_sha256": H,
                "contract_sha256": H,
                "source_registry_sha256": H,
                "rule_sha256": H,
                "source_manifest_sha256": H,
                "bronze_manifest_sha256": H,
                "input_offsets": ["invalid"],
                "offsets": [],
                "snapshots": [],
                "counts": {},
            }
        )
    value = PublicationManifest(
        "g1",
        None,
        "BAD",
        H,
        H,
        H,
        H,
        H,
        H,
        (OffsetRange(0, 0, 1),),
        (OffsetRange(0, 0, 1),),
        tuple(SnapshotRef(name, H) for name in TABLES),
        {"rows": 1},
    )
    with pytest.raises(ValueError, match="mode"):
        value.validate(TABLES)


def test_aws_publication_contract_edges():
    with pytest.raises(ValueError, match="fields"):
        registration_item({})
    with pytest.raises(ValueError, match="parent"):
        registration_item(
            {
                "generation_id": "g1",
                "parent_generation_id": "bad parent",
                "mode": "LIVE",
                "manifest_uri": "s3://bucket/manifest.json",
                "manifest_sha256": H,
            }
        )
    transaction = publication_transaction("table", "g2", "g1", H, 4)
    values = transaction[1]["Update"]["ExpressionAttributeValues"]
    assert values[":parent"] == {"S": "g1"}
