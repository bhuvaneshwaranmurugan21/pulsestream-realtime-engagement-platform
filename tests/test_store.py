from __future__ import annotations

from pathlib import Path

import pytest

from pulsestream.generator import generate_bronze_segments, load_envelopes
from pulsestream.store import PublicationConflict, PulseStore

KEY = b"test-key-which-is-at-least-thirty-two-bytes"
H = "b" * 64


def test_restart_idempotency_replay_and_publish_race(tmp_path: Path, registry, rules):
    bronze = generate_bronze_segments(registry, tmp_path / "bronze", 200)
    envelopes = load_envelopes(tmp_path / "bronze", bronze)
    store = PulseStore(tmp_path / "state.sqlite")
    with pytest.raises(RuntimeError, match="injected_failure"):
        store.process_envelopes(envelopes, registry, KEY, rules, fail_after_commits=37)
    assert store.process_envelopes(envelopes, registry, KEY, rules) == len(envelopes) - 37
    assert store.process_envelopes(envelopes, registry, KEY, rules) == 0
    assert store.ingestion_counts() == {
        "ACCEPTED_LATE": 1,
        "ACCEPTED_LIVE": 200,
        "DUPLICATE": 1,
        "QUARANTINED": 4,
    }
    args = dict(
        artifact_root=tmp_path / "publication",
        source_manifest_sha256=H,
        bronze_manifest_sha256=bronze["manifest_sha256"],
        implementation_sha256=H,
        contract_sha256=H,
        source_registry_sha256=H,
        rules=rules,
    )
    live = store.build_generation(
        generation_id="live-1", parent_generation_id=None, mode="LIVE", **args
    )
    store.publish("live-1")
    stale = store.build_generation(
        generation_id="stale-1", parent_generation_id=None, mode="LIVE", **args
    )
    with pytest.raises(PublicationConflict, match="stale_parent"):
        store.publish(stale.generation_id)
    replay = store.build_generation(
        generation_id="replay-1", parent_generation_id=live.generation_id, mode="REPLAY", **args
    )
    store.publish(replay.generation_id)
    assert replay.counts["late_events_included"] == 1
    assert store.exception_counts() == {"RESOLVED": 1}
    assert store.active_generation_id() == "replay-1"


def test_partition_gap_is_rejected(tmp_path: Path, registry, rules, valid_event):
    store = PulseStore(tmp_path / "state.sqlite")
    with pytest.raises(ValueError, match="partition_gap"):
        store.process_envelopes(
            [{"partition": 0, "offset": 2, "payload": valid_event}], registry, KEY, rules
        )
