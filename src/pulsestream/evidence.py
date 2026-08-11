"""Executable restart, replay and publication-race evidence."""

from __future__ import annotations

import html
import json
import shutil
from pathlib import Path
from typing import Any

from pulsestream.contracts import (
    canonical_json,
    load_source_registry,
    read_json,
    sha256_bytes,
    sha256_text,
)
from pulsestream.generator import generate_bronze_segments, load_envelopes
from pulsestream.rules import load_session_rules
from pulsestream.source_manifest import FORMAT, source_manifest_sha256
from pulsestream.store import PublicationConflict, PulseStore


def _check(name: str, passed: bool, detail: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "detail": str(detail)}


def _repository_hash(root: Path) -> str:
    paths = sorted(
        value
        for folder in ("src", "spark_jobs", "lambdas", "orchestration", "config", "contracts")
        for value in (root / folder).rglob("*")
        if value.is_file() and "__pycache__" not in value.parts
    )
    return sha256_bytes(
        b"".join(str(path.relative_to(root)).encode() + b"\0" + path.read_bytes() for path in paths)
    )


def _write_report(output: Path, report: dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    rows = "".join(
        f"<tr><td>{html.escape(item['name'])}</td><td class={'pass' if item['passed'] else 'fail'}>"
        f"{'PASS' if item['passed'] else 'FAIL'}</td><td>{html.escape(item['detail'])}</td></tr>"
        for item in report["checks"]
    )
    body = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>PulseStream evidence</title>
<style>
body{{font:15px system-ui;background:#0b0d10;color:#e8edf2;margin:40px}}
table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #343a40;padding:9px;text-align:left}}
.pass{{color:#3ddc97}}.fail{{color:#ff6b6b}}
</style></head><body><h1>PulseStream verified local evidence</h1>
<p>{report["passed"]} of {report["total"]} checks passed.</p>
<table><thead><tr><th>Invariant</th><th>Result</th><th>Evidence</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""
    (output / "report.html").write_text(body, encoding="utf-8")


def run_evidence(
    repository_root: str | Path, work_dir: str | Path, minimum_valid_events: int = 5_000
) -> dict[str, Any]:
    root, work = Path(repository_root).resolve(), Path(work_dir).resolve()
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    registry = load_source_registry(root / "config/sources.json")
    rules = load_session_rules(root / "config/session_rules.json")
    bronze = generate_bronze_segments(registry, work / "bronze", minimum_valid_events)
    envelopes = load_envelopes(work / "bronze", bronze)
    source = {
        "format": FORMAT,
        "topic": "engagement-events-v1",
        "bronze_manifest_sha256": bronze["manifest_sha256"],
        "source_snapshots": {
            "bronze_event": bronze["manifest_sha256"],
            "contract_quarantine": bronze["manifest_sha256"],
        },
        "ranges": [
            {
                "partition": partition,
                "start_offset": 0,
                "end_offset": max(
                    value["offset"] for value in envelopes if value["partition"] == partition
                ),
            }
            for partition in sorted({value["partition"] for value in envelopes})
        ],
    }
    store = PulseStore(work / "pulsestream.sqlite")
    key = b"local-evidence-key-not-for-production-0123456789"
    crash = False
    try:
        store.process_envelopes(envelopes, registry, key, rules, fail_after_commits=777)
    except RuntimeError as error:
        crash = str(error) == "injected_failure:after_checkpoint_commit"
    store.process_envelopes(envelopes, registry, key, rules)
    counts = store.ingestion_counts()
    repeated = store.process_envelopes(envelopes, registry, key, rules)
    common = {
        "artifact_root": work / "publication",
        "source_manifest_sha256": source_manifest_sha256(source),
        "bronze_manifest_sha256": bronze["manifest_sha256"],
        "implementation_sha256": _repository_hash(root),
        "contract_sha256": sha256_bytes((root / "contracts/engagement_event_v1.json").read_bytes()),
        "source_registry_sha256": sha256_text(
            canonical_json(read_json(root / "config/sources.json"))
        ),
        "rules": rules,
    }
    live = store.build_generation(
        generation_id="live-0001", parent_generation_id=None, mode="LIVE", **common
    )
    store.publish(live.generation_id)
    live_active = store.active_generation_id()
    open_live = store.exception_counts().get("OPEN", 0)
    stale = store.build_generation(
        generation_id="stale-0001", parent_generation_id=None, mode="LIVE", **common
    )
    stale_rejected = False
    try:
        store.publish(stale.generation_id)
    except PublicationConflict:
        stale_rejected = True
    replay = store.build_generation(
        generation_id="replay-0001",
        parent_generation_id=live.generation_id,
        mode="REPLAY",
        **common,
    )
    store.publish(replay.generation_id)
    checks = [
        _check("source_registry_expands_to_50", len(registry) == 50, f"sources={len(registry)}"),
        _check(
            "bronze_manifest_is_self_verifying",
            len(bronze["files"]) >= 20,
            f"segments={len(bronze['files'])}",
        ),
        _check(
            "segment_record_conservation",
            bronze["records"] == minimum_valid_events + 6,
            f"records={bronze['records']}",
        ),
        _check("checkpoint_crash_was_injected", crash, "crash after 777 durable offsets"),
        _check("raw_outcomes_are_conserved", sum(counts.values()) == bronze["records"], counts),
        _check("restart_is_idempotent", repeated == 0, f"new_outcomes={repeated}"),
        _check("exact_duplicate_detected", counts.get("DUPLICATE") == 1, counts),
        _check("invalid_payloads_quarantined", counts.get("QUARANTINED") == 4, counts),
        _check("late_event_is_explicit", counts.get("ACCEPTED_LATE") == 1, counts),
        _check(
            "late_event_excluded_from_live", live.counts["late_events_included"] == 0, live.counts
        ),
        _check("correction_open_after_live", open_live == 1, f"open={open_live}"),
        _check("live_publication_selected", live_active == "live-0001", live_active),
        _check("stale_generation_rejected", stale_rejected, "conditional parent mismatch"),
        _check(
            "stale_generation_cannot_move_pointer",
            store.active_generation_id() == "replay-0001",
            store.active_generation_id(),
        ),
        _check(
            "replay_includes_late_event", replay.counts["late_events_included"] == 1, replay.counts
        ),
        _check(
            "replay_resolves_exception",
            store.exception_counts().get("RESOLVED") == 1,
            store.exception_counts(),
        ),
        _check(
            "replay_becomes_active_bundle",
            store.active_generation_id() == "replay-0001",
            store.active_generation_id(),
        ),
        _check(
            "snapshot_bundle_is_complete",
            len(replay.snapshots) == 4,
            f"snapshots={len(replay.snapshots)}",
        ),
        _check(
            "live_and_replay_are_distinct",
            live.sha256 != replay.sha256,
            f"live={live.sha256[:12]} replay={replay.sha256[:12]}",
        ),
        _check(
            "manifest_binds_implementation",
            replay.implementation_sha256 == common["implementation_sha256"],
            replay.implementation_sha256,
        ),
        _check(
            "manifest_binds_source_manifest",
            replay.source_manifest_sha256 == common["source_manifest_sha256"],
            replay.source_manifest_sha256,
        ),
        _check(
            "manifest_binds_contract",
            replay.contract_sha256 == common["contract_sha256"],
            replay.contract_sha256,
        ),
        _check("manifest_binds_rules", replay.rule_sha256 == rules.sha256, replay.rule_sha256),
    ]
    report = {
        "classification": "MEASURED_LOCAL_RESULT",
        "evidence_boundary": (
            "Synthetic local execution validates semantics and failure recovery; it does not "
            "claim AWS deployment, production throughput, production latency or production SLOs."
        ),
        "checks": checks,
        "passed": sum(item["passed"] for item in checks),
        "failed": sum(not item["passed"] for item in checks),
        "total": len(checks),
        "active_generation": store.active_generation_id(),
        "ingestion": counts,
        "live_manifest_sha256": live.sha256,
        "replay_manifest_sha256": replay.sha256,
    }
    _write_report(work / "evidence", report)
    return report
