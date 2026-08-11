"""Checkpoint-safe local reference and conditional snapshot-bundle publication."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from collections.abc import Iterable
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pulsestream.contracts import (
    ContractError,
    Source,
    canonical_json,
    identity_fingerprint,
    redact_payload,
    sha256_text,
    validate_event,
)
from pulsestream.publication import (
    OffsetRange,
    PublicationManifest,
    SnapshotRef,
    manifest_from_dict,
)
from pulsestream.rules import SessionRules

SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoint(
  partition_id INTEGER PRIMARY KEY, last_offset INTEGER NOT NULL, max_event_time TEXT
);
CREATE TABLE IF NOT EXISTS ingestion_outcome(
  partition_id INTEGER, offset_value INTEGER, event_id TEXT, outcome TEXT, reason TEXT,
  PRIMARY KEY(partition_id,offset_value)
);
CREATE TABLE IF NOT EXISTS seen_event(event_id TEXT PRIMARY KEY,identity_sha256 TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accepted_event(
  event_id TEXT PRIMARY KEY, source_id TEXT, user_token TEXT, event_type TEXT,
  event_time TEXT, arrival_time TEXT, consent_state TEXT, page_id TEXT, campaign_id TEXT,
  beyond_watermark INTEGER, first_partition INTEGER, first_offset INTEGER
);
CREATE TABLE IF NOT EXISTS quarantine(
  partition_id INTEGER, offset_value INTEGER, event_id TEXT, reason TEXT,
  redacted_payload_json TEXT, PRIMARY KEY(partition_id,offset_value)
);
CREATE TABLE IF NOT EXISTS correction_exception(
  event_id TEXT PRIMARY KEY, reason TEXT, status TEXT, opened_partition INTEGER,
  opened_offset INTEGER, resolved_generation_id TEXT
);
CREATE TABLE IF NOT EXISTS generation(
  generation_id TEXT PRIMARY KEY, parent_generation_id TEXT, mode TEXT, status TEXT,
  manifest_json TEXT, manifest_sha256 TEXT
);
CREATE TABLE IF NOT EXISTS publication_pointer(
  scope TEXT PRIMARY KEY, active_generation_id TEXT, manifest_sha256 TEXT,
  version INTEGER NOT NULL
);
INSERT OR IGNORE INTO publication_pointer VALUES('engagement',NULL,NULL,0);
"""


class PublicationConflict(RuntimeError):
    pass


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value)


class PulseStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        with closing(self.connect()) as connection:
            connection.executescript(SCHEMA)
            connection.commit()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def process_envelopes(
        self,
        envelopes: Iterable[dict[str, Any]],
        registry: dict[str, Source],
        token_key: bytes,
        rules: SessionRules,
        fail_after_commits: int | None = None,
    ) -> int:
        committed = 0
        for envelope in envelopes:
            partition, offset = int(envelope["partition"]), int(envelope["offset"])
            with closing(self.connect()) as connection:
                existing = connection.execute(
                    "SELECT 1 FROM ingestion_outcome WHERE partition_id=? AND offset_value=?",
                    (partition, offset),
                ).fetchone()
                if existing is not None:
                    continue
                checkpoint = connection.execute(
                    "SELECT last_offset,max_event_time FROM checkpoint WHERE partition_id=?",
                    (partition,),
                ).fetchone()
                expected = 0 if checkpoint is None else int(checkpoint["last_offset"]) + 1
                if offset != expected:
                    raise ValueError(
                        f"partition_gap:{partition}:expected={expected}:actual={offset}"
                    )
                max_event_time = None if checkpoint is None else checkpoint["max_event_time"]
                event_time = self._process_one(
                    connection,
                    partition,
                    offset,
                    envelope["payload"],
                    registry,
                    token_key,
                    rules,
                    max_event_time,
                )
                next_max = max_event_time
                if event_time is not None and (
                    next_max is None or _time(event_time) > _time(next_max)
                ):
                    next_max = event_time
                connection.execute(
                    "INSERT INTO checkpoint VALUES(?,?,?) ON CONFLICT(partition_id) "
                    "DO UPDATE SET last_offset=excluded.last_offset,"
                    "max_event_time=excluded.max_event_time",
                    (partition, offset, next_max),
                )
                connection.commit()
            committed += 1
            if fail_after_commits is not None and committed == fail_after_commits:
                raise RuntimeError("injected_failure:after_checkpoint_commit")
        return committed

    def _process_one(
        self,
        connection: sqlite3.Connection,
        partition: int,
        offset: int,
        raw: Any,
        registry: dict[str, Source],
        token_key: bytes,
        rules: SessionRules,
        max_event_time: str | None,
    ) -> str | None:
        try:
            event = validate_event(raw, registry, token_key)
        except ContractError as error:
            event_id = raw.get("event_id") if isinstance(raw, dict) else None
            connection.execute(
                "INSERT INTO quarantine VALUES(?,?,?,?,?)",
                (
                    partition,
                    offset,
                    event_id,
                    error.reason,
                    canonical_json(redact_payload(raw if isinstance(raw, dict) else {})),
                ),
            )
            connection.execute(
                "INSERT INTO ingestion_outcome VALUES(?,?,?,?,?)",
                (partition, offset, event_id, "QUARANTINED", error.reason),
            )
            return None
        fingerprint = identity_fingerprint(raw)
        existing = connection.execute(
            "SELECT identity_sha256 FROM seen_event WHERE event_id=?", (event.event_id,)
        ).fetchone()
        if existing is not None:
            conflict = existing["identity_sha256"] != fingerprint
            outcome = "QUARANTINED" if conflict else "DUPLICATE"
            reason = "identity_payload_conflict" if conflict else "business_identity_replay"
            if conflict:
                connection.execute(
                    "INSERT INTO quarantine VALUES(?,?,?,?,?)",
                    (
                        partition,
                        offset,
                        event.event_id,
                        reason,
                        canonical_json(redact_payload(raw)),
                    ),
                )
            connection.execute(
                "INSERT INTO ingestion_outcome VALUES(?,?,?,?,?)",
                (partition, offset, event.event_id, outcome, reason),
            )
            return event.event_time
        late = False
        if max_event_time is not None:
            late = _time(event.event_time) < _time(max_event_time) - timedelta(
                minutes=rules.live_watermark_minutes
            )
        connection.execute("INSERT INTO seen_event VALUES(?,?)", (event.event_id, fingerprint))
        connection.execute(
            "INSERT INTO accepted_event VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                event.event_id,
                event.source_id,
                event.user_token,
                event.event_type,
                event.event_time,
                event.arrival_time,
                event.consent_state,
                event.page_id,
                event.campaign_id,
                int(late),
                partition,
                offset,
            ),
        )
        if late:
            connection.execute(
                "INSERT INTO correction_exception VALUES(?,?, 'OPEN', ?, ?, NULL)",
                (event.event_id, "beyond_live_watermark", partition, offset),
            )
        connection.execute(
            "INSERT INTO ingestion_outcome VALUES(?,?,?,?,?)",
            (
                partition,
                offset,
                event.event_id,
                "ACCEPTED_LATE" if late else "ACCEPTED_LIVE",
                "beyond_live_watermark" if late else "contract_valid",
            ),
        )
        return event.event_time

    def ingestion_counts(self) -> dict[str, int]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT outcome,COUNT(*) count FROM ingestion_outcome GROUP BY outcome"
            ).fetchall()
        return {str(row["outcome"]): int(row["count"]) for row in rows}

    def checkpoint_offsets(self) -> tuple[OffsetRange, ...]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT partition_id,last_offset FROM checkpoint ORDER BY partition_id"
            ).fetchall()
        return tuple(OffsetRange(int(row[0]), 0, int(row[1])) for row in rows)

    def _events(self, include_late: bool) -> list[dict[str, Any]]:
        query = "SELECT * FROM accepted_event"
        if not include_late:
            query += " WHERE beyond_watermark=0"
        query += " ORDER BY event_time,event_id"
        with closing(self.connect()) as connection:
            return [dict(row) for row in connection.execute(query).fetchall()]

    @staticmethod
    def _sessions(events: list[dict[str, Any]], rules: SessionRules) -> list[dict[str, Any]]:
        by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for event in events:
            if event["consent_state"] == "analytics_allowed":
                by_user[event["user_token"]].append(event)
        result: list[dict[str, Any]] = []
        for user, values in sorted(by_user.items()):
            values.sort(key=lambda event: (event["event_time"], event["event_id"]))
            groups: list[list[dict[str, Any]]] = []
            for event in values:
                if not groups or _time(event["event_time"]) - _time(
                    groups[-1][-1]["event_time"]
                ) > timedelta(minutes=rules.session_gap_minutes):
                    groups.append([])
                groups[-1].append(event)
            for group in groups:
                result.append(
                    {
                        "session_id": sha256_text(
                            f"{user}:{group[0]['event_time']}:{group[0]['event_id']}"
                        ),
                        "user_token": user,
                        "started_at": group[0]["event_time"],
                        "ended_at": group[-1]["event_time"],
                        "event_count": len(group),
                        "first_event_id": group[0]["event_id"],
                        "last_event_id": group[-1]["event_id"],
                    }
                )
        return result

    @staticmethod
    def _aggregates(
        events: list[dict[str, Any]], sessions: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        counts: Counter[tuple[str, str]] = Counter()
        users: dict[tuple[str, str], set[str]] = defaultdict(set)
        session_counts: Counter[str] = Counter(value["started_at"][:10] for value in sessions)
        for event in events:
            if event["consent_state"] != "analytics_allowed":
                continue
            key = (event["event_time"][:10], event["event_type"])
            counts[key] += 1
            users[key].add(event["user_token"])
        return [
            {
                "event_date": key[0],
                "event_type": key[1],
                "event_count": count,
                "unique_users": len(users[key]),
                "sessions_started_on_date": session_counts[key[0]],
            }
            for key, count in sorted(counts.items())
        ]

    def _corrections(self, mode: str, generation_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM correction_exception ORDER BY event_id"
                ).fetchall()
            ]
        for row in rows:
            if mode == "REPLAY" and row["status"] == "OPEN":
                row["status"] = "RESOLVED"
                row["resolved_generation_id"] = generation_id
        return rows

    def build_generation(
        self,
        *,
        generation_id: str,
        parent_generation_id: str | None,
        mode: str,
        artifact_root: str | Path,
        source_manifest_sha256: str,
        bronze_manifest_sha256: str,
        implementation_sha256: str,
        contract_sha256: str,
        source_registry_sha256: str,
        rules: SessionRules,
    ) -> PublicationManifest:
        events = self._events(mode == "REPLAY")
        sessions = self._sessions(events, rules)
        aggregates = self._aggregates(events, sessions)
        corrections = self._corrections(mode, generation_id)
        tables = {
            "curated_event": events,
            "session_version": sessions,
            "engagement_aggregate": aggregates,
            "correction_exception": corrections,
        }
        if set(tables) != set(rules.required_snapshot_tables):
            raise ValueError("configured and implemented snapshot tables diverged")
        root = Path(artifact_root) / "candidates" / generation_id
        root.mkdir(parents=True, exist_ok=False)
        snapshots: list[SnapshotRef] = []
        for table in rules.required_snapshot_tables:
            body = "".join(canonical_json(row) + "\n" for row in tables[table])
            digest = sha256_text(body)
            (root / f"{table}-{digest}.jsonl").write_text(body, encoding="utf-8")
            snapshots.append(SnapshotRef(table, digest))
        offsets = self.checkpoint_offsets()
        manifest = PublicationManifest(
            generation_id,
            parent_generation_id,
            mode,
            implementation_sha256,
            contract_sha256,
            source_registry_sha256,
            rules.sha256,
            source_manifest_sha256,
            bronze_manifest_sha256,
            offsets,
            offsets,
            tuple(snapshots),
            {
                "curated_events": len(events),
                "sessions": len(sessions),
                "aggregates": len(aggregates),
                "open_corrections": sum(row["status"] == "OPEN" for row in corrections),
                "late_events_included": sum(int(row["beyond_watermark"]) for row in events),
            },
        )
        manifest.validate(rules.required_snapshot_tables)
        with closing(self.connect()) as connection:
            connection.execute(
                "INSERT INTO generation VALUES(?,?,?,'CANDIDATE',?,?)",
                (
                    generation_id,
                    parent_generation_id,
                    mode,
                    canonical_json(manifest.as_dict()),
                    manifest.sha256,
                ),
            )
            connection.commit()
        return manifest

    def publish(self, generation_id: str) -> PublicationManifest:
        with closing(self.connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            generation = connection.execute(
                "SELECT * FROM generation WHERE generation_id=?", (generation_id,)
            ).fetchone()
            pointer = connection.execute(
                "SELECT * FROM publication_pointer WHERE scope='engagement'"
            ).fetchone()
            if generation is None or generation["status"] != "CANDIDATE":
                raise ValueError("generation is not publishable")
            if pointer["active_generation_id"] != generation["parent_generation_id"]:
                connection.execute(
                    "UPDATE generation SET status='SUPERSEDED' WHERE generation_id=?",
                    (generation_id,),
                )
                connection.commit()
                raise PublicationConflict("stale_parent")
            connection.execute(
                "UPDATE publication_pointer SET active_generation_id=?,manifest_sha256=?,"
                "version=version+1 WHERE scope='engagement'",
                (generation_id, generation["manifest_sha256"]),
            )
            connection.execute(
                "UPDATE generation SET status='PUBLISHED' WHERE generation_id=?", (generation_id,)
            )
            if generation["mode"] == "REPLAY":
                connection.execute(
                    "UPDATE correction_exception SET status='RESOLVED',resolved_generation_id=? "
                    "WHERE status='OPEN'",
                    (generation_id,),
                )
            connection.commit()
            return manifest_from_dict(json.loads(generation["manifest_json"]))

    def active_generation_id(self) -> str | None:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT active_generation_id FROM publication_pointer WHERE scope='engagement'"
            ).fetchone()
        value = row[0]
        return None if value is None else str(value)

    def exception_counts(self) -> dict[str, int]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT status,COUNT(*) FROM correction_exception GROUP BY status"
            ).fetchall()
        return {str(row[0]): int(row[1]) for row in rows}
