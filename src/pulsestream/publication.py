"""Immutable multi-table snapshot bundle contract."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pulsestream.contracts import canonical_json, sha256_text

HEX64 = re.compile(r"^[a-f0-9]{64}$")
IDENTIFIER = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
SNAPSHOT = re.compile(r"^(?:[a-f0-9]{64}|[1-9][0-9]{0,19})$")


@dataclass(frozen=True)
class OffsetRange:
    partition: int
    start_offset: int
    end_offset: int

    def as_dict(self) -> dict[str, int]:
        return {
            "partition": self.partition,
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
        }


@dataclass(frozen=True)
class SnapshotRef:
    table: str
    snapshot_id: str

    def as_dict(self) -> dict[str, str]:
        return {"table": self.table, "snapshot_id": self.snapshot_id}


@dataclass(frozen=True)
class PublicationManifest:
    generation_id: str
    parent_generation_id: str | None
    mode: str
    implementation_sha256: str
    contract_sha256: str
    source_registry_sha256: str
    rule_sha256: str
    source_manifest_sha256: str
    bronze_manifest_sha256: str
    input_offsets: tuple[OffsetRange, ...]
    offsets: tuple[OffsetRange, ...]
    snapshots: tuple[SnapshotRef, ...]
    counts: dict[str, int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "generation_id": self.generation_id,
            "parent_generation_id": self.parent_generation_id,
            "mode": self.mode,
            "implementation_sha256": self.implementation_sha256,
            "contract_sha256": self.contract_sha256,
            "source_registry_sha256": self.source_registry_sha256,
            "rule_sha256": self.rule_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
            "bronze_manifest_sha256": self.bronze_manifest_sha256,
            "input_offsets": [v.as_dict() for v in self.input_offsets],
            "offsets": [v.as_dict() for v in self.offsets],
            "snapshots": [v.as_dict() for v in self.snapshots],
            "counts": dict(sorted(self.counts.items())),
        }

    @property
    def sha256(self) -> str:
        return sha256_text(canonical_json(self.as_dict()))

    def validate(self, required_tables: tuple[str, ...]) -> None:
        if IDENTIFIER.fullmatch(self.generation_id) is None:
            raise ValueError("invalid generation identifier")
        if (
            self.parent_generation_id is not None
            and IDENTIFIER.fullmatch(self.parent_generation_id) is None
        ):
            raise ValueError("invalid parent identifier")
        if self.mode not in {"LIVE", "REPLAY"}:
            raise ValueError("unsupported publication mode")
        hashes = (
            self.implementation_sha256,
            self.contract_sha256,
            self.source_registry_sha256,
            self.rule_sha256,
            self.source_manifest_sha256,
            self.bronze_manifest_sha256,
        )
        if any(HEX64.fullmatch(v) is None for v in hashes):
            raise ValueError("publication hashes must be SHA-256")
        for label, ranges in (("input", self.input_offsets), ("frontier", self.offsets)):
            if not ranges or any(
                v.partition < 0 or v.start_offset < 0 or v.end_offset < v.start_offset
                for v in ranges
            ):
                raise ValueError(f"{label} offsets are incomplete")
            if len({v.partition for v in ranges}) != len(ranges):
                raise ValueError(f"{label} offsets repeat a partition")
        if {v.table for v in self.snapshots} != set(required_tables) or len(self.snapshots) != len(
            required_tables
        ):
            raise ValueError("snapshot set does not match required tables")
        if any(SNAPSHOT.fullmatch(v.snapshot_id) is None for v in self.snapshots):
            raise ValueError("invalid snapshot identifier")
        if not self.counts or any(isinstance(v, bool) or v < 0 for v in self.counts.values()):
            raise ValueError("invalid publication counts")


def manifest_from_dict(raw: dict[str, Any]) -> PublicationManifest:
    required = {
        "generation_id",
        "parent_generation_id",
        "mode",
        "implementation_sha256",
        "contract_sha256",
        "source_registry_sha256",
        "rule_sha256",
        "source_manifest_sha256",
        "bronze_manifest_sha256",
        "input_offsets",
        "offsets",
        "snapshots",
        "counts",
    }
    if set(raw) != required:
        raise ValueError("manifest contains missing or unexpected fields")
    try:
        return PublicationManifest(
            generation_id=str(raw["generation_id"]),
            parent_generation_id=None
            if raw["parent_generation_id"] is None
            else str(raw["parent_generation_id"]),
            mode=str(raw["mode"]),
            implementation_sha256=str(raw["implementation_sha256"]),
            contract_sha256=str(raw["contract_sha256"]),
            source_registry_sha256=str(raw["source_registry_sha256"]),
            rule_sha256=str(raw["rule_sha256"]),
            source_manifest_sha256=str(raw["source_manifest_sha256"]),
            bronze_manifest_sha256=str(raw["bronze_manifest_sha256"]),
            input_offsets=tuple(OffsetRange(**v) for v in raw["input_offsets"]),
            offsets=tuple(OffsetRange(**v) for v in raw["offsets"]),
            snapshots=tuple(SnapshotRef(**v) for v in raw["snapshots"]),
            counts={str(k): int(v) for k, v in raw["counts"].items()},
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("manifest has invalid field types") from error
