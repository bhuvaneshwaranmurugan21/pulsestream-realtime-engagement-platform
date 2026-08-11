"""One canonical rule document shared by local and Spark implementations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pulsestream.contracts import canonical_json, read_json, sha256_text


@dataclass(frozen=True)
class SessionRules:
    version: int
    session_gap_minutes: int
    live_watermark_minutes: int
    target_file_size_mib: int
    required_snapshot_tables: tuple[str, ...]
    sha256: str


def session_rules_from_dict(raw: Any) -> SessionRules:
    required = {
        "version",
        "session_gap_minutes",
        "live_watermark_minutes",
        "target_file_size_mib",
        "required_snapshot_tables",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise ValueError("invalid session-rule shape")
    numeric = tuple(raw[name] for name in required - {"required_snapshot_tables"})
    if any(isinstance(v, bool) or not isinstance(v, int) or v < 1 for v in numeric):
        raise ValueError("rule numbers must be positive integers")
    tables = raw["required_snapshot_tables"]
    if not isinstance(tables, list) or len(tables) < 4 or len(set(tables)) != len(tables):
        raise ValueError("required snapshot tables are invalid")
    return SessionRules(
        raw["version"],
        raw["session_gap_minutes"],
        raw["live_watermark_minutes"],
        raw["target_file_size_mib"],
        tuple(tables),
        sha256_text(canonical_json(raw)),
    )


def load_session_rules(path: str | Path) -> SessionRules:
    return session_rules_from_dict(read_json(path))
