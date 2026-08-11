from __future__ import annotations

import json
import struct
from pathlib import Path

from pulsestream.contracts import load_source_registry
from pulsestream.rules import load_session_rules

ROOT = Path(__file__).resolve().parents[1]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def png_dimensions(path: Path) -> tuple[int, int]:
    header = path.read_bytes()[:24]
    if header[:8] != b"\x89PNG\r\n\x1a\n" or header[12:16] != b"IHDR":
        raise ValueError("architecture asset is not a PNG")
    return struct.unpack(">II", header[16:24])


registry = load_source_registry(ROOT / "config/sources.json")
rules = load_session_rules(ROOT / "config/session_rules.json")
require(len(registry) == 50, "architecture requires the documented 50-source registry")
require(
    set(rules.required_snapshot_tables)
    == {"curated_event", "session_version", "engagement_aggregate", "correction_exception"},
    "publication bundle contract changed",
)
width, height = png_dimensions(ROOT / "architecture/pulsestream-engagement-architecture.png")
require((width, height) == (2048, 813), "LinkedIn architecture asset dimensions changed")
print(json.dumps({"sources": len(registry), "snapshots": 4, "image": [width, height]}))
