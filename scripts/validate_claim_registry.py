from __future__ import annotations

import json
from pathlib import Path

from pulsestream.contracts import load_source_registry

ROOT = Path(__file__).resolve().parents[1]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


registry = json.loads((ROOT / "evidence/claim_registry.json").read_text(encoding="utf-8"))
allowed = {"MEASURED_LOCAL_RESULT", "IMPLEMENTED_CONFIGURATION", "DESIGN_TARGET", "NOT_CLAIMED"}
require(registry["schema_version"] == 1 and registry["claims"], "invalid claim registry")
require(
    len({item["id"] for item in registry["claims"]}) == len(registry["claims"]),
    "claim IDs are not unique",
)
for item in registry["claims"]:
    require(
        set(item) == {"id", "classification", "value", "evidence"},
        f"invalid fields for {item.get('id')}",
    )
    require(item["classification"] in allowed, f"invalid class for {item['id']}")
    require((ROOT / item["evidence"]).is_file(), f"missing evidence for {item['id']}")
summary = json.loads((ROOT / "evidence/verified-local/summary.json").read_text(encoding="utf-8"))
require(
    summary["passed"] == summary["total"] == 23 and summary["failed"] == 0,
    "committed evidence is not green",
)
require(len(load_source_registry(ROOT / "config/sources.json")) == 50, "source count changed")
print("claim registry valid")
