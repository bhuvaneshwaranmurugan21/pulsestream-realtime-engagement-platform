"""Producer contracts, privacy boundary and canonical hashing helpers."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

EVENT_TYPES = ("page_view", "search", "product_view", "add_to_cart", "checkout")
CONSENT_STATES = ("analytics_allowed", "essential_only")
IDENTIFIER = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
PROHIBITED_FIELDS = frozenset(
    {"password", "authorization", "access_token", "refresh_token", "cookie", "pan", "cvv", "cvc"}
)
REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "source_id",
        "user_id",
        "event_type",
        "event_time",
        "arrival_time",
        "consent_state",
    }
)
ALLOWED_FIELDS = REQUIRED_FIELDS | {"page_id", "campaign_id"}
IDENTITY_FIELDS = tuple(sorted(ALLOWED_FIELDS - {"arrival_time"}))


class ContractError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Source:
    source_id: str
    family: str
    owner: str
    contract: str
    lateness_minutes: int


@dataclass(frozen=True)
class EngagementEvent:
    event_id: str
    source_id: str
    user_token: str
    event_type: str
    event_time: str
    arrival_time: str
    consent_state: str
    page_id: str | None
    campaign_id: str | None


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode())


def read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def source_registry_from_dict(raw: Any) -> dict[str, Source]:
    if not isinstance(raw, dict) or set(raw) != {"version", "families"} or raw["version"] != 1:
        raise ContractError("unsupported_source_registry")
    if not isinstance(raw["families"], list):
        raise ContractError("invalid_source_families")
    registry: dict[str, Source] = {}
    required = {"family", "instances", "owner", "contract", "lateness_minutes"}
    for item in raw["families"]:
        if not isinstance(item, dict) or set(item) != required:
            raise ContractError("invalid_source_family")
        count, lateness = item["instances"], item["lateness_minutes"]
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 1 for v in (count, lateness)):
            raise ContractError("invalid_source_numbers")
        for number in range(1, count + 1):
            source_id = f"{item['family']}-{number:02d}"
            if source_id in registry:
                raise ContractError("duplicate_source_id")
            registry[source_id] = Source(
                source_id, str(item["family"]), str(item["owner"]), str(item["contract"]), lateness
            )
    if not registry:
        raise ContractError("empty_source_registry")
    return registry


def load_source_registry(path: str | Path) -> dict[str, Source]:
    return source_registry_from_dict(read_json(path))


def reject_prohibited_fields(raw: dict[str, Any]) -> None:
    found: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key.casefold() in PROHIBITED_FIELDS:
                    found.add(key.casefold())
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(raw)
    if found:
        raise ContractError("prohibited_fields:" + ",".join(sorted(found)))


def redact_payload(raw: dict[str, Any]) -> dict[str, Any]:
    sensitive = PROHIBITED_FIELDS | {"user_id", "user_email"}

    def redact(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                k: "<redacted>" if k.casefold() in sensitive else redact(v)
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [redact(v) for v in value]
        return value

    return dict(redact(raw))


def tokenize_user(value: str, key: bytes) -> str:
    if len(key) < 32:
        raise ContractError("token_key_too_short")
    return hmac.new(key, value.strip().lower().encode(), hashlib.sha256).hexdigest()


def identity_fingerprint(raw: dict[str, Any]) -> str:
    return sha256_text(canonical_json({k: raw.get(k) for k in IDENTITY_FIELDS if k in raw}))


def _timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ContractError(f"invalid_{field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ContractError(f"invalid_{field}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ContractError(f"timezone_required:{field}")
    return parsed.isoformat()


def validate_event(raw: Any, registry: dict[str, Source], key: bytes) -> EngagementEvent:
    if not isinstance(raw, dict):
        raise ContractError("payload_not_object")
    reject_prohibited_fields(raw)
    if set(raw) - ALLOWED_FIELDS or not set(raw) >= REQUIRED_FIELDS:
        raise ContractError("contract_fields")
    if raw["schema_version"] != "1.0":
        raise ContractError("schema_version")
    for name in ("event_id", "source_id"):
        if not isinstance(raw[name], str) or IDENTIFIER.fullmatch(raw[name]) is None:
            raise ContractError(f"invalid_{name}")
    if raw["source_id"] not in registry:
        raise ContractError("unknown_source")
    if raw["event_type"] not in EVENT_TYPES or raw["consent_state"] not in CONSENT_STATES:
        raise ContractError("invalid_enum")
    if not isinstance(raw["user_id"], str) or not raw["user_id"]:
        raise ContractError("invalid_user_id")
    return EngagementEvent(
        raw["event_id"],
        raw["source_id"],
        tokenize_user(raw["user_id"], key),
        raw["event_type"],
        _timestamp(raw["event_time"], "event_time"),
        _timestamp(raw["arrival_time"], "arrival_time"),
        raw["consent_state"],
        raw.get("page_id"),
        raw.get("campaign_id"),
    )
