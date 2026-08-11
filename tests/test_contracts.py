from __future__ import annotations

import pytest

from pulsestream.contracts import (
    ContractError,
    canonical_json,
    redact_payload,
    source_registry_from_dict,
    tokenize_user,
    validate_event,
)

KEY = b"a" * 32


def test_valid_event_is_normalized_and_tokenized(registry, valid_event):
    event = validate_event(valid_event, registry, KEY)
    assert event.event_id == "evt-1"
    assert event.user_token == tokenize_user("PERSON@example.test", KEY)
    assert event.event_time.endswith("+00:00")


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"schema_version": "2.0"}, "schema_version"),
        ({"source_id": "missing"}, "unknown_source"),
        ({"event_type": "click"}, "invalid_enum"),
        ({"event_time": "2026-08-11T09:00:00"}, "timezone_required:event_time"),
        ({"surprise": "field"}, "contract_fields"),
    ],
)
def test_invalid_contracts_are_rejected(registry, valid_event, change, reason):
    with pytest.raises(ContractError, match=reason):
        validate_event(valid_event | change, registry, KEY)


def test_nested_secret_is_rejected_and_redacted(registry, valid_event):
    raw = valid_event | {"metadata": {"authorization": "secret"}}
    with pytest.raises(ContractError, match="prohibited_fields:authorization"):
        validate_event(raw, registry, KEY)
    assert redact_payload(raw)["metadata"]["authorization"] == "<redacted>"


def test_key_length_and_registry_shape_are_strict(valid_event, registry):
    with pytest.raises(ContractError, match="token_key_too_short"):
        validate_event(valid_event, registry, b"short")
    with pytest.raises(ContractError, match="unsupported_source_registry"):
        source_registry_from_dict({})
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
