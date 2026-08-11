"""Validated DynamoDB requests for generation registration and atomic publication."""

from __future__ import annotations

import re
from typing import Any

from pulsestream.publication import HEX64, IDENTIFIER

MODE = re.compile(r"^(LIVE|REPLAY)$")


def _text(value: Any, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"invalid {label}")
    return value


def registration_item(request: dict[str, Any]) -> dict[str, dict[str, str]]:
    required = {"generation_id", "parent_generation_id", "mode", "manifest_uri", "manifest_sha256"}
    if set(request) != required:
        raise ValueError("registration fields do not match contract")
    generation = _text(request["generation_id"], "generation_id", IDENTIFIER)
    parent = request["parent_generation_id"]
    if parent is not None:
        parent = _text(parent, "parent_generation_id", IDENTIFIER)
    mode = _text(request["mode"], "mode", MODE)
    digest = _text(request["manifest_sha256"], "manifest_sha256", HEX64)
    uri = request["manifest_uri"]
    if not isinstance(uri, str) or not uri.startswith("s3://") or len(uri) > 1024:
        raise ValueError("invalid manifest_uri")
    item = {
        "generation_id": {"S": generation},
        "mode": {"S": mode},
        "status": {"S": "CANDIDATE"},
        "manifest_uri": {"S": uri},
        "manifest_sha256": {"S": digest},
    }
    if parent is not None:
        item["parent_generation_id"] = {"S": parent}
    else:
        item["parent_generation_id"] = {"NULL": "true"}
    return item


def publication_transaction(
    table: str,
    generation_id: str,
    parent_generation_id: str | None,
    manifest_sha256: str,
    expected_pointer_version: int,
) -> list[dict[str, Any]]:
    _text(generation_id, "generation_id", IDENTIFIER)
    if parent_generation_id is not None:
        _text(parent_generation_id, "parent_generation_id", IDENTIFIER)
    _text(manifest_sha256, "manifest_sha256", HEX64)
    if isinstance(expected_pointer_version, bool) or expected_pointer_version < 0:
        raise ValueError("invalid expected_pointer_version")
    pointer_condition = "pointer_version=:expected_version AND "
    pointer_values: dict[str, dict[str, Any]] = {
        ":generation": {"S": generation_id},
        ":digest": {"S": manifest_sha256},
        ":expected_version": {"N": str(expected_pointer_version)},
        ":next_version": {"N": str(expected_pointer_version + 1)},
    }
    if parent_generation_id is None:
        pointer_condition += "attribute_not_exists(active_generation_id)"
    else:
        pointer_condition += "active_generation_id=:parent"
        pointer_values[":parent"] = {"S": parent_generation_id}
    return [
        {
            "Update": {
                "TableName": table,
                "Key": {"generation_id": {"S": generation_id}},
                "UpdateExpression": "SET #status=:published",
                "ConditionExpression": "#status = :candidate AND manifest_sha256 = :digest",
                "ExpressionAttributeNames": {"#status": "status"},
                "ExpressionAttributeValues": {
                    ":candidate": {"S": "CANDIDATE"},
                    ":published": {"S": "PUBLISHED"},
                    ":digest": {"S": manifest_sha256},
                },
            }
        },
        {
            "Update": {
                "TableName": table,
                "Key": {"generation_id": {"S": "ACTIVE#engagement"}},
                "UpdateExpression": (
                    "SET active_generation_id=:generation, manifest_sha256=:digest, "
                    "pointer_version=:next_version"
                ),
                "ConditionExpression": pointer_condition,
                "ExpressionAttributeValues": pointer_values,
            }
        },
    ]
