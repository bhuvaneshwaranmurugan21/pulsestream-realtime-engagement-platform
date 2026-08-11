"""Register a completed, manifest-bound candidate exactly once."""

from __future__ import annotations

import json
import os
from typing import Any

import boto3

from pulsestream.aws_publication import registration_item
from pulsestream.publication import manifest_from_dict
from pulsestream.uri import read_json_uri, s3_location


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    table = os.environ["GENERATION_TABLE"]
    if set(event) != {"generation_id", "mode", "manifest_uri"}:
        raise ValueError("candidate request fields do not match contract")
    bucket, _ = s3_location(event["manifest_uri"])
    if bucket != os.environ["MANIFEST_BUCKET"]:
        raise ValueError("manifest bucket is not approved")
    manifest = manifest_from_dict(read_json_uri(event["manifest_uri"]))
    manifest.validate(tuple(os.environ["REQUIRED_TABLES"].split(",")))
    if manifest.generation_id != event["generation_id"] or manifest.mode != event["mode"]:
        raise ValueError("candidate request does not match manifest identity")
    item = registration_item(
        {
            "generation_id": manifest.generation_id,
            "parent_generation_id": manifest.parent_generation_id,
            "mode": manifest.mode,
            "manifest_uri": event["manifest_uri"],
            "manifest_sha256": manifest.sha256,
        }
    )
    boto3.client("dynamodb").put_item(
        TableName=table,
        Item=item,
        ConditionExpression="attribute_not_exists(generation_id)",
    )
    return {
        "statusCode": 201,
        "body": json.dumps(
            {
                "generation_id": manifest.generation_id,
                "manifest_sha256": manifest.sha256,
                "status": "CANDIDATE",
            }
        ),
    }
