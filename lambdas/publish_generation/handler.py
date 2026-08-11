"""Move the active bundle pointer with a DynamoDB compare-and-swap transaction."""

from __future__ import annotations

import json
import os
from typing import Any

import boto3

from pulsestream.aws_publication import publication_transaction


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    if set(event) != {"generation_id"}:
        raise ValueError("publication fields do not match contract")
    table = os.environ["GENERATION_TABLE"]
    client = boto3.client("dynamodb")
    candidate = client.get_item(
        TableName=table,
        Key={"generation_id": {"S": event["generation_id"]}},
        ConsistentRead=True,
    ).get("Item")
    pointer = client.get_item(
        TableName=table,
        Key={"generation_id": {"S": "ACTIVE#engagement"}},
        ConsistentRead=True,
    ).get("Item")
    if candidate is None or pointer is None:
        raise ValueError("candidate or active pointer does not exist")
    parent = candidate["parent_generation_id"]
    parent_generation_id = None if "NULL" in parent else parent["S"]
    operations = publication_transaction(
        table,
        event["generation_id"],
        parent_generation_id,
        candidate["manifest_sha256"]["S"],
        int(pointer["pointer_version"]["N"]),
    )
    client.transact_write_items(TransactItems=operations)
    return {
        "statusCode": 200,
        "body": json.dumps({"active_generation_id": event["generation_id"]}),
    }
