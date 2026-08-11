"""Small JSON URI boundary shared by EMR jobs and Lambda controllers."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pulsestream.contracts import canonical_json


def s3_location(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError("invalid S3 URI")
    return parsed.netloc, parsed.path.lstrip("/")


def read_json_uri(uri: str) -> Any:
    if uri.startswith("s3://"):
        bucket, key = s3_location(uri)
        boto3 = importlib.import_module("boto3")
        response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        if int(response.get("ContentLength", 0)) > 1_048_576:
            raise ValueError("JSON control document exceeds 1 MiB")
        return json.loads(response["Body"].read())
    parsed = urlparse(uri)
    if parsed.scheme not in {"", "file"}:
        raise ValueError("unsupported JSON URI")
    return json.loads(Path(parsed.path).read_text(encoding="utf-8"))


def write_json_uri(uri: str, value: Any) -> None:
    body = (canonical_json(value) + "\n").encode()
    if uri.startswith("s3://"):
        bucket, key = s3_location(uri)
        boto3 = importlib.import_module("boto3")
        boto3.client("s3").put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
        return
    parsed = urlparse(uri)
    if parsed.scheme not in {"", "file"}:
        raise ValueError("unsupported JSON URI")
    path = Path(parsed.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
