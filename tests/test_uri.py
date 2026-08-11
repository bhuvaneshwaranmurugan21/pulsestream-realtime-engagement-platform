from __future__ import annotations

from pathlib import Path

import pytest

from pulsestream.uri import read_json_uri, s3_location, write_json_uri


def test_local_json_uri_round_trip(tmp_path: Path):
    path = tmp_path / "nested/control.json"
    write_json_uri(str(path), {"b": 1, "a": 2})
    assert path.read_text(encoding="utf-8") == '{"a":2,"b":1}\n'
    assert read_json_uri(path.as_uri()) == {"a": 2, "b": 1}


def test_s3_and_scheme_validation():
    assert s3_location("s3://bucket/path/manifest.json") == ("bucket", "path/manifest.json")
    with pytest.raises(ValueError, match="invalid S3"):
        s3_location("s3://bucket")
    with pytest.raises(ValueError, match="unsupported"):
        read_json_uri("https://example.test/control.json")
