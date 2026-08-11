from __future__ import annotations

import pytest

from pulsestream.aws_publication import publication_transaction, registration_item

H = "c" * 64


def test_candidate_registration_and_publication_transaction():
    item = registration_item(
        {
            "generation_id": "live-1",
            "parent_generation_id": None,
            "mode": "LIVE",
            "manifest_uri": "s3://artifacts/live-1/manifest.json",
            "manifest_sha256": H,
        }
    )
    assert item["status"] == {"S": "CANDIDATE"}
    transaction = publication_transaction("generation-table", "live-1", None, H, 0)
    assert len(transaction) == 2
    assert transaction[1]["Update"]["ExpressionAttributeValues"][":next_version"] == {"N": "1"}


@pytest.mark.parametrize(
    "change",
    [
        {"generation_id": "bad id"},
        {"mode": "BATCH"},
        {"manifest_uri": "https://example.test/manifest"},
        {"manifest_sha256": "bad"},
    ],
)
def test_bad_candidate_registration_is_rejected(change):
    request = {
        "generation_id": "live-1",
        "parent_generation_id": None,
        "mode": "LIVE",
        "manifest_uri": "s3://artifacts/live-1/manifest.json",
        "manifest_sha256": H,
    }
    with pytest.raises(ValueError):
        registration_item(request | change)


def test_negative_pointer_version_is_rejected():
    with pytest.raises(ValueError, match="pointer_version"):
        publication_transaction("generation-table", "live-1", None, H, -1)
