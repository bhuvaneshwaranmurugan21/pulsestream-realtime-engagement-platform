from __future__ import annotations

import pytest

from pulsestream.publication import OffsetRange, SnapshotRef
from spark_jobs.branching import (
    advance_frontier,
    branch_table,
    generation_branch_name,
    snapshot_map,
    validated_branch_table,
)


def test_offset_frontier_never_rewinds():
    parent = (OffsetRange(0, 0, 5), OffsetRange(1, 0, 9))
    current = (OffsetRange(0, 6, 7), OffsetRange(1, 10, 12))
    assert advance_frontier(current, parent, "LIVE") == (
        OffsetRange(0, 0, 7),
        OffsetRange(1, 0, 12),
    )
    assert advance_frontier(current, parent, "REPLAY") == parent
    with pytest.raises(ValueError, match="non-contiguous"):
        advance_frontier((OffsetRange(0, 5, 7),), parent, "LIVE")


def test_generation_branch_guard_and_identifier():
    branch = generation_branch_name("generation-1")
    assert len(branch) == 42
    assert branch_table("glue.analytics.events", branch) == (
        f"glue.analytics.events.branch_{branch}"
    )
    with pytest.raises(ValueError, match="unsafe"):
        validated_branch_table("glue.analytics.events; DROP TABLE")
    refs = (SnapshotRef("event", "1"), SnapshotRef("session", "2"))
    assert snapshot_map(refs) == {"event": "1", "session": "2"}
