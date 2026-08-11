"""Pure parent-snapshot branching and source-frontier rules."""

from __future__ import annotations

import re

from pulsestream.contracts import sha256_text
from pulsestream.publication import OffsetRange, SnapshotRef

SAFE_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){2}$")
SAFE_BRANCH_TABLE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){2}\.branch_[a-z0-9_]{1,63}$"
)


def generation_branch_name(generation_id: str) -> str:
    return f"candidate_{sha256_text(generation_id)[:32]}"


def branch_table(table: str, branch: str) -> str:
    if SAFE_TABLE.fullmatch(table) is None or re.fullmatch(r"[a-z0-9_]{1,63}", branch) is None:
        raise ValueError("unsafe Iceberg table or branch identifier")
    return f"{table}.branch_{branch}"


def validated_branch_table(value: str) -> str:
    if SAFE_BRANCH_TABLE.fullmatch(value) is None:
        raise ValueError("unsafe Iceberg branch target")
    return value


def snapshot_map(values: tuple[SnapshotRef, ...]) -> dict[str, str]:
    result = {value.table: value.snapshot_id for value in values}
    if len(result) != len(values):
        raise ValueError("parent manifest repeats a table")
    return result


def validate_incremental_offsets(
    current: tuple[OffsetRange, ...], parent: tuple[OffsetRange, ...]
) -> None:
    ends = {value.partition: value.end_offset for value in parent}
    if len(ends) != len(parent):
        raise ValueError("parent repeats a Kafka partition")
    for value in current:
        previous = ends.get(value.partition)
        if previous is not None and value.start_offset != previous + 1:
            raise ValueError(f"non-contiguous source range for partition {value.partition}")


def advance_frontier(
    current: tuple[OffsetRange, ...], parent: tuple[OffsetRange, ...], mode: str
) -> tuple[OffsetRange, ...]:
    if mode == "REPLAY":
        if not parent:
            raise ValueError("a replay requires a published parent frontier")
        return parent
    if mode != "LIVE":
        raise ValueError("unsupported generation mode")
    validate_incremental_offsets(current, parent)
    result = {value.partition: value for value in parent}
    for value in current:
        previous = result.get(value.partition)
        result[value.partition] = OffsetRange(
            value.partition,
            value.start_offset if previous is None else previous.start_offset,
            value.end_offset,
        )
    return tuple(result[key] for key in sorted(result))
