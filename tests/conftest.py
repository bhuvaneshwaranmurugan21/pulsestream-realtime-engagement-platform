from __future__ import annotations

from pathlib import Path

import pytest

from pulsestream.contracts import load_source_registry
from pulsestream.rules import load_session_rules

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def registry():
    return load_source_registry(ROOT / "config/sources.json")


@pytest.fixture
def rules():
    return load_session_rules(ROOT / "config/session_rules.json")


@pytest.fixture
def valid_event(registry):
    return {
        "schema_version": "1.0",
        "event_id": "evt-1",
        "source_id": next(iter(registry)),
        "user_id": "person@example.test",
        "event_type": "page_view",
        "event_time": "2026-08-11T09:00:00+00:00",
        "arrival_time": "2026-08-11T09:00:01+00:00",
        "consent_state": "analytics_allowed",
        "page_id": "home",
    }
