from __future__ import annotations

import json
import sys
from pathlib import Path

from pulsestream import cli
from pulsestream.evidence import run_evidence

ROOT = Path(__file__).resolve().parents[1]


def test_end_to_end_evidence_is_green(tmp_path: Path):
    report = run_evidence(ROOT, tmp_path / "run", 1_000)
    assert report["failed"] == 0
    assert report["passed"] == report["total"] == 23
    assert (tmp_path / "run/evidence/report.html").is_file()


def test_cli_returns_report_status(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.setattr(
        cli,
        "run_evidence",
        lambda *_args, **_kwargs: {"failed": 0, "classification": "MEASURED_LOCAL_RESULT"},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["pulsestream", "evidence", "--work-dir", str(tmp_path), "--events", "1000"],
    )
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["failed"] == 0
