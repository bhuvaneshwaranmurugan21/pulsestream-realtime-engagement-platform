from __future__ import annotations

import fcntl
import shutil
from pathlib import Path

from pulsestream.evidence import run_evidence

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / "artifacts/committed-evidence"
LOCK = ROOT / "artifacts/evidence.lock"
LOCK.parent.mkdir(parents=True, exist_ok=True)
with LOCK.open("w", encoding="utf-8") as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        raise SystemExit("another evidence refresh is already running") from error
    REPORT = run_evidence(ROOT, WORK, 5_000)
    if REPORT["failed"]:
        raise SystemExit("evidence checks failed")
    TARGET = ROOT / "evidence/verified-local"
    TARGET.mkdir(parents=True, exist_ok=True)
    for name in ("summary.json", "report.html"):
        shutil.copy2(WORK / "evidence" / name, TARGET / name)
