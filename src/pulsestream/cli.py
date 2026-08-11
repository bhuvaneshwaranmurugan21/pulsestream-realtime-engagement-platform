from __future__ import annotations

import argparse
import json

from pulsestream.evidence import run_evidence


def main() -> int:
    parser = argparse.ArgumentParser(prog="pulsestream")
    subcommands = parser.add_subparsers(dest="command", required=True)
    evidence = subcommands.add_parser("evidence")
    evidence.add_argument("--repository-root", default=".")
    evidence.add_argument("--work-dir", required=True)
    evidence.add_argument("--events", type=int, default=5_000)
    arguments = parser.parse_args()
    if arguments.command == "evidence":
        report = run_evidence(arguments.repository_root, arguments.work_dir, arguments.events)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["failed"] == 0 else 1
    return 2
