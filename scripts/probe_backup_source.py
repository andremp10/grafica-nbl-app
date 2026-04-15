#!/usr/bin/env python3
"""Preflight probe for the configured ETL backup source."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.error_log_sink import capture_traceback, ensure_run_id, persist_error_event  # noqa: E402
from scripts.fetch_backup import BackupSourceError, probe_backup_source  # noqa: E402

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("probe_backup_source")


def _resolve_target_date(args: argparse.Namespace) -> date | None:
    if args.latest:
        return None
    if args.date:
        return date.fromisoformat(args.date)
    return date.today()


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe the configured backup source.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", metavar="YYYY-MM-DD", help="Target backup date.")
    group.add_argument(
        "--latest",
        action="store_true",
        help="Always probe the latest file, ignoring date.",
    )
    args = parser.parse_args()
    target_date = _resolve_target_date(args)

    try:
        result = probe_backup_source(target_date=target_date)
        payload = asdict(result)
        if payload["target_date"] is not None:
            payload["target_date"] = payload["target_date"].isoformat()
        if payload["modified_at"] is not None:
            payload["modified_at"] = payload["modified_at"].isoformat(timespec="seconds")
        print(json.dumps(payload, indent=2))
    except Exception as exc:
        details = {"target_date": target_date.isoformat() if target_date else None}
        if isinstance(exc, BackupSourceError):
            details["category"] = exc.category
        persist_error_event(
            script_name="scripts/probe_backup_source.py",
            step_name="probe_backup_source",
            phase="probe",
            event_type="backup_probe_failure",
            message=str(exc),
            error_class=type(exc).__name__,
            traceback_text=capture_traceback(exc),
            run_id=ensure_run_id(),
            details=details,
        )
        log.error("Backup source probe failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
