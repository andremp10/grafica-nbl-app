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

from scripts.error_log_sink import capture_traceback, ensure_run_id, persist_error_event  # noqa: E402
from scripts.fetch_backup import (  # noqa: E402
    BackupSourceError,
    load_backup_config,
    probe_backup_source,
    _list_ftp_entries,
    _list_sftp_entries,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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


def _list_all_files(config, protocol: str) -> None:
    """Lista todos os arquivos disponíveis no servidor remoto."""
    log.info("Listando arquivos em %s via %s...", config.remote_dir_for(protocol), protocol.upper())

    if protocol == "sftp":
        entries = _list_sftp_entries(config)
    else:
        use_tls = protocol == "ftps"
        entries = _list_ftp_entries(config, use_tls=use_tls)

    # Filtrar apenas arquivos que batem com o padrão de backup
    matched = [e for e in entries if config.date_pattern.match(e.name)]
    others  = [e for e in entries if not config.date_pattern.match(e.name)]

    matched.sort(key=lambda e: e.name, reverse=True)

    result = {
        "protocol": protocol,
        "remote_dir": config.remote_dir_for(protocol),
        "total_files": len(entries),
        "matched_backups": len(matched),
        "backups": [
            {
                "name": e.name,
                "size_mb": round(e.size_bytes / 1_048_576, 2) if e.size_bytes else None,
                "size_bytes": e.size_bytes,
                "modified_at": e.modified_at.isoformat(timespec="seconds") if e.modified_at else None,
            }
            for e in matched
        ],
        "other_files": [e.name for e in others],
    }

    print(json.dumps(result, indent=2, ensure_ascii=False))
    log.info(
        "Listagem concluída: %d arquivo(s) de backup encontrado(s) de %d total.",
        len(matched),
        len(entries),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe the configured backup source.")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", metavar="YYYY-MM-DD", help="Target backup date.")
    group.add_argument(
        "--latest",
        action="store_true",
        help="Always probe the latest file, ignoring date.",
    )
    group.add_argument(
        "--list",
        action="store_true",
        help="List ALL backup files available on the remote server.",
    )

    args = parser.parse_args()

    # ── Modo listagem completa ────────────────────────────────────────────────
    if args.list:
        try:
            config = load_backup_config()
            protocol = config.protocol.lower().strip()
            _list_all_files(config, protocol)
        except Exception as exc:
            details: dict = {}
            if isinstance(exc, BackupSourceError):
                details["category"] = exc.category
            persist_error_event(
                script_name="scripts/probe_backup_source.py",
                step_name="list_backup_files",
                phase="probe",
                event_type="backup_list_failure",
                message=str(exc),
                error_class=type(exc).__name__,
                traceback_text=capture_traceback(exc),
                run_id=ensure_run_id(),
                details=details,
            )
            log.error("Backup source listing failed: %s", exc)
            sys.exit(1)
        return

    # ── Modo probe (comportamento original) ──────────────────────────────────
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
