#!/usr/bin/env python3
"""Daily orchestrator for MySQL -> Supabase ETL."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

# Ensure repository root is on sys.path when invoked as:
#   python scripts/daily_job.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.check_env import check_env
from scripts.fetch_backup import fetch_backup
from scripts.import_dump import import_dump
from scripts.truncate_supabase import truncate_supabase

load_dotenv()

ROOT = PROJECT_ROOT
COMPOSE_FILE = os.getenv("DOCKER_COMPOSE_FILE", "docker-compose.yml")
LOGS_DIR = Path(os.getenv("LOGS_DIR", "./logs"))
BACKUPS_DIR = Path(os.getenv("BACKUP_LOCAL_DIR", "./backups"))
MANIFEST_PATH = BACKUPS_DIR / "manifest.json"

LOGS_DIR.mkdir(parents=True, exist_ok=True)
BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOGS_DIR / f"daily_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
log = logging.getLogger("daily_job")


class StepTimer:
    def __init__(self, name: str):
        self.name = name
        self.start = 0.0
        self.elapsed = 0.0

    def __enter__(self) -> "StepTimer":
        self.start = time.monotonic()
        log.info("[START] %s", self.name)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.elapsed = time.monotonic() - self.start
        if exc_type:
            log.error("[FAIL] %s (%.1fs)", self.name, self.elapsed)
        else:
            log.info("[OK] %s (%.1fs)", self.name, self.elapsed)
        return False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _persist_manifest(manifest: dict) -> None:
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _mark_step(manifest: dict, step: str, status: str, details: dict | None = None) -> None:
    payload = {
        "status": status,
        "at": _utc_now(),
    }
    if details:
        payload.update(details)
    manifest.setdefault("steps", {})[step] = payload
    _persist_manifest(manifest)


def _run_etl(dry_run: bool) -> None:
    etl_script = ROOT / "etl" / "run.py"
    env = os.environ.copy()
    if dry_run:
        env["ETL_VALIDATE_ONLY"] = "1"

    result = subprocess.run([sys.executable, str(etl_script)], env=env, capture_output=False)
    if result.returncode != 0:
        raise RuntimeError(f"ETL failed with exit code {result.returncode}")


def _docker_compose_stop_mysql() -> None:
    try:
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "stop", "mysql"],
            check=True,
            capture_output=True,
            text=True,
        )
        log.info("[OK] mysql service stopped")
    except subprocess.CalledProcessError as exc:
        log.warning("[WARN] could not stop mysql service: %s", exc.stderr)


def _write_summary(steps: list[dict]) -> None:
    summary_path = LOGS_DIR / f"summary_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    total = sum(step["elapsed"] for step in steps)
    lines = [
        "=" * 60,
        f"NBL ETL Daily Job - {datetime.now().isoformat(timespec='seconds')}",
        "=" * 60,
    ]
    for step in steps:
        status = "OK" if step["ok"] else "FAIL"
        lines.append(f"[{status:<4}] {step['name']:<34} {step['elapsed']:6.1f}s")
    lines += ["-" * 60, f"Total: {total:.1f}s", "=" * 60]

    text = "\n".join(lines)
    log.info("\n%s", text)
    summary_path.write_text(text + "\n", encoding="utf-8")
    log.info("Summary saved at %s", summary_path)


def _run_step(
    name: str,
    manifest: dict,
    steps: list[dict],
    func: Callable[[], None],
) -> None:
    with StepTimer(name) as timer:
        func()
    steps.append({"name": name, "ok": True, "elapsed": timer.elapsed})
    _mark_step(manifest, name, "ok", {"elapsed_seconds": round(timer.elapsed, 2)})


def run_daily_job(
    dry_run: bool = False,
    skip_fetch: bool = False,
    backup_path_override: Path | None = None,
    backup_date: date | None = None,
) -> None:
    manifest = {
        "run_started_at": _utc_now(),
        "mode": "dry-run" if dry_run else "production",
        "status": "running",
        "steps": {},
        "backup": {},
    }
    _persist_manifest(manifest)

    steps: list[dict] = []
    backup_path: Path | None = backup_path_override
    truncate_enabled = os.getenv("TRUNCATE_ENABLED", "0") == "1"

    try:
        _run_step(
            "0. Validate env",
            manifest,
            steps,
            lambda: check_env(mode="dry-run" if dry_run else "production"),
        )

        if not skip_fetch:
            def _fetch() -> None:
                nonlocal backup_path
                backup_path = fetch_backup(target_date=backup_date)

            _run_step("1. Fetch backup", manifest, steps, _fetch)
        else:
            log.info("[OK] fetch skipped - using %s", backup_path)
            steps.append({"name": "1. Fetch backup (skipped)", "ok": True, "elapsed": 0.0})
            _mark_step(manifest, "1. Fetch backup", "skipped")

        if backup_path is None:
            raise RuntimeError("No backup file available")
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_path}")

        stat = backup_path.stat()
        manifest["backup"] = {
            "path": str(backup_path.resolve()),
            "name": backup_path.name,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / 1_048_576, 3),
            "selected_for_date": backup_date.isoformat() if backup_date else "latest",
            "recorded_at": _utc_now(),
        }
        _persist_manifest(manifest)

        if dry_run:
            log.info("[OK] dry-run: skipping MySQL import")
            steps.append({"name": "2. Import dump MySQL (dry-run)", "ok": True, "elapsed": 0.0})
            _mark_step(manifest, "2. Import dump MySQL", "skipped")
        else:
            _run_step("2. Import dump MySQL", manifest, steps, lambda: import_dump(backup_path))

        # Validação obrigatória de transformação ANTES do truncate.
        # Garante fail-fast: se transformação estiver errada, não limpa o Supabase.
        _run_step(
            "2.5 Validate transformations (no write)",
            manifest,
            steps,
            lambda: _run_etl(dry_run=True),
        )

        if truncate_enabled and not dry_run:
            _run_step("3. Truncate Supabase", manifest, steps, lambda: truncate_supabase())
        else:
            reason = "dry-run" if dry_run else "TRUNCATE_ENABLED=0"
            log.info("[OK] truncate skipped (%s)", reason)
            steps.append({"name": "3. Truncate Supabase (skipped)", "ok": True, "elapsed": 0.0})
            _mark_step(manifest, "3. Truncate Supabase", "skipped", {"reason": reason})

        _run_step("4. ETL MySQL -> Supabase", manifest, steps, lambda: _run_etl(dry_run=dry_run))

        if not dry_run:
            _run_step("5. Stop MySQL Docker", manifest, steps, _docker_compose_stop_mysql)

        manifest["status"] = "success"
        manifest["run_finished_at"] = _utc_now()
        _persist_manifest(manifest)

    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        manifest["run_finished_at"] = _utc_now()
        _persist_manifest(manifest)
        raise

    finally:
        _write_summary(steps)


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily ETL orchestrator")
    parser.add_argument("--dry-run", action="store_true", help="Run validation-only mode")
    parser.add_argument("--skip-fetch", action="store_true", help="Reuse existing backup file")
    parser.add_argument("--backup-file", metavar="PATH", help="Use specific backup file")
    parser.add_argument("--backup-date", metavar="YYYY-MM-DD", help="Target backup date")
    args = parser.parse_args()

    backup_path_override: Path | None = None
    skip_fetch = args.skip_fetch
    if args.backup_file:
        backup_path_override = Path(args.backup_file).resolve()
        skip_fetch = True

    backup_date: date | None = None
    if args.backup_date:
        backup_date = date.fromisoformat(args.backup_date)

    if args.dry_run:
        os.environ["ETL_VALIDATE_ONLY"] = "1"

    log.info("=== NBL ETL Daily Job | %s | dry_run=%s ===", datetime.now().isoformat(timespec="seconds"), args.dry_run)

    try:
        run_daily_job(
            dry_run=args.dry_run,
            skip_fetch=skip_fetch,
            backup_path_override=backup_path_override,
            backup_date=backup_date,
        )
        log.info("=== Job completed successfully ===")
    except Exception as exc:
        log.error("=== Job failed: %s ===", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
