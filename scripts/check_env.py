#!/usr/bin/env python3
"""Fail-fast validator for ETL environment variables."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.error_log_sink import ensure_run_id, persist_error_event  # noqa: E402

load_dotenv()

_ALLOWED_PROTOCOLS = {"sftp", "ftp", "ftps"}

_ALWAYS_REQUIRED = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_DB_URL",
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_DATABASE",
    "BACKUP_REMOTE_DIR",
    "BACKUP_PROTOCOL",
]


def _get(name: str) -> str:
    return (os.getenv(name, "") or "").strip()


def _protocol() -> str:
    return _get("BACKUP_PROTOCOL").lower()


def _check_format(errors: list[str]) -> None:
    supabase_url = _get("SUPABASE_URL")
    if supabase_url and not supabase_url.startswith("https://"):
        errors.append("SUPABASE_URL: must start with 'https://'.")

    db_url = _get("SUPABASE_DB_URL")
    if db_url and not db_url.startswith("postgresql://"):
        errors.append("SUPABASE_DB_URL: must start with 'postgresql://'.")

    port = _get("MYSQL_PORT")
    if port:
        try:
            int(port)
        except ValueError:
            errors.append("MYSQL_PORT: must be an integer.")

    proto = _protocol()
    if proto and proto not in _ALLOWED_PROTOCOLS:
        errors.append("BACKUP_PROTOCOL: must be one of {sftp, ftp, ftps}.")


def _check_sftp_auth(errors: list[str], warn_only: bool) -> None:
    if _protocol() != "sftp":
        return

    key_path = _get("BACKUP_SSH_KEY_PATH")
    key_b64 = _get("BACKUP_SSH_KEY_B64")
    key_inline = _get("BACKUP_SSH_KEY")
    password = _get("BACKUP_SFTP_PASSWORD")

    if key_b64 or key_inline or password:
        return

    if key_path:
        expanded = Path(key_path).expanduser()
        if not expanded.exists():
            msg = (
                "BACKUP_SSH_KEY_PATH: file not found. "
                "Provide BACKUP_SSH_KEY/BACKUP_SSH_KEY_B64/BACKUP_SFTP_PASSWORD as fallback."
            )
            if warn_only:
                print(f"  [WARN] {msg}")
            else:
                errors.append(msg)
        return

    msg = (
        "SFTP auth missing. Configure one of: "
        "BACKUP_SSH_KEY_PATH, BACKUP_SSH_KEY, BACKUP_SSH_KEY_B64, BACKUP_SFTP_PASSWORD."
    )
    if warn_only:
        print(f"  [WARN] {msg}")
    else:
        errors.append(msg)


def _check_protocol_requirements(errors: list[str], mode: str) -> None:
    proto = _protocol()
    if not proto:
        return

    if proto in {"ftp", "ftps"}:
        for var in ["BACKUP_FTP_HOST", "BACKUP_FTP_USER", "BACKUP_FTP_PASSWORD"]:
            if not _get(var):
                errors.append(f"{var}: missing or empty")

    if proto == "sftp":
        for var in ["BACKUP_SFTP_HOST", "BACKUP_SFTP_USER"]:
            if not _get(var):
                errors.append(f"{var}: missing or empty")
        _check_sftp_auth(errors, warn_only=(mode == "dry-run"))


def _check_truncate_gate(errors: list[str], mode: str) -> None:
    if mode != "production":
        return
    truncate_enabled = _get("TRUNCATE_ENABLED") == "1"
    truncate_confirm = _get("TRUNCATE_CONFIRM")
    if truncate_enabled and truncate_confirm != "YES":
        errors.append(
            "TRUNCATE_ENABLED=1 but TRUNCATE_CONFIRM is not 'YES'. "
            "Set TRUNCATE_CONFIRM=YES in secrets/variables."
        )


def collect_env_errors(mode: str = "production") -> list[str]:
    errors: list[str] = []

    for var in _ALWAYS_REQUIRED:
        if not _get(var):
            errors.append(f"{var}: missing or empty")

    _check_format(errors)
    _check_protocol_requirements(errors, mode=mode)
    _check_truncate_gate(errors, mode=mode)
    return errors


def check_env(mode: str = "production") -> None:
    errors = collect_env_errors(mode=mode)

    if errors:
        print(f"\n[check_env] FAIL ({mode}) - {len(errors)} issue(s):\n")
        for err in errors:
            print(f"  [FAIL] {err}")
        print()
        sys.exit(1)

    print(f"[check_env] OK ({mode}) - required environment is valid.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate env vars before ETL job.")
    parser.add_argument(
        "--mode",
        choices=["production", "dry-run"],
        default="production",
        help="Validation mode.",
    )
    # Backward compatibility with previous flag.
    parser.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    mode = "dry-run" if args.dry_run else args.mode
    errors = collect_env_errors(mode=mode)
    if errors:
        persist_error_event(
            script_name="scripts/check_env.py",
            step_name="check_env",
            phase="validation",
            event_type="env_validation_failure",
            message=f"{len(errors)} environment validation error(s)",
            error_class="EnvironmentError",
            run_id=ensure_run_id(),
            details={"mode": mode, "errors": errors},
        )
    check_env(mode=mode)


if __name__ == "__main__":
    main()
