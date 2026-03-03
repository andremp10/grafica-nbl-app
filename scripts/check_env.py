#!/usr/bin/env python3
"""
check_env.py - fail-fast environment validator.

Security rule:
- Never print secret values.
- Print only variable names and validation messages.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_ALWAYS_REQUIRED: list[str] = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_DB_URL",
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_DATABASE",
    "BACKUP_REMOTE_DIR",
]

_REAL_REQUIRED_BASE: list[str] = [
    "BACKUP_FTP_HOST",
    "BACKUP_FTP_USER",
    "BACKUP_FTP_PASSWORD",
]


def _protocol() -> str:
    return (os.getenv("BACKUP_PROTOCOL", "sftp") or "sftp").strip().lower()


def _check_format(errors: list[str]) -> None:
    url = os.getenv("SUPABASE_URL", "")
    if url and not url.startswith("https://"):
        errors.append("SUPABASE_URL: must start with 'https://'.")

    db_url = os.getenv("SUPABASE_DB_URL", "")
    if db_url and not db_url.startswith("postgresql://"):
        errors.append("SUPABASE_DB_URL: must start with 'postgresql://'.")

    port = os.getenv("MYSQL_PORT", "")
    if port:
        try:
            int(port)
        except ValueError:
            errors.append("MYSQL_PORT: must be an integer.")

    proto = _protocol()
    if proto not in {"sftp", "ftp", "ftps"}:
        errors.append("BACKUP_PROTOCOL: must be one of {sftp, ftp, ftps}.")


def _check_sftp_auth(errors: list[str], dry_run: bool) -> None:
    if _protocol() != "sftp":
        return

    key_path_raw = os.getenv("BACKUP_SSH_KEY_PATH", "").strip()
    key_b64 = os.getenv("BACKUP_SSH_KEY_B64", "").strip()
    sftp_password = os.getenv("BACKUP_SFTP_PASSWORD", "").strip()

    if sftp_password or key_b64:
        return

    if key_path_raw:
        key_path = Path(key_path_raw).expanduser()
        if not key_path.exists():
            msg = (
                f"BACKUP_SSH_KEY_PATH: file not found at '{key_path}'. "
                "Set BACKUP_SSH_KEY_B64 or BACKUP_SFTP_PASSWORD as fallback."
            )
            if dry_run:
                print(f"  [WARN] {msg}")
            else:
                errors.append(msg)
        return

    msg = (
        "SFTP auth missing. Configure one of: "
        "BACKUP_SSH_KEY_PATH, BACKUP_SSH_KEY_B64, BACKUP_SFTP_PASSWORD."
    )
    if dry_run:
        print(f"  [WARN] {msg}")
    else:
        errors.append(msg)


def _check_truncate_gate(errors: list[str], dry_run: bool) -> None:
    if dry_run:
        return
    enabled = os.getenv("TRUNCATE_ENABLED", "0") == "1"
    confirm = os.getenv("TRUNCATE_CONFIRM", "NO")
    if enabled and confirm != "YES":
        errors.append(
            "TRUNCATE_ENABLED=1 but TRUNCATE_CONFIRM is not 'YES'. "
            "Set TRUNCATE_CONFIRM=YES to allow truncate."
        )


def check_env(dry_run: bool = False) -> None:
    errors: list[str] = []

    for var in _ALWAYS_REQUIRED:
        if not (os.getenv(var, "") or "").strip():
            errors.append(f"{var}: missing or empty")

    if not dry_run:
        for var in _REAL_REQUIRED_BASE:
            if not (os.getenv(var, "") or "").strip():
                errors.append(f"{var}: missing or empty")

        if _protocol() == "sftp":
            for var in ["BACKUP_SFTP_HOST", "BACKUP_SFTP_USER"]:
                if not (os.getenv(var, "") or "").strip():
                    errors.append(f"{var}: missing or empty")

    _check_format(errors)
    _check_sftp_auth(errors, dry_run=dry_run)
    _check_truncate_gate(errors, dry_run=dry_run)

    mode_label = "dry-run" if dry_run else "real"
    if errors:
        print(f"\n[check_env] FAIL ({mode_label}) - {len(errors)} issue(s):\n")
        for err in errors:
            print(f"  [FAIL] {err}")
        print()
        sys.exit(1)

    print(f"[check_env] OK ({mode_label}) - required environment is valid.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate env vars before ETL job.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Less strict mode for validation-only runs.",
    )
    args = parser.parse_args()
    check_env(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
