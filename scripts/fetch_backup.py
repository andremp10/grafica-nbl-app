#!/usr/bin/env python3
"""
fetch_backup.py - Download backup file from remote source.

Strategy by protocol:
- BACKUP_PROTOCOL=sftp: try SFTP, then fallback to FTP
- BACKUP_PROTOCOL=ftp: use FTP only
- BACKUP_PROTOCOL=ftps: use FTPS only

Filename pattern:
  BACKUP_FILENAME_PREFIX + YYYY-MM-DD + BACKUP_EXTENSION
"""

from __future__ import annotations

import argparse
import base64
import ftplib
import logging
import os
import re
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

try:
    import paramiko

    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

load_dotenv()


def _clean_env(value: str) -> str:
    """Remove accidental inline comments from .env values."""
    raw = (value or "").strip()
    if " #" in raw:
        raw = raw.split(" #", 1)[0].strip()
    return raw


# Config
BACKUP_PROTOCOL = _clean_env(os.getenv("BACKUP_PROTOCOL", "sftp")).lower()

SFTP_HOST = _clean_env(os.getenv("BACKUP_SFTP_HOST", "49.12.151.235"))
SFTP_PORT = int(_clean_env(os.getenv("BACKUP_SFTP_PORT", "22")) or "22")
SFTP_USER = _clean_env(os.getenv("BACKUP_SFTP_USER", "root"))
SFTP_PASSWORD = _clean_env(os.getenv("BACKUP_SFTP_PASSWORD", ""))
SSH_KEY_PATH = _clean_env(os.getenv("BACKUP_SSH_KEY_PATH", ""))
SSH_KEY_B64 = _clean_env(os.getenv("BACKUP_SSH_KEY_B64", ""))

FTP_HOST = _clean_env(os.getenv("BACKUP_FTP_HOST", "162.241.203.52"))
FTP_PORT = int(_clean_env(os.getenv("BACKUP_FTP_PORT", "21")) or "21")
FTP_USER = _clean_env(os.getenv("BACKUP_FTP_USER", "nblgra65"))
FTP_PASSWORD = _clean_env(os.getenv("BACKUP_FTP_PASSWORD", ""))
FTP_REMOTE_DIR = _clean_env(
    os.getenv("BACKUP_FTP_REMOTE_DIR", "/public_html/.well-known/backup-jet")
)

REMOTE_DIR = _clean_env(os.getenv("BACKUP_REMOTE_DIR", "/var/lib/vz/dump"))
PREFIX = _clean_env(os.getenv("BACKUP_FILENAME_PREFIX", "nblgrafica_app-"))
EXT = _clean_env(os.getenv("BACKUP_EXTENSION", ".sql"))
LOCAL_DIR = Path(_clean_env(os.getenv("BACKUP_LOCAL_DIR", "./backups")))

DATE_PATTERN = re.compile(
    r"^" + re.escape(PREFIX) + r"(\d{4}-\d{2}-\d{2})" + re.escape(EXT) + r"$"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_backup")


def _gz_magic_ok(path: Path) -> bool:
    with open(path, "rb") as f:
        return f.read(2) == b"\x1f\x8b"


def _validate_local(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Downloaded file not found: {path}")
    size = path.stat().st_size
    if size <= 0:
        raise ValueError(f"Downloaded file is empty: {path}")
    log.info("Downloaded size: %d bytes (%.1f MB)", size, size / 1_048_576)

    if path.suffix == ".gz":
        if not _gz_magic_ok(path):
            raise ValueError(f"Invalid gzip magic bytes: {path}")
        log.info("Gzip magic bytes OK.")


def _select_best(file_list: list[tuple[str, datetime | None]], target_date: date | None) -> str:
    candidates: list[tuple[str, date, datetime | None]] = []
    for fname, mtime in file_list:
        m = DATE_PATTERN.match(fname)
        if m:
            file_date = date.fromisoformat(m.group(1))
            candidates.append((fname, file_date, mtime))

    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo com padrão '{PREFIX}YYYY-MM-DD{EXT}' encontrado."
        )

    if target_date is not None:
        exact = [c for c in candidates if c[1] == target_date]
        if exact:
            return exact[0][0]
        log.warning("Target date %s not found. Using latest available file.", target_date)

    candidates.sort(key=lambda c: (c[1], c[2] or datetime.min), reverse=True)
    chosen = candidates[0][0]
    log.info("Selected file: %s (date in name: %s)", chosen, candidates[0][1])
    return chosen


def _load_ssh_key() -> "paramiko.PKey":
    if SSH_KEY_B64:
        log.info("Loading SSH key from BACKUP_SSH_KEY_B64.")
        raw = base64.b64decode(SSH_KEY_B64)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as tmp:
            tmp.write(raw)
            tmp_path = tmp.name
        try:
            try:
                return paramiko.RSAKey.from_private_key_file(tmp_path)
            except paramiko.ssh_exception.SSHException:
                return paramiko.Ed25519Key.from_private_key_file(tmp_path)
        finally:
            os.unlink(tmp_path)

    if not SSH_KEY_PATH:
        raise FileNotFoundError("BACKUP_SSH_KEY_PATH is empty.")

    key_path = Path(SSH_KEY_PATH).expanduser()
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key file not found: {key_path}")

    try:
        return paramiko.RSAKey.from_private_key_file(str(key_path))
    except paramiko.ssh_exception.SSHException:
        return paramiko.Ed25519Key.from_private_key_file(str(key_path))


def _fetch_sftp(target_date: date | None, dest_dir: Path) -> Path:
    if not HAS_PARAMIKO:
        raise ImportError("paramiko is not installed - SFTP unavailable.")

    log.info("SFTP: connecting to %s:%d as '%s'...", SFTP_HOST, SFTP_PORT, SFTP_USER)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs: dict = {
        "username": SFTP_USER,
        "port": SFTP_PORT,
        "timeout": 30,
        "allow_agent": False,
        "look_for_keys": False,
    }

    try:
        connect_kwargs["pkey"] = _load_ssh_key()
    except Exception as exc:
        log.warning("Could not load SSH key (%s).", exc)
        if SFTP_PASSWORD:
            log.info("Using BACKUP_SFTP_PASSWORD for SFTP authentication.")
            connect_kwargs["password"] = SFTP_PASSWORD

    ssh.connect(SFTP_HOST, **connect_kwargs)
    sftp = ssh.open_sftp()

    attrs = sftp.listdir_attr(REMOTE_DIR)
    file_list = [
        (a.filename, datetime.fromtimestamp(a.st_mtime) if a.st_mtime else None)
        for a in attrs
    ]

    fname = _select_best(file_list, target_date)
    remote_path = f"{REMOTE_DIR.rstrip('/')}/{fname}"
    local_path = dest_dir / fname

    log.info("SFTP: downloading %s -> %s", remote_path, local_path)
    sftp.get(remote_path, str(local_path))
    sftp.close()
    ssh.close()

    _validate_local(local_path)
    return local_path


def _fetch_ftp(target_date: date | None, dest_dir: Path, use_tls: bool = False) -> Path:
    if not FTP_PASSWORD:
        raise ValueError("BACKUP_FTP_PASSWORD not set - FTP unavailable.")

    proto = "FTPS" if use_tls else "FTP"
    log.info("%s: connecting to %s:%d as '%s'...", proto, FTP_HOST, FTP_PORT, FTP_USER)

    ftp: ftplib.FTP
    if use_tls:
        ftp = ftplib.FTP_TLS()
    else:
        ftp = ftplib.FTP()

    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASSWORD)
    if use_tls and isinstance(ftp, ftplib.FTP_TLS):
        ftp.prot_p()
    ftp.set_pasv(True)

    remote_dir = FTP_REMOTE_DIR or "."
    log.info("%s: entering remote dir '%s'", proto, remote_dir)
    if remote_dir != ".":
        ftp.cwd(remote_dir)

    file_list: list[tuple[str, datetime | None]] = []
    try:
        for name, facts in ftp.mlsd():
            mtime = None
            if "modify" in facts:
                try:
                    mtime = datetime.strptime(facts["modify"], "%Y%m%d%H%M%S")
                except ValueError:
                    mtime = None
            file_list.append((name, mtime))
    except ftplib.error_perm:
        names = ftp.nlst()
        file_list = [(n, None) for n in names]

    fname = _select_best(file_list, target_date)
    local_path = dest_dir / fname

    log.info("%s: downloading %s -> %s", proto, fname, local_path)
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {fname}", f.write)
    ftp.quit()

    _validate_local(local_path)
    return local_path


def fetch_backup(target_date: date | None = None) -> Path:
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    protocol = BACKUP_PROTOCOL or "sftp"
    protocol = protocol.lower().strip()
    if protocol not in {"sftp", "ftp", "ftps"}:
        raise ValueError("BACKUP_PROTOCOL must be one of: sftp, ftp, ftps")

    errors: list[str] = []

    if protocol == "ftp":
        return _fetch_ftp(target_date, LOCAL_DIR, use_tls=False).resolve()

    if protocol == "ftps":
        return _fetch_ftp(target_date, LOCAL_DIR, use_tls=True).resolve()

    # protocol == sftp: fallback to ftp
    try:
        path = _fetch_sftp(target_date, LOCAL_DIR)
        log.info("Backup fetched via SFTP: %s", path.resolve())
        return path.resolve()
    except Exception as exc:
        msg = f"SFTP failed: {exc}"
        log.warning(msg)
        errors.append(msg)

    try:
        path = _fetch_ftp(target_date, LOCAL_DIR, use_tls=False)
        log.info("Backup fetched via FTP fallback: %s", path.resolve())
        return path.resolve()
    except Exception as exc:
        msg = f"FTP failed: {exc}"
        log.error(msg)
        errors.append(msg)

    raise RuntimeError(
        "Could not download backup by any protocol.\n"
        + "\n".join(f"  - {e}" for e in errors)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Download remote backup (SFTP/FTP/FTPS).")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", metavar="YYYY-MM-DD", help="Target backup date.")
    group.add_argument(
        "--latest",
        action="store_true",
        help="Always download latest file, ignoring date.",
    )
    args = parser.parse_args()

    if args.latest:
        target = None
    elif args.date:
        target = date.fromisoformat(args.date)
    else:
        target = date.today()

    try:
        result = fetch_backup(target)
        print(result)
    except Exception as exc:
        log.error("Fatal fetch error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
