#!/usr/bin/env python3
"""Probe and download backup files from the configured remote source."""

from __future__ import annotations

import argparse
import base64
import ftplib
import logging
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

try:
    import paramiko

    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

load_dotenv()

ALLOWED_PROTOCOLS = {"ftp", "ftps", "sftp"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_backup")


def _clean_env(value: str) -> str:
    """Remove accidental inline comments from .env values."""
    raw = (value or "").strip()
    if " #" in raw:
        raw = raw.split(" #", 1)[0].strip()
    return raw


def _parse_timeout(name: str, default: int) -> int:
    raw = _clean_env(os.getenv(name, ""))
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s inválido (%r). Usando default=%d.", name, raw, default)
        return default
    if value <= 0:
        log.warning("%s deve ser > 0 (got=%r). Usando default=%d.", name, raw, default)
        return default
    return value


@dataclass(frozen=True)
class BackupConfig:
    protocol: str
    sftp_host: str
    sftp_port: int
    sftp_user: str
    sftp_password: str
    ssh_key_path: str
    ssh_key_b64: str
    ftp_host: str
    ftp_port: int
    ftp_user: str
    ftp_password: str
    ftp_remote_dir: str
    sftp_remote_dir: str
    prefix: str
    extension: str
    local_dir: Path
    retention_days: int
    sftp_timeout: int
    ftp_timeout: int
    ftp_retries: int
    ftp_blocksize: int

    @property
    def date_pattern(self) -> re.Pattern[str]:
        return re.compile(
            r"^" + re.escape(self.prefix) + r"(\d{4}-\d{2}-\d{2})" + re.escape(self.extension) + r"$"
        )

    def remote_dir_for(self, protocol: str | None = None) -> str:
        active_protocol = (protocol or self.protocol).lower().strip()
        if active_protocol in {"ftp", "ftps"}:
            return self.ftp_remote_dir or "."
        return self.sftp_remote_dir or "."


@dataclass(frozen=True)
class BackupEntry:
    name: str
    modified_at: datetime | None = None
    size_bytes: int | None = None


@dataclass(frozen=True)
class BackupProbeResult:
    protocol: str
    remote_dir: str
    target_date: date | None
    selected_file: str
    matched_files: int
    size_bytes: int | None = None
    modified_at: datetime | None = None


class BackupSourceError(RuntimeError):
    def __init__(self, category: str, message: str):
        self.category = category
        self.detail = message
        super().__init__(f"{category}: {message}")


def load_backup_config() -> BackupConfig:
    default_timeout = _parse_timeout("BACKUP_TIMEOUT", 30)
    protocol = _clean_env(os.getenv("BACKUP_PROTOCOL", "ftp")).lower()
    return BackupConfig(
        protocol=protocol,
        sftp_host=_clean_env(os.getenv("BACKUP_SFTP_HOST", "49.12.151.235")),
        sftp_port=int(_clean_env(os.getenv("BACKUP_SFTP_PORT", "22")) or "22"),
        sftp_user=_clean_env(os.getenv("BACKUP_SFTP_USER", "root")),
        sftp_password=_clean_env(os.getenv("BACKUP_SFTP_PASSWORD", "")),
        ssh_key_path=_clean_env(os.getenv("BACKUP_SSH_KEY_PATH", "")),
        ssh_key_b64=_clean_env(os.getenv("BACKUP_SSH_KEY_B64", "")),
        ftp_host=_clean_env(os.getenv("BACKUP_FTP_HOST", "162.241.203.52")),
        ftp_port=int(_clean_env(os.getenv("BACKUP_FTP_PORT", "21")) or "21"),
        ftp_user=_clean_env(os.getenv("BACKUP_FTP_USER", "nblgra65")),
        ftp_password=_clean_env(os.getenv("BACKUP_FTP_PASSWORD", "")),
        ftp_remote_dir=_clean_env(
            os.getenv("BACKUP_FTP_REMOTE_DIR", "/public_html/.well-known/backup-jet")
        ),
        sftp_remote_dir=_clean_env(os.getenv("BACKUP_REMOTE_DIR", "/var/lib/vz/dump")),
        prefix=_clean_env(os.getenv("BACKUP_FILENAME_PREFIX", "nblgrafica_app-")),
        extension=_clean_env(os.getenv("BACKUP_EXTENSION", ".sql")),
        local_dir=Path(_clean_env(os.getenv("BACKUP_LOCAL_DIR", "./backups"))),
        retention_days=int(_clean_env(os.getenv("BACKUP_RETENTION_DAYS", "7")) or "7"),
        sftp_timeout=_parse_timeout("BACKUP_SFTP_TIMEOUT", default_timeout),
        ftp_timeout=_parse_timeout("BACKUP_FTP_TIMEOUT", default_timeout),
        ftp_retries=_parse_timeout("BACKUP_FTP_RETRIES", 6),
        ftp_blocksize=_parse_timeout("BACKUP_FTP_BLOCKSIZE", 1024 * 1024),
    )


DEFAULT_CONFIG = load_backup_config()
BACKUP_PROTOCOL = DEFAULT_CONFIG.protocol
PREFIX = DEFAULT_CONFIG.prefix
EXT = DEFAULT_CONFIG.extension
LOCAL_DIR = DEFAULT_CONFIG.local_dir
BACKUP_RETENTION_DAYS = DEFAULT_CONFIG.retention_days
DATE_PATTERN = DEFAULT_CONFIG.date_pattern


def _normalize_protocol(raw: str) -> str:
    protocol = (raw or "").strip().lower()
    if protocol not in ALLOWED_PROTOCOLS:
        raise BackupSourceError(
            "protocol",
            f"BACKUP_PROTOCOL must be one of {sorted(ALLOWED_PROTOCOLS)} (got {raw!r})",
        )
    return protocol


def _gz_magic_ok(path: Path) -> bool:
    with open(path, "rb") as handle:
        return handle.read(2) == b"\x1f\x8b"


def _validate_local(path: Path) -> None:
    if not path.exists():
        raise BackupSourceError("invalid_file", f"Downloaded file not found: {path}")
    size = path.stat().st_size
    if size <= 0:
        raise BackupSourceError("invalid_file", f"Downloaded file is empty: {path}")
    log.info("Downloaded size: %d bytes (%.1f MB)", size, size / 1_048_576)

    if path.suffix == ".gz":
        if not _gz_magic_ok(path):
            raise BackupSourceError("invalid_file", f"Invalid gzip magic bytes: {path}")
        log.info("Gzip magic bytes OK.")


def _select_best(
    file_list: list[tuple[str, datetime | None]],
    target_date: date | None,
    *,
    pattern: re.Pattern[str] = DATE_PATTERN,
    prefix: str = PREFIX,
    extension: str = EXT,
) -> str:
    candidates: list[tuple[str, date, datetime | None]] = []
    for fname, mtime in file_list:
        match = pattern.match(fname)
        if match:
            file_date = date.fromisoformat(match.group(1))
            candidates.append((fname, file_date, mtime))

    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo com padrão '{prefix}YYYY-MM-DD{extension}' encontrado."
        )

    if target_date is not None:
        exact = [candidate for candidate in candidates if candidate[1] == target_date]
        if exact:
            return exact[0][0]
        log.warning("Target date %s not found. Using latest available file.", target_date)

    candidates.sort(key=lambda candidate: (candidate[1], candidate[2] or datetime.min), reverse=True)
    chosen = candidates[0][0]
    log.info("Selected file: %s (date in name: %s)", chosen, candidates[0][1])
    return chosen


def _select_entry(
    entries: list[BackupEntry],
    target_date: date | None,
    config: BackupConfig,
) -> tuple[BackupEntry, int]:
    try:
        selected_name = _select_best(
            [(entry.name, entry.modified_at) for entry in entries],
            target_date,
            pattern=config.date_pattern,
            prefix=config.prefix,
            extension=config.extension,
        )
    except FileNotFoundError as exc:
        raise BackupSourceError("no_matching_files", str(exc)) from exc

    matching_entries = [entry for entry in entries if config.date_pattern.match(entry.name)]
    selected_entry = next(entry for entry in matching_entries if entry.name == selected_name)
    return selected_entry, len(matching_entries)


def _load_ssh_key(config: BackupConfig) -> "paramiko.PKey":
    if config.ssh_key_b64:
        log.info("Loading SSH key from BACKUP_SSH_KEY_B64.")
        raw = base64.b64decode(config.ssh_key_b64)
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

    if not config.ssh_key_path:
        raise FileNotFoundError("BACKUP_SSH_KEY_PATH is empty.")

    key_path = Path(config.ssh_key_path).expanduser()
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key file not found: {key_path}")

    try:
        return paramiko.RSAKey.from_private_key_file(str(key_path))
    except paramiko.ssh_exception.SSHException:
        return paramiko.Ed25519Key.from_private_key_file(str(key_path))


def _connect_sftp(config: BackupConfig) -> tuple["paramiko.SSHClient", "paramiko.SFTPClient"]:
    if not HAS_PARAMIKO:
        raise BackupSourceError("connectivity", "paramiko is not installed - SFTP unavailable.")

    connect_kwargs: dict[str, object] = {
        "username": config.sftp_user,
        "port": config.sftp_port,
        "timeout": config.sftp_timeout,
        "allow_agent": False,
        "look_for_keys": False,
    }

    try:
        connect_kwargs["pkey"] = _load_ssh_key(config)
    except Exception as exc:
        if config.sftp_password:
            log.info("Using BACKUP_SFTP_PASSWORD for SFTP authentication.")
            connect_kwargs["password"] = config.sftp_password
        else:
            raise BackupSourceError("authentication", f"SFTP auth configuration inválida: {exc}") from exc

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        log.info("SFTP: connecting to %s:%d as '%s'...", config.sftp_host, config.sftp_port, config.sftp_user)
        ssh.connect(config.sftp_host, **connect_kwargs)
        return ssh, ssh.open_sftp()
    except paramiko.AuthenticationException as exc:
        ssh.close()
        raise BackupSourceError("authentication", f"SFTP authentication failed: {exc}") from exc
    except FileNotFoundError as exc:
        ssh.close()
        raise BackupSourceError("authentication", str(exc)) from exc
    except Exception as exc:
        ssh.close()
        raise BackupSourceError("connectivity", f"SFTP connection failed: {exc}") from exc


def _connect_ftp(config: BackupConfig, *, use_tls: bool) -> ftplib.FTP:
    proto = "FTPS" if use_tls else "FTP"
    remote_dir = config.remote_dir_for("ftps" if use_tls else "ftp")
    ftp: ftplib.FTP = ftplib.FTP_TLS() if use_tls else ftplib.FTP()

    try:
        log.info("%s: connecting to %s:%d as '%s'...", proto, config.ftp_host, config.ftp_port, config.ftp_user)
        ftp.connect(config.ftp_host, config.ftp_port, timeout=config.ftp_timeout)
        ftp.login(config.ftp_user, config.ftp_password)
    except ftplib.error_perm as exc:
        try:
            ftp.close()
        except Exception:
            pass
        raise BackupSourceError("authentication", f"{proto} login failed: {exc}") from exc
    except Exception as exc:
        try:
            ftp.close()
        except Exception:
            pass
        raise BackupSourceError("connectivity", f"{proto} connection failed: {exc}") from exc

    try:
        if use_tls and isinstance(ftp, ftplib.FTP_TLS):
            ftp.prot_p()
        ftp.set_pasv(True)
        log.info("%s: entering remote dir '%s'", proto, remote_dir)
        if remote_dir != ".":
            ftp.cwd(remote_dir)
    except ftplib.error_perm as exc:
        try:
            ftp.close()
        except Exception:
            pass
        raise BackupSourceError("remote_dir", f"{proto} could not enter '{remote_dir}': {exc}") from exc
    except Exception as exc:
        try:
            ftp.close()
        except Exception:
            pass
        raise BackupSourceError("connectivity", f"{proto} remote dir failed: {exc}") from exc

    return ftp


def _list_sftp_entries(config: BackupConfig) -> list[BackupEntry]:
    ssh, sftp = _connect_sftp(config)
    remote_dir = config.remote_dir_for("sftp")
    try:
        attrs = sftp.listdir_attr(remote_dir)
        return [
            BackupEntry(
                name=attr.filename,
                modified_at=datetime.fromtimestamp(attr.st_mtime) if attr.st_mtime else None,
                size_bytes=int(attr.st_size) if attr.st_size is not None else None,
            )
            for attr in attrs
        ]
    except FileNotFoundError as exc:
        raise BackupSourceError("remote_dir", f"SFTP remote dir not found: {remote_dir}") from exc
    except OSError as exc:
        raise BackupSourceError("remote_dir", f"SFTP could not list '{remote_dir}': {exc}") from exc
    finally:
        try:
            sftp.close()
        finally:
            ssh.close()


def _list_ftp_entries(config: BackupConfig, *, use_tls: bool) -> list[BackupEntry]:
    ftp = _connect_ftp(config, use_tls=use_tls)
    try:
        entries: list[BackupEntry] = []
        try:
            for name, facts in ftp.mlsd():
                modified_at = None
                if "modify" in facts:
                    try:
                        modified_at = datetime.strptime(facts["modify"], "%Y%m%d%H%M%S")
                    except ValueError:
                        modified_at = None
                size_bytes = None
                if "size" in facts:
                    try:
                        size_bytes = int(facts["size"])
                    except (TypeError, ValueError):
                        size_bytes = None
                entries.append(BackupEntry(name=name, modified_at=modified_at, size_bytes=size_bytes))
        except ftplib.error_perm:
            for name in ftp.nlst():
                entries.append(BackupEntry(name=name))
        return entries
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()


def _fetch_sftp_file(config: BackupConfig, filename: str, dest_dir: Path) -> Path:
    ssh, sftp = _connect_sftp(config)
    remote_dir = config.remote_dir_for("sftp")
    local_path = dest_dir / filename
    remote_path = f"{remote_dir.rstrip('/')}/{filename}"
    try:
        log.info("SFTP: downloading %s -> %s", remote_path, local_path)
        sftp.get(remote_path, str(local_path))
        return local_path
    except FileNotFoundError as exc:
        raise BackupSourceError("invalid_file", f"SFTP file not found: {remote_path}") from exc
    except OSError as exc:
        raise BackupSourceError("connectivity", f"SFTP download failed: {exc}") from exc
    finally:
        try:
            sftp.close()
        finally:
            ssh.close()


def _fetch_ftp_file(config: BackupConfig, filename: str, dest_dir: Path, *, use_tls: bool) -> Path:
    proto = "FTPS" if use_tls else "FTP"
    local_path = dest_dir / filename
    ftp = _connect_ftp(config, use_tls=use_tls)

    remote_size = None
    try:
        remote_size = ftp.size(filename)
    except Exception:
        remote_size = None

    try:
        offset = local_path.stat().st_size
    except FileNotFoundError:
        offset = 0

    if remote_size is not None and offset and offset > int(remote_size):
        log.warning("Local file is larger than remote. Restarting download: %s", local_path)
        local_path.unlink(missing_ok=True)
        offset = 0

    for attempt in range(1, config.ftp_retries + 1):
        try:
            if offset:
                log.info("%s: resuming at %d bytes (%d/%d)", proto, offset, attempt, config.ftp_retries)
            mode = "ab" if offset else "wb"
            with open(local_path, mode) as handle:
                ftp.retrbinary(
                    f"RETR {filename}",
                    handle.write,
                    blocksize=config.ftp_blocksize,
                    rest=offset or None,
                )
            break
        except Exception as exc:
            log.warning("%s: download attempt %d/%d failed (%s)", proto, attempt, config.ftp_retries, exc)
            try:
                ftp.close()
            except Exception:
                pass
            if attempt >= config.ftp_retries:
                raise BackupSourceError("connectivity", f"{proto} download failed: {exc}") from exc
            ftp = _connect_ftp(config, use_tls=use_tls)
            try:
                offset = local_path.stat().st_size
            except FileNotFoundError:
                offset = 0

    try:
        ftp.quit()
    except Exception:
        ftp.close()
    return local_path


def probe_backup_source(
    target_date: date | None = None,
    config: BackupConfig | None = None,
) -> BackupProbeResult:
    config = config or load_backup_config()
    protocol = _normalize_protocol(config.protocol)

    if protocol == "ftp":
        entries = _list_ftp_entries(config, use_tls=False)
    elif protocol == "ftps":
        entries = _list_ftp_entries(config, use_tls=True)
    else:
        entries = _list_sftp_entries(config)

    selected_entry, matched_files = _select_entry(entries, target_date, config)
    if selected_entry.size_bytes is not None and selected_entry.size_bytes <= 0:
        raise BackupSourceError(
            "invalid_file",
            f"Selected remote file is empty: {selected_entry.name}",
        )

    result = BackupProbeResult(
        protocol=protocol,
        remote_dir=config.remote_dir_for(protocol),
        target_date=target_date,
        selected_file=selected_entry.name,
        matched_files=matched_files,
        size_bytes=selected_entry.size_bytes,
        modified_at=selected_entry.modified_at,
    )
    log.info(
        "Probe OK: protocol=%s remote_dir=%s selected=%s matched_files=%d",
        result.protocol,
        result.remote_dir,
        result.selected_file,
        result.matched_files,
    )
    return result


def _purge_old_backups(
    keep_days: int,
    *,
    local_dir: Path = LOCAL_DIR,
    pattern: re.Pattern[str] = DATE_PATTERN,
    prefix: str = PREFIX,
    extension: str = EXT,
    keep_paths: tuple[Path, ...] = (),
) -> None:
    """Remove backups locais com data anterior a keep_days dias."""
    if keep_days <= 0:
        return
    cutoff = date.today()
    removed = 0
    protected = {path.resolve() for path in keep_paths}
    for path in local_dir.glob(f"{prefix}*{extension}"):
        if path.resolve() in protected:
            continue
        match = pattern.match(path.name)
        if not match:
            continue
        file_date = date.fromisoformat(match.group(1))
        age = (cutoff - file_date).days
        if age > keep_days:
            try:
                path.unlink()
                log.info("Backup antigo removido (age=%dd): %s", age, path.name)
                removed += 1
            except OSError as exc:
                log.warning("Não foi possível remover %s: %s", path.name, exc)
    if removed:
        log.info("Retenção: %d arquivo(s) antigo(s) removido(s) (política: %dd).", removed, keep_days)


def download_backup_file(
    filename: str,
    *,
    config: BackupConfig | None = None,
) -> Path:
    config = config or load_backup_config()
    protocol = _normalize_protocol(config.protocol)
    config.local_dir.mkdir(parents=True, exist_ok=True)

    if protocol == "ftp":
        path = _fetch_ftp_file(config, filename, config.local_dir, use_tls=False)
    elif protocol == "ftps":
        path = _fetch_ftp_file(config, filename, config.local_dir, use_tls=True)
    else:
        path = _fetch_sftp_file(config, filename, config.local_dir)

    _validate_local(path)
    _purge_old_backups(
        config.retention_days,
        local_dir=config.local_dir,
        pattern=config.date_pattern,
        prefix=config.prefix,
        extension=config.extension,
        keep_paths=(path,),
    )
    return path.resolve()


def fetch_backup(target_date: date | None = None, config: BackupConfig | None = None) -> Path:
    config = config or load_backup_config()
    probe = probe_backup_source(target_date=target_date, config=config)
    return download_backup_file(probe.selected_file, config=config)


def _resolve_target_date(args: argparse.Namespace) -> date | None:
    if args.latest:
        return None
    if args.date:
        return date.fromisoformat(args.date)
    return date.today()


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

    try:
        result = fetch_backup(_resolve_target_date(args))
        print(result)
    except Exception as exc:
        log.error("Fatal fetch error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
