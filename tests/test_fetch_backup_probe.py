from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from scripts import fetch_backup


def _make_config(tmp_path: Path, protocol: str = "ftp") -> fetch_backup.BackupConfig:
    return fetch_backup.BackupConfig(
        protocol=protocol,
        sftp_host="sftp.example.test",
        sftp_port=22,
        sftp_user="root",
        sftp_password="secret",
        ssh_key_path="",
        ssh_key_b64="",
        ftp_host="ftp.example.test",
        ftp_port=21,
        ftp_user="user",
        ftp_password="password",
        ftp_remote_dir="/public_html/.well-known/backup-jet",
        sftp_remote_dir="/var/lib/vz/dump",
        prefix="nblgrafica_app-",
        extension=".sql",
        local_dir=tmp_path,
        retention_days=7,
        sftp_timeout=30,
        ftp_timeout=30,
        ftp_retries=3,
        ftp_blocksize=1024,
    )


def test_probe_backup_source_success_ftp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    entries = [
        fetch_backup.BackupEntry("nblgrafica_app-2026-04-04.sql", datetime(2026, 4, 4, 3, 0, 0), 10),
        fetch_backup.BackupEntry("nblgrafica_app-2026-04-05.sql", datetime(2026, 4, 5, 3, 0, 0), 20),
    ]

    monkeypatch.setattr(fetch_backup, "_list_ftp_entries", lambda cfg, use_tls: entries)

    result = fetch_backup.probe_backup_source(date(2026, 4, 5), config=config)

    assert result.protocol == "ftp"
    assert result.remote_dir == "/public_html/.well-known/backup-jet"
    assert result.selected_file == "nblgrafica_app-2026-04-05.sql"
    assert result.matched_files == 2
    assert result.size_bytes == 20


def test_probe_backup_source_raises_explicit_authentication_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)

    def _raise(*args, **kwargs):
        raise fetch_backup.BackupSourceError("authentication", "FTP login failed: 530 Login authentication failed")

    monkeypatch.setattr(fetch_backup, "_list_ftp_entries", _raise)

    with pytest.raises(fetch_backup.BackupSourceError, match="authentication") as exc_info:
        fetch_backup.probe_backup_source(date(2026, 4, 5), config=config)

    assert exc_info.value.category == "authentication"


def test_probe_backup_source_raises_explicit_remote_dir_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)

    def _raise(*args, **kwargs):
        raise fetch_backup.BackupSourceError("remote_dir", "FTP could not enter '/public_html/.well-known/backup-jet'")

    monkeypatch.setattr(fetch_backup, "_list_ftp_entries", _raise)

    with pytest.raises(fetch_backup.BackupSourceError, match="remote_dir") as exc_info:
        fetch_backup.probe_backup_source(date(2026, 4, 5), config=config)

    assert exc_info.value.category == "remote_dir"


def test_probe_backup_source_raises_when_no_matching_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    entries = [fetch_backup.BackupEntry("other_backup.sql", datetime(2026, 4, 5, 3, 0, 0), 10)]

    monkeypatch.setattr(fetch_backup, "_list_ftp_entries", lambda cfg, use_tls: entries)

    with pytest.raises(fetch_backup.BackupSourceError, match="no_matching_files") as exc_info:
        fetch_backup.probe_backup_source(date(2026, 4, 5), config=config)

    assert exc_info.value.category == "no_matching_files"


def test_probe_backup_source_raises_when_selected_file_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    entries = [fetch_backup.BackupEntry("nblgrafica_app-2026-04-05.sql", datetime(2026, 4, 5, 3, 0, 0), 0)]

    monkeypatch.setattr(fetch_backup, "_list_ftp_entries", lambda cfg, use_tls: entries)

    with pytest.raises(fetch_backup.BackupSourceError, match="invalid_file") as exc_info:
        fetch_backup.probe_backup_source(date(2026, 4, 5), config=config)

    assert exc_info.value.category == "invalid_file"


def test_fetch_backup_downloads_file_selected_by_probe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    expected = (tmp_path / "nblgrafica_app-2026-04-05.sql").resolve()
    selected = fetch_backup.BackupProbeResult(
        protocol="ftp",
        remote_dir=config.ftp_remote_dir,
        target_date=date(2026, 4, 5),
        selected_file="nblgrafica_app-2026-04-05.sql",
        matched_files=1,
        size_bytes=20,
        modified_at=datetime(2026, 4, 5, 3, 0, 0),
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(fetch_backup, "probe_backup_source", lambda target_date=None, config=None: selected)

    def _fake_download(filename: str, *, config=None) -> Path:
        captured["filename"] = filename
        captured["config"] = config
        return expected

    monkeypatch.setattr(fetch_backup, "download_backup_file", _fake_download)

    result = fetch_backup.fetch_backup(date(2026, 4, 5), config=config)

    assert result == expected
    assert captured["filename"] == "nblgrafica_app-2026-04-05.sql"
    assert captured["config"] == config


def test_download_backup_keeps_selected_file_even_if_older_than_retention(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    selected = tmp_path / "nblgrafica_app-2026-03-19.sql"
    stale_other = tmp_path / "nblgrafica_app-2026-03-18.sql"
    stale_other.write_text("old", encoding="utf-8")

    def _fake_fetch(*args, **kwargs) -> Path:
        selected.write_text("selected", encoding="utf-8")
        return selected

    monkeypatch.setattr(fetch_backup, "_fetch_ftp_file", _fake_fetch)

    result = fetch_backup.download_backup_file("nblgrafica_app-2026-03-19.sql", config=config)

    assert result == selected.resolve()
    assert selected.exists()
    assert not stale_other.exists()
