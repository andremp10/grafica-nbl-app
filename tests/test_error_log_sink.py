from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from scripts import error_log_sink


def test_build_error_event_serializes_runtime_details(monkeypatch) -> None:
    monkeypatch.setenv("ETL_RUN_ID", "gha-123-1")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_WORKFLOW", "Nightly ETL")

    event = error_log_sink.build_error_event(
        script_name="scripts/daily_job.py",
        step_name="fetch",
        phase="probe",
        event_type="backup_probe_failure",
        message="FTP login failed",
        details={
            "path": Path("logs/test.json"),
            "when": datetime(2026, 4, 6, 1, 20, tzinfo=timezone.utc),
            "day": date(2026, 4, 6),
            "tags": {"ftp", "nightly"},
        },
    )

    assert event["run_id"] == "gha-123-1"
    assert event["details"]["path"] == "logs/test.json"
    assert event["details"]["when"] == "2026-04-06T01:20:00+00:00"
    assert event["details"]["day"] == "2026-04-06"
    assert sorted(event["details"]["tags"]) == ["ftp", "nightly"]
    assert event["details"]["github"]["GITHUB_RUN_ID"] == "123"


def test_ensure_run_id_generates_and_reuses_value(monkeypatch) -> None:
    monkeypatch.delenv("ETL_RUN_ID", raising=False)

    first = error_log_sink.ensure_run_id()
    second = error_log_sink.ensure_run_id()

    assert first == second
    assert first.startswith("local-")
