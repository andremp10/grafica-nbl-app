#!/usr/bin/env python3
"""Persist ETL/runtime failures to Supabase for incident analysis."""

from __future__ import annotations

import json
import os
import socket
import traceback
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

ERROR_LOG_TABLE = os.getenv("ETL_ERROR_LOG_TABLE", "etl_error_logs").strip() or "etl_error_logs"
LOCAL_FAILURE_LOG = Path(os.getenv("LOGS_DIR", "./logs")) / "error_sink_failures.log"
MAX_TEXT_CAPTURE = int(os.getenv("ETL_ERROR_LOG_MAX_TEXT", "200000"))


def ensure_run_id() -> str:
    current = (os.getenv("ETL_RUN_ID", "") or "").strip()
    if current:
        return current
    generated = f"local-{uuid.uuid4()}"
    os.environ["ETL_RUN_ID"] = generated
    return generated


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, set):
        return sorted(_json_safe(v) for v in value)
    return str(value)


def _runtime_context() -> dict[str, Any]:
    github_keys = [
        "GITHUB_RUN_ID",
        "GITHUB_RUN_ATTEMPT",
        "GITHUB_WORKFLOW",
        "GITHUB_JOB",
        "GITHUB_REF",
        "GITHUB_SHA",
        "GITHUB_ACTOR",
        "GITHUB_EVENT_NAME",
        "GITHUB_REPOSITORY",
    ]
    github = {key: os.getenv(key) for key in github_keys if os.getenv(key)}
    return {
        "hostname": socket.gethostname(),
        "cwd": str(Path.cwd()),
        "backup_protocol": (os.getenv("BACKUP_PROTOCOL", "") or "").strip() or None,
        "run_mode": (os.getenv("ETL_VALIDATE_ONLY", "") or "").strip() or None,
        "github": github,
    }


def build_error_event(
    *,
    script_name: str,
    event_type: str,
    message: str,
    step_name: str | None = None,
    phase: str | None = None,
    severity: str = "error",
    error_class: str | None = None,
    table_name: str | None = None,
    legacy_id: str | int | None = None,
    probable_constraint: str | None = None,
    traceback_text: str | None = None,
    details: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = _runtime_context()
    if details:
        payload.update(_json_safe(details))
    return {
        "run_id": run_id or ensure_run_id(),
        "script_name": script_name,
        "step_name": step_name,
        "phase": phase,
        "event_type": event_type,
        "severity": severity,
        "table_name": table_name,
        "legacy_id": None if legacy_id in (None, "", "None", "null") else str(legacy_id),
        "error_class": error_class,
        "probable_constraint": probable_constraint,
        "message": message,
        "traceback": traceback_text,
        "details": payload,
    }


def _connection():
    dsn = (os.getenv("SUPABASE_DB_URL", "") or "").strip()
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL is missing; cannot persist error log")
    return psycopg2.connect(dsn)


def _ensure_table(cur) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS public.{ERROR_LOG_TABLE} (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          run_id text NOT NULL,
          script_name text NOT NULL,
          step_name text,
          phase text,
          event_type text NOT NULL,
          severity text NOT NULL DEFAULT 'error',
          table_name text,
          legacy_id text,
          error_class text,
          probable_constraint text,
          message text NOT NULL,
          traceback text,
          details jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          created_at timestamp with time zone NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_{ERROR_LOG_TABLE}_run_id ON public.{ERROR_LOG_TABLE} (run_id);
        CREATE INDEX IF NOT EXISTS idx_{ERROR_LOG_TABLE}_created_at ON public.{ERROR_LOG_TABLE} (created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_{ERROR_LOG_TABLE}_event_type ON public.{ERROR_LOG_TABLE} (event_type);
        """
    )


def _write_local_failure(note: str) -> None:
    try:
        LOCAL_FAILURE_LOG.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().isoformat(timespec="seconds")
        with LOCAL_FAILURE_LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"[{timestamp}Z] {note}\n")
    except Exception:
        pass


def persist_error_events(events: Iterable[dict[str, Any]]) -> int:
    rows = list(events)
    if not rows:
        return 0
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                _ensure_table(cur)
                execute_values(
                    cur,
                    f"""
                    INSERT INTO public.{ERROR_LOG_TABLE} (
                      run_id,
                      script_name,
                      step_name,
                      phase,
                      event_type,
                      severity,
                      table_name,
                      legacy_id,
                      error_class,
                      probable_constraint,
                      message,
                      traceback,
                      details
                    ) VALUES %s
                    """,
                    [
                        (
                            row["run_id"],
                            row["script_name"],
                            row.get("step_name"),
                            row.get("phase"),
                            row["event_type"],
                            row.get("severity", "error"),
                            row.get("table_name"),
                            row.get("legacy_id"),
                            row.get("error_class"),
                            row.get("probable_constraint"),
                            row["message"],
                            row.get("traceback"),
                            Json(_json_safe(row.get("details") or {})),
                        )
                        for row in rows
                    ],
                )
        return len(rows)
    except Exception as exc:
        _write_local_failure(f"persist_error_events failed: {exc}")
        return 0


def persist_error_event(**kwargs: Any) -> int:
    return persist_error_events([build_error_event(**kwargs)])


def read_json_file(path: str | Path | None) -> Any:
    if not path:
        return None
    target = Path(path)
    if not target.exists():
        return None
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_text_file(path: str | Path | None, max_chars: int | None = None) -> str | None:
    if not path:
        return None
    target = Path(path)
    if not target.exists():
        return None
    try:
        text = target.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None
    limit = MAX_TEXT_CAPTURE if max_chars is None else max_chars
    if limit > 0 and len(text) > limit:
        return text[-limit:]
    return text


def capture_traceback(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
