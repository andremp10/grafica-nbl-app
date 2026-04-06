#!/usr/bin/env python3
"""Post-load verification for Supabase data."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.error_log_sink import capture_traceback, ensure_run_id, persist_error_event, read_json_file  # noqa: E402

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("verify_supabase_load")

_TABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _require_env(name: str) -> str:
    value = (os.getenv(name, "") or "").strip()
    if not value:
        raise ValueError(f"{name}: missing or empty")
    return value


def _parse_tables(raw: str) -> list[str]:
    tables = [t.strip() for t in raw.split(",") if t.strip()]
    if not tables:
        raise ValueError("VERIFY_TABLES: at least one table is required")
    for table in tables:
        if not _TABLE_RE.match(table):
            raise ValueError(f"VERIFY_TABLES: invalid table name '{table}'")
    return tables


def _parse_min_rows_by_table(raw: str, tables: list[str], default_min_rows: int) -> dict[str, int]:
    thresholds = {table: default_min_rows for table in tables}
    text = (raw or "").strip()
    if not text:
        return thresholds

    for part in text.split(","):
        item = part.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"VERIFY_MIN_ROWS_BY_TABLE: invalid item '{item}' (expected table:min)")
        table, value = item.split(":", 1)
        table = table.strip()
        value = value.strip()
        if table not in thresholds:
            raise ValueError(
                f"VERIFY_MIN_ROWS_BY_TABLE: unknown table '{table}' (must be in VERIFY_TABLES)"
            )
        try:
            minimum = int(value)
        except ValueError as exc:
            raise ValueError(
                f"VERIFY_MIN_ROWS_BY_TABLE: invalid minimum for '{table}' -> '{value}'"
            ) from exc
        if minimum < 0:
            raise ValueError(f"VERIFY_MIN_ROWS_BY_TABLE: minimum must be >= 0 for '{table}'")
        thresholds[table] = minimum

    return thresholds


def _count_rows(cur, table: str) -> int:
    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    row = cur.fetchone()
    return int(row[0])


def _has_column(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def _find_recency_column(cur, table: str, candidates: list[str]) -> str | None:
    for column in candidates:
        if _has_column(cur, table, column):
            return column
    return None


def _max_age_hours(cur, table: str, column: str) -> float | None:
    cur.execute(
        f'SELECT EXTRACT(EPOCH FROM (NOW() - MAX("{column}"))) / 3600.0 FROM "{table}" WHERE "{column}" IS NOT NULL'
    )
    row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def _parse_iso_datetime(raw: str) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError("VERIFY_MIN_DATE: empty value")
    if len(text) == 10:
        text = f"{text}T00:00:00+00:00"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("VERIFY_MIN_DATE: invalid ISO date/datetime") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _count_rows_after(cur, table: str, column: str, min_dt: datetime) -> int:
    cur.execute(
        f'SELECT COUNT(*) FROM "{table}" WHERE "{column}" > %s',
        (min_dt,),
    )
    row = cur.fetchone()
    return int(row[0])


def _load_baseline(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_baseline(path: Path, counts: dict[str, int]) -> None:
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "counts": counts,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _save_diagnostics(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def verify_supabase_load() -> None:
    db_url = _require_env("SUPABASE_DB_URL")
    tables = _parse_tables(os.getenv("VERIFY_TABLES", "is_pedidos,is_clientes"))
    min_rows = int(os.getenv("VERIFY_MIN_ROWS", "1"))
    min_rows_by_table = _parse_min_rows_by_table(
        os.getenv("VERIFY_MIN_ROWS_BY_TABLE", ""),
        tables,
        min_rows,
    )
    recency_table = os.getenv("VERIFY_RECENCY_TABLE", tables[0]).strip() or tables[0]
    max_age_hours = float(os.getenv("VERIFY_MAX_AGE_HOURS", "72"))
    recency_candidates = [
        c.strip()
        for c in os.getenv("VERIFY_RECENCY_COLUMNS", "updated_at,created_at,data").split(",")
        if c.strip()
    ]
    min_date_raw = (os.getenv("VERIFY_MIN_DATE", "") or "").strip()
    min_date_table = (os.getenv("VERIFY_MIN_DATE_TABLE", recency_table) or recency_table).strip()
    min_date_candidates = [
        c.strip()
        for c in os.getenv("VERIFY_MIN_DATE_COLUMNS", ",".join(recency_candidates)).split(",")
        if c.strip()
    ]
    min_date_min_rows = int(os.getenv("VERIFY_MIN_DATE_MIN_ROWS", "1"))
    baseline_path = Path(os.getenv("VERIFY_BASELINE_PATH", "./backups/verify_baseline.json"))
    diagnostics_path = Path(os.getenv("VERIFY_DIAGNOSTICS_PATH", "./logs/verify_diagnostics.json"))

    if recency_table not in tables:
        raise ValueError("VERIFY_RECENCY_TABLE must be present in VERIFY_TABLES")
    if min_date_raw and min_date_table not in tables:
        raise ValueError("VERIFY_MIN_DATE_TABLE must be present in VERIFY_TABLES")

    log.info("Verifying row counts in %s", ", ".join(tables))

    counts: dict[str, int] = {}
    diagnostics = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": "running",
        "tables": tables,
        "min_rows_by_table": min_rows_by_table,
        "counts": counts,
        "recency": {
            "table": recency_table,
            "candidates": recency_candidates,
            "selected_column": None,
            "max_age_hours_threshold": max_age_hours,
            "age_hours": None,
            "baseline_previous_count": None,
        },
        "min_date": {
            "enabled": bool(min_date_raw),
            "table": min_date_table,
            "candidates": min_date_candidates,
            "threshold": min_date_raw or None,
            "min_rows": min_date_min_rows,
            "selected_column": None,
            "rows_after": None,
        },
        "baseline_path": str(baseline_path),
    }

    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                for table in tables:
                    count = _count_rows(cur, table)
                    counts[table] = count
                    required = min_rows_by_table[table]
                    if count < required:
                        raise RuntimeError(
                            f"verification failed: table '{table}' has {count} rows (< {required})"
                        )
                    log.info("[VERIFY OK] table=%s count=%d min_required=%d", table, count, required)

                recency_column = _find_recency_column(cur, recency_table, recency_candidates)
                diagnostics["recency"]["selected_column"] = recency_column
                if recency_column:
                    age_hours = _max_age_hours(cur, recency_table, recency_column)
                    diagnostics["recency"]["age_hours"] = age_hours
                    if age_hours is None:
                        raise RuntimeError(
                            f"verification failed: '{recency_table}.{recency_column}' has no non-null values"
                        )
                    if age_hours > max_age_hours:
                        raise RuntimeError(
                            f"verification failed: data too old in '{recency_table}.{recency_column}' "
                            f"({age_hours:.2f}h > {max_age_hours:.2f}h)"
                        )
                    log.info(
                        "[VERIFY OK] recency table=%s column=%s age_hours=%.2f",
                        recency_table,
                        recency_column,
                        age_hours,
                    )
                else:
                    baseline = _load_baseline(baseline_path)
                    previous = baseline.get("counts", {}).get(recency_table)
                    current = counts[recency_table]
                    diagnostics["recency"]["baseline_previous_count"] = previous
                    if previous is not None and current == previous:
                        raise RuntimeError(
                            "verification failed: no recency column and rowcount did not change "
                            f"for '{recency_table}' (current={current}, previous={previous})"
                        )
                    log.info(
                        "[VERIFY OK] no recency column in %s; baseline comparison passed (previous=%s current=%s)",
                        recency_table,
                        previous,
                        current,
                    )

                if min_date_raw:
                    min_dt = _parse_iso_datetime(min_date_raw)
                    min_col = _find_recency_column(cur, min_date_table, min_date_candidates)
                    diagnostics["min_date"]["selected_column"] = min_col
                    if not min_col:
                        raise RuntimeError(
                            "verification failed: no suitable date column found for VERIFY_MIN_DATE "
                            f"in table '{min_date_table}'"
                        )
                    rows_after = _count_rows_after(cur, min_date_table, min_col, min_dt)
                    diagnostics["min_date"]["rows_after"] = rows_after
                    if rows_after < min_date_min_rows:
                        raise RuntimeError(
                            "verification failed: expected rows after VERIFY_MIN_DATE "
                            f"({min_date_raw}) in '{min_date_table}.{min_col}', got {rows_after}"
                        )
                    log.info(
                        "[VERIFY OK] min-date table=%s column=%s threshold=%s rows=%d",
                        min_date_table,
                        min_col,
                        min_date_raw,
                        rows_after,
                    )

        _save_baseline(baseline_path, counts)
        diagnostics["status"] = "success"
        log.info("Supabase load verification completed successfully.")
    except Exception as exc:
        diagnostics["status"] = "failed"
        diagnostics["error"] = str(exc)
        raise
    finally:
        _save_diagnostics(diagnostics_path, diagnostics)


def main() -> None:
    try:
        verify_supabase_load()
    except Exception as exc:
        diagnostics_path = Path(os.getenv("VERIFY_DIAGNOSTICS_PATH", "./logs/verify_diagnostics.json"))
        persist_error_event(
            script_name="scripts/verify_supabase_load.py",
            step_name="verify_supabase_load",
            phase="verification",
            event_type="verify_failure",
            message=str(exc),
            error_class=type(exc).__name__,
            traceback_text=capture_traceback(exc),
            run_id=ensure_run_id(),
            details={
                "diagnostics_path": str(diagnostics_path.resolve()),
                "diagnostics": read_json_file(diagnostics_path),
            },
        )
        log.error("Verification failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
