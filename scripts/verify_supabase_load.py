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


def verify_supabase_load() -> None:
    db_url = _require_env("SUPABASE_DB_URL")
    tables = _parse_tables(os.getenv("VERIFY_TABLES", "is_pedidos,is_clientes"))
    min_rows = int(os.getenv("VERIFY_MIN_ROWS", "1"))
    recency_table = os.getenv("VERIFY_RECENCY_TABLE", tables[0]).strip() or tables[0]
    max_age_hours = float(os.getenv("VERIFY_MAX_AGE_HOURS", "72"))
    recency_candidates = [
        c.strip()
        for c in os.getenv("VERIFY_RECENCY_COLUMNS", "updated_at,created_at,data").split(",")
        if c.strip()
    ]
    baseline_path = Path(os.getenv("VERIFY_BASELINE_PATH", "./backups/verify_baseline.json"))

    if recency_table not in tables:
        raise ValueError("VERIFY_RECENCY_TABLE must be present in VERIFY_TABLES")

    log.info("Verifying row counts in %s", ", ".join(tables))

    counts: dict[str, int] = {}
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            for table in tables:
                count = _count_rows(cur, table)
                counts[table] = count
                if count < min_rows:
                    raise RuntimeError(
                        f"verification failed: table '{table}' has {count} rows (< {min_rows})"
                    )
                log.info("[VERIFY OK] table=%s count=%d", table, count)

            recency_column = _find_recency_column(cur, recency_table, recency_candidates)
            if recency_column:
                age_hours = _max_age_hours(cur, recency_table, recency_column)
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

    _save_baseline(baseline_path, counts)
    log.info("Supabase load verification completed successfully.")


def main() -> None:
    try:
        verify_supabase_load()
    except Exception as exc:
        log.error("Verification failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
