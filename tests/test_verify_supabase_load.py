"""Unit tests for scripts.verify_supabase_load."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.verify_supabase_load import _parse_min_rows_by_table, verify_supabase_load


class CursorStub:
    def __init__(self, fetch_values: list[tuple | None]):
        self._values = iter(fetch_values)
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, query: str, params=None) -> None:
        self.executed.append((query, params))

    def fetchone(self):
        return next(self._values)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class ConnStub:
    def __init__(self, cursor: CursorStub):
        self._cursor = cursor

    def cursor(self) -> CursorStub:
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _base_env(tmp_path: Path) -> dict[str, str]:
    return {
        "SUPABASE_DB_URL": "postgresql://postgres:test@db.example.com:5432/postgres",
        "VERIFY_TABLES": "is_pedidos,is_clientes",
        "VERIFY_MIN_ROWS": "1",
        "VERIFY_RECENCY_TABLE": "is_pedidos",
        "VERIFY_RECENCY_COLUMNS": "updated_at,created_at",
        "VERIFY_MAX_AGE_HOURS": "72",
        "VERIFY_BASELINE_PATH": str(tmp_path / "verify_baseline.json"),
    }


def test_verify_success_with_recency(tmp_path: Path):
    env = _base_env(tmp_path)

    # fetchone order:
    # count(is_pedidos), count(is_clientes), has(updated_at), has(created_at), max_age_hours
    cursor = CursorStub([(10,), (5,), None, (1,), (2.0,)])
    conn = ConnStub(cursor)

    with patch.dict(os.environ, env, clear=False):
        with patch("scripts.verify_supabase_load.psycopg2.connect", return_value=conn):
            verify_supabase_load()

    baseline = json.loads((tmp_path / "verify_baseline.json").read_text(encoding="utf-8"))
    assert baseline["counts"]["is_pedidos"] == 10
    assert baseline["counts"]["is_clientes"] == 5


def test_verify_fails_when_key_table_empty(tmp_path: Path):
    env = _base_env(tmp_path)
    cursor = CursorStub([(0,)])
    conn = ConnStub(cursor)

    with patch.dict(os.environ, env, clear=False):
        with patch("scripts.verify_supabase_load.psycopg2.connect", return_value=conn):
            with pytest.raises(RuntimeError, match="is_pedidos"):
                verify_supabase_load()


def test_verify_fallback_to_baseline_when_no_recency_column(tmp_path: Path):
    env = _base_env(tmp_path)
    env["VERIFY_RECENCY_COLUMNS"] = "updated_at,created_at"

    baseline_path = tmp_path / "verify_baseline.json"
    baseline_path.write_text(
        json.dumps({"updated_at": "2026-03-03T00:00:00+00:00", "counts": {"is_pedidos": 100}}),
        encoding="utf-8",
    )

    # counts, has(updated_at)=None, has(created_at)=None
    cursor = CursorStub([(101,), (50,), None, None])
    conn = ConnStub(cursor)

    with patch.dict(os.environ, env, clear=False):
        with patch("scripts.verify_supabase_load.psycopg2.connect", return_value=conn):
            verify_supabase_load()


def test_verify_success_with_min_date_threshold(tmp_path: Path):
    env = _base_env(tmp_path)
    env["VERIFY_MIN_DATE"] = "2026-01-25"
    env["VERIFY_MIN_DATE_TABLE"] = "is_pedidos"
    env["VERIFY_MIN_DATE_COLUMNS"] = "created_at,updated_at"
    env["VERIFY_MIN_DATE_MIN_ROWS"] = "1"

    # counts, has(updated_at)=None, has(created_at)=1, max_age, has(created_at)=1, rows_after
    cursor = CursorStub([(10,), (5,), None, (1,), (2.0,), (1,), (123,)])
    conn = ConnStub(cursor)

    with patch.dict(os.environ, env, clear=False):
        with patch("scripts.verify_supabase_load.psycopg2.connect", return_value=conn):
            verify_supabase_load()


def test_verify_fails_when_no_rows_after_min_date(tmp_path: Path):
    env = _base_env(tmp_path)
    env["VERIFY_MIN_DATE"] = "2026-01-25"
    env["VERIFY_MIN_DATE_TABLE"] = "is_pedidos"
    env["VERIFY_MIN_DATE_COLUMNS"] = "created_at,updated_at"
    env["VERIFY_MIN_DATE_MIN_ROWS"] = "1"

    # counts, has(updated_at)=None, has(created_at)=1, max_age, has(created_at)=1, rows_after=0
    cursor = CursorStub([(10,), (5,), None, (1,), (2.0,), (1,), (0,)])
    conn = ConnStub(cursor)

    with patch.dict(os.environ, env, clear=False):
        with patch("scripts.verify_supabase_load.psycopg2.connect", return_value=conn):
            with pytest.raises(RuntimeError, match="VERIFY_MIN_DATE"):
                verify_supabase_load()


def test_parse_min_rows_by_table_overrides_default() -> None:
    tables = ["is_pedidos", "is_clientes_pf", "is_clientes_pj"]
    parsed = _parse_min_rows_by_table(
        "is_clientes_pf:4589,is_clientes_pj:3021",
        tables,
        default_min_rows=1,
    )
    assert parsed == {
        "is_pedidos": 1,
        "is_clientes_pf": 4589,
        "is_clientes_pj": 3021,
    }


def test_parse_min_rows_by_table_rejects_unknown_table() -> None:
    with pytest.raises(ValueError, match="unknown table"):
        _parse_min_rows_by_table("is_financeiro_lancamentos:100", ["is_pedidos"], 1)
