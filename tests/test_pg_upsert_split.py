"""Unit tests for split-based fallback in etl.run.pg_upsert."""

from __future__ import annotations

from etl import run as etl_run


class CursorStub:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class ConnStub:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self) -> CursorStub:
        return CursorStub()

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


def test_pg_upsert_splits_batch_and_isolates_bad_rows(monkeypatch) -> None:
    calls: list[list[tuple]] = []

    def fake_execute_values(cur, sql, values, page_size):
        calls.append(list(values))
        if any(row[0] == "bad" for row in values):
            raise RuntimeError("bad row")

    monkeypatch.setattr("psycopg2.extras.execute_values", fake_execute_values)

    conn = ConnStub()
    batch = [
        {"id": "good-1", "__legacy_id": "1", "payload": "ok"},
        {"id": "bad", "__legacy_id": "2", "payload": "boom"},
        {"id": "good-2", "__legacy_id": "3", "payload": "ok"},
        {"id": "good-3", "__legacy_id": "4", "payload": "ok"},
    ]

    etl_run.ETL_ERRORS.clear()
    ok, err = etl_run.pg_upsert(conn, "is_test", batch, "id")

    assert (ok, err) == (3, 1)
    assert conn.commit_calls == 2
    assert conn.rollback_calls == 4
    assert len(calls) == 6
    assert any(item["legacy_id"] == "2" for item in etl_run.ETL_ERRORS)


def test_pg_upsert_success_keeps_single_commit(monkeypatch) -> None:
    calls: list[list[tuple]] = []

    def fake_execute_values(cur, sql, values, page_size):
        calls.append(list(values))

    monkeypatch.setattr("psycopg2.extras.execute_values", fake_execute_values)

    conn = ConnStub()
    batch = [
        {"id": "good-1", "__legacy_id": "1", "payload": "ok"},
        {"id": "good-2", "__legacy_id": "2", "payload": "ok"},
    ]

    etl_run.ETL_ERRORS.clear()
    ok, err = etl_run.pg_upsert(conn, "is_test", batch, "id")

    assert (ok, err) == (2, 0)
    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert len(calls) == 1
