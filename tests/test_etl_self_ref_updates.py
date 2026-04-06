from __future__ import annotations

from typing import Any

import etl.run as run


class FetchAllCursor:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows
        self.sql: str | None = None

    def execute(self, sql: str) -> None:
        self.sql = sql

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)


class FetchManyCursor:
    def __init__(self, batches: list[list[dict[str, Any]]]):
        self._batches = list(batches)
        self.sql: str | None = None

    def execute(self, sql: str) -> None:
        self.sql = sql

    def fetchmany(self, _size: int) -> list[dict[str, Any]]:
        if not self._batches:
            return []
        return self._batches.pop(0)


def test_process_categorias_applies_parent_id_in_second_phase(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_pg_flush(pg, table, batch, conflict_col, ok, err):
        captured["table"] = table
        captured["batch"] = [row.copy() for row in batch]
        return [], ok + len(batch), err

    def fake_apply(pg, table, ref_col, updates):
        captured["ref_col"] = ref_col
        captured["updates"] = list(updates)
        return len(updates), 0

    monkeypatch.setattr(run, "pg_flush", fake_pg_flush)
    monkeypatch.setattr(run, "_apply_self_ref_updates", fake_apply)

    cursor = FetchAllCursor(
        [
            {
                "id": 1,
                "slug": "root",
                "chave": "root",
                "titulo": "Root",
                "title": None,
                "description": None,
                "descricao": None,
                "status": 1,
                "pai": None,
            },
            {
                "id": 2,
                "slug": "child",
                "chave": "child",
                "titulo": "Child",
                "title": None,
                "description": None,
                "descricao": None,
                "status": 1,
                "pai": "root",
            },
        ]
    )

    ok, err = run._process_categorias(cursor, object(), {"root": 1, "child": 2})

    child_id = run.uuid5_for("is_produtos_categorias", 2)
    parent_id = run.uuid5_for("is_produtos_categorias", 1)

    assert ok == 2
    assert err == 0
    assert captured["table"] == "is_produtos_categorias"
    assert captured["ref_col"] == "parent_id"
    assert any(row["id"] == child_id and row["parent_id"] is None for row in captured["batch"])
    assert (child_id, parent_id) in captured["updates"]


def test_process_pagamentos_applies_original_id_in_second_phase(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_transform(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": run.uuid5_for("is_pedidos_pagamentos", row["id"]),
            "cliente_id": run.uuid5_for("is_clientes", row["cliente"]),
            "forma": row.get("forma"),
            "original_id": (
                run.uuid5_for("is_pedidos_pagamentos", row["original"]) if row.get("original") else None
            ),
        }

    def fake_pg_flush(pg, table, batch, conflict_col, ok, err):
        captured.setdefault("batches", []).append([row.copy() for row in batch])
        return [], ok + len(batch), err

    def fake_apply(pg, table, ref_col, updates):
        captured["table"] = table
        captured["ref_col"] = ref_col
        captured["updates"] = list(updates)
        return len(updates), 0

    monkeypatch.setattr(run, "transform_pagamento", fake_transform)
    monkeypatch.setattr(run, "pg_flush", fake_pg_flush)
    monkeypatch.setattr(run, "_apply_self_ref_updates", fake_apply)

    cursor = FetchManyCursor(
        [
            [
                {"id": 10, "cliente": 100, "forma": None, "original": None},
                {"id": 11, "cliente": 100, "forma": None, "original": 10},
            ],
            [],
        ]
    )

    ok, err = run._process_pagamentos(cursor, object())

    first_batch = captured["batches"][0]
    original_id = run.uuid5_for("is_pedidos_pagamentos", 10)
    child_id = run.uuid5_for("is_pedidos_pagamentos", 11)

    assert ok == 2
    assert err == 0
    assert captured["table"] == "is_pedidos_pagamentos"
    assert captured["ref_col"] == "original_id"
    assert any(row["id"] == child_id and row["original_id"] is None and row["forma"] == "" for row in first_batch)
    assert (child_id, original_id) in captured["updates"]
