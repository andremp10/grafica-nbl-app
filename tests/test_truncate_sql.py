"""
test_truncate_sql.py — Valida a geração do SQL de TRUNCATE.

Verifica:
1. SQL contém todas as tabelas do EXEC_ORDER
2. Tabelas chat_* NÃO estão incluídas
3. SQL contém RESTART IDENTITY CASCADE
4. Safety gate: TRUNCATE_CONFIRM != "YES" lança PermissionError
5. TRUNCATE_ENABLED=0 sai sem executar
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.truncate_supabase import (
    EXEC_ORDER,
    APP_ONLY_TABLES,
    _resolve_tables,
    _build_truncate_sql,
    truncate_supabase,
)


# ─────────────────────────────────────────────────────────────
# Testes de _resolve_tables
# ─────────────────────────────────────────────────────────────
class TestResolveTables:
    def test_default_returns_reversed_exec_order(self):
        tables = _resolve_tables()
        assert tables == list(reversed(EXEC_ORDER))

    def test_app_only_tables_excluded(self):
        tables = _resolve_tables()
        for t in APP_ONLY_TABLES:
            assert t not in tables, f"Tabela app-only '{t}' não deve estar no truncate."

    def test_chat_tables_not_included(self):
        """chat_sessions e chat_messages nunca devem aparecer."""
        tables = _resolve_tables()
        assert "chat_sessions" not in tables
        assert "chat_messages" not in tables

    def test_override_list(self):
        override = ["is_pedidos", "is_pedidos_itens"]
        tables = _resolve_tables(override_list=override)
        assert tables == override

    def test_override_filters_app_only(self):
        override = ["is_pedidos", "chat_sessions", "chat_messages"]
        tables = _resolve_tables(override_list=override)
        assert "chat_sessions" not in tables
        assert "chat_messages" not in tables
        assert "is_pedidos" in tables

    def test_all_30_etl_tables_present(self):
        tables = _resolve_tables()
        assert len(tables) == len(EXEC_ORDER), (
            f"Esperado {len(EXEC_ORDER)} tabelas, obtido {len(tables)}."
        )
        for t in EXEC_ORDER:
            assert t in tables, f"Tabela ETL '{t}' ausente do truncate."


# ─────────────────────────────────────────────────────────────
# Testes de _build_truncate_sql
# ─────────────────────────────────────────────────────────────
class TestBuildTruncateSql:
    def test_contains_restart_identity_cascade(self):
        sql = _build_truncate_sql(["is_pedidos", "is_pedidos_itens"])
        assert "RESTART IDENTITY CASCADE" in sql.upper()

    def test_starts_with_truncate(self):
        sql = _build_truncate_sql(["is_pedidos"])
        assert sql.upper().startswith("TRUNCATE")

    def test_all_tables_quoted(self):
        tables = ["is_pedidos", "is_clientes"]
        sql = _build_truncate_sql(tables)
        for t in tables:
            assert f'"{t}"' in sql

    def test_single_statement(self):
        """Deve gerar UM ÚNICO statement (um ponto e vírgula no final)."""
        sql = _build_truncate_sql(["is_pedidos", "is_clientes", "is_produtos"])
        assert sql.count(";") == 1

    def test_full_exec_order_sql(self):
        """SQL completo com todas as tabelas ETL não deve conter chat_*."""
        tables = _resolve_tables()
        sql = _build_truncate_sql(tables)
        assert "chat_sessions" not in sql
        assert "chat_messages" not in sql
        assert "RESTART IDENTITY CASCADE" in sql.upper()
        # Todas as tabelas ETL presentes
        for t in EXEC_ORDER:
            assert t in sql, f"Tabela '{t}' ausente do SQL de truncate."


# ─────────────────────────────────────────────────────────────
# Testes de safety gate
# ─────────────────────────────────────────────────────────────
class TestSafetyGate:
    def test_raises_permission_error_without_confirm(self):
        """Sem TRUNCATE_CONFIRM=YES deve lançar PermissionError."""
        with patch.dict(os.environ, {"TRUNCATE_CONFIRM": "NO", "TRUNCATE_ENABLED": "1"}):
            with pytest.raises(PermissionError, match="TRUNCATE_CONFIRM"):
                truncate_supabase()

    def test_raises_without_db_url(self):
        """Sem SUPABASE_DB_URL configurado deve lançar ValueError."""
        with patch.dict(os.environ, {
            "TRUNCATE_CONFIRM": "YES",
            "TRUNCATE_ENABLED": "1",
            "SUPABASE_DB_URL": "",
        }):
            with pytest.raises(ValueError, match="SUPABASE_DB_URL"):
                truncate_supabase()

    def test_dry_run_does_not_execute(self):
        """dry_run=True não deve chamar psycopg2.connect."""
        with patch("scripts.truncate_supabase.psycopg2.connect") as mock_conn:
            with patch.dict(os.environ, {
                "TRUNCATE_CONFIRM": "NO",
                "TRUNCATE_ENABLED": "1",
                "SUPABASE_DB_URL": "postgresql://fake/db",
            }):
                truncate_supabase(dry_run=True)
            mock_conn.assert_not_called()

    def test_truncate_disabled_exits_early(self):
        """TRUNCATE_ENABLED=0 deve sair sem conectar."""
        with patch("scripts.truncate_supabase.psycopg2.connect") as mock_conn:
            with patch.dict(os.environ, {
                "TRUNCATE_ENABLED": "0",
                "TRUNCATE_CONFIRM": "YES",
                "SUPABASE_DB_URL": "postgresql://fake/db",
            }):
                truncate_supabase()
            mock_conn.assert_not_called()

    def test_truncate_executes_with_valid_env(self):
        """Com env correto e psycopg2 mockado, deve chamar execute com TRUNCATE."""
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("scripts.truncate_supabase.psycopg2.connect", return_value=mock_conn):
            with patch.dict(os.environ, {
                "TRUNCATE_ENABLED": "1",
                "TRUNCATE_CONFIRM": "YES",
                "SUPABASE_DB_URL": "postgresql://fake/db",
            }):
                truncate_supabase()

        # Verifica que execute foi chamado com SQL de TRUNCATE
        execute_calls = mock_cursor.execute.call_args_list
        assert execute_calls, "execute() não foi chamado."
        sql_executed = execute_calls[0][0][0]
        assert sql_executed.upper().startswith("TRUNCATE")
        assert "RESTART IDENTITY CASCADE" in sql_executed.upper()
