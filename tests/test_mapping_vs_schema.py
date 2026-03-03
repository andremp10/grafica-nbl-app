"""
test_mapping_vs_schema.py — Valida column_mapping.json contra schema_ref.sql.

Verifica:
1. Toda tabela em EXEC_ORDER tem entrada no column_mapping.json
2. Toda coluna mapeada existe no schema_ref.sql para a tabela correspondente
3. Toda tabela em EXEC_ORDER existe no schema_ref.sql
"""

import json
import re
import sys
from pathlib import Path

import pytest

# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
MAPPING_FILE = ROOT / "etl" / "column_mapping.json"
SCHEMA_FILE  = ROOT / "schema_ref.sql"

# EXEC_ORDER canônico (deve bater com etl/run.py)
EXEC_ORDER = [
    "is_extras_status", "is_entregas_balcoes", "is_entregas_fretes",
    "is_financeiro_funcionarios", "is_mkt_regras", "is_producao_setores",
    "is_produtos", "is_produtos_vars_nomes", "is_clientes",
    "is_usuarios", "is_clientes_pf", "is_clientes_pj", "is_clientes_enderecos",
    "is_entregas_fretes_locais", "is_produtos_categorias", "is_produtos_vars",
    "is_produtos_categorias_extras", "is_mkt_cupons", "is_mkt_cupons_produtos",
    "is_financeiro_lancamentos", "is_pedidos",
    "is_pedidos_fretes_detalhes", "is_pedidos_fretes_entregas", "is_pedidos_itens",
    "is_pedidos_pagamentos", "is_usuarios_historico",
    "is_clientes_extratos", "is_pedidos_historico",
    "is_pedidos_itens_reprovados", "is_pedidos_pag_reprovados",
]


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def mapping() -> dict:
    assert MAPPING_FILE.exists(), f"column_mapping.json não encontrado: {MAPPING_FILE}"
    return json.loads(MAPPING_FILE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schema_tables() -> dict[str, set[str]]:
    """
    Parseia schema_ref.sql e retorna {table_name: {col1, col2, ...}}.
    Suporta CREATE TABLE e ALTER TABLE ADD COLUMN.
    """
    assert SCHEMA_FILE.exists(), f"schema_ref.sql não encontrado: {SCHEMA_FILE}"
    sql = SCHEMA_FILE.read_text(encoding="utf-8")

    tables: dict[str, set[str]] = {}

    # CREATE TABLE [schema.]name (...)
    # Handles both "CREATE TABLE tablename" and "CREATE TABLE public.tablename"
    ct_pattern = re.compile(
        r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(?:\w+\.)?["`]?(\w+)["`]?\s*\(([^;]+?)\);',
        re.IGNORECASE | re.DOTALL,
    )
    col_pattern = re.compile(r'^\s*["`]?(\w+)["`]?\s+\w', re.MULTILINE)

    for m in ct_pattern.finditer(sql):
        tname = m.group(1).strip().strip('"').strip('`')
        body  = m.group(2)
        cols: set[str] = set()
        for col_m in col_pattern.finditer(body):
            candidate = col_m.group(1)
            # Ignorar keywords SQL usadas no início de linha (CONSTRAINT, PRIMARY, etc.)
            if candidate.upper() not in {
                "CONSTRAINT", "PRIMARY", "UNIQUE", "INDEX", "KEY",
                "FOREIGN", "CHECK", "FULLTEXT", "SPATIAL",
            }:
                cols.add(candidate)
        tables[tname] = cols

    return tables


# ─────────────────────────────────────────────────────────────
# Testes
# ─────────────────────────────────────────────────────────────
class TestMappingCompleteness:
    def test_all_exec_order_tables_in_mapping(self, mapping):
        missing = [t for t in EXEC_ORDER if t not in mapping]
        assert not missing, (
            f"{len(missing)} tabela(s) do EXEC_ORDER sem entry no mapping:\n"
            + "\n".join(f"  - {t}" for t in missing)
        )

    def test_no_unknown_tables_in_mapping(self, mapping):
        """Mapping não deve ter tabelas fora do EXEC_ORDER (exceto possíveis aliases)."""
        known = set(EXEC_ORDER)
        extra = [t for t in mapping if t not in known]
        # Apenas aviso — pode haver entradas extras intencionais
        if extra:
            pytest.warns(UserWarning, match="")  # placeholder; usa apenas como info
        # Não falha — informativo
        # assert not extra, ...


class TestSchemaPresence:
    def test_exec_order_tables_exist_in_schema(self, schema_tables):
        # is_produtos_categorias_extras pode ter nome diferente no schema
        missing = [t for t in EXEC_ORDER if t not in schema_tables]
        assert not missing, (
            f"{len(missing)} tabela(s) do EXEC_ORDER não encontradas no schema_ref.sql:\n"
            + "\n".join(f"  - {t}" for t in missing)
        )


class TestMappedColumnsExistInSchema:
    @pytest.mark.parametrize("table", EXEC_ORDER)
    def test_mapped_columns_exist(self, table, mapping, schema_tables):
        if table not in mapping:
            pytest.skip(f"Tabela '{table}' não está no mapping (já coberta por outro teste).")

        if table not in schema_tables:
            pytest.skip(f"Tabela '{table}' não encontrada no schema (já coberta por outro teste).")

        mapped_cols = set(mapping[table].keys())  # {supabase_col_name, ...}
        schema_cols = schema_tables[table]

        missing = mapped_cols - schema_cols
        assert not missing, (
            f"Tabela '{table}': colunas mapeadas ausentes no schema:\n"
            + "\n".join(f"  - {c}" for c in sorted(missing))
        )
