#!/usr/bin/env python3
"""
truncate_supabase.py — Trunca tabelas ETL no Supabase antes do re-load.

ATENÇÃO: Operação destrutiva e irreversível.
  Requer TRUNCATE_CONFIRM=YES para executar.

Uso standalone:
  TRUNCATE_CONFIRM=YES python scripts/truncate_supabase.py
  TRUNCATE_CONFIRM=YES python scripts/truncate_supabase.py --dry-run   (mostra SQL, não executa)
  TRUNCATE_CONFIRM=YES python scripts/truncate_supabase.py --tables is_pedidos,is_pedidos_itens
"""

import argparse
import logging
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# EXEC_ORDER (mesma que etl/run.py — deve permanecer sincronizado)
# A truncagem é feita em ordem REVERSA para respeitar FKs.
# ─────────────────────────────────────────────────────────────
EXEC_ORDER = [
    # Nível 0
    "is_extras_status",
    "is_entregas_balcoes",
    "is_entregas_fretes",
    "is_financeiro_funcionarios",
    "is_mkt_regras",
    "is_producao_setores",
    "is_produtos",
    "is_produtos_vars_nomes",
    "is_clientes",
    # Nível 1
    "is_usuarios",
    "is_clientes_pf",
    "is_clientes_pj",
    "is_clientes_enderecos",
    "is_entregas_fretes_locais",
    "is_produtos_categorias",
    "is_produtos_vars",
    "is_produtos_categorias_extras",
    # Nível 2
    "is_mkt_cupons",
    "is_mkt_cupons_produtos",
    "is_financeiro_lancamentos",
    "is_pedidos",
    # Nível 3
    "is_pedidos_fretes_detalhes",
    "is_pedidos_fretes_entregas",
    "is_pedidos_itens",
    "is_pedidos_pagamentos",
    "is_usuarios_historico",
    # Nível 4
    "is_clientes_extratos",
    "is_pedidos_historico",
    "is_pedidos_itens_reprovados",
    "is_pedidos_pag_reprovados",
]

# Tabelas app-only que NÃO devem ser truncadas
APP_ONLY_TABLES = {"chat_sessions", "chat_messages"}

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
SUPABASE_DB_URL    = os.getenv("SUPABASE_DB_URL", "")
TRUNCATE_ENABLED   = os.getenv("TRUNCATE_ENABLED", "0") == "1"
TRUNCATE_CONFIRM   = os.getenv("TRUNCATE_CONFIRM", "NO")
TRUNCATE_MODE      = os.getenv("TRUNCATE_TABLES_MODE", "exec_order")  # exec_order | list
TRUNCATE_LIST      = os.getenv("TRUNCATE_TABLES_LIST", "")  # CSV se mode=list

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("truncate_supabase")


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def _resolve_tables(override_list: list[str] | None = None) -> list[str]:
    """
    Retorna a lista de tabelas a truncar, em ordem reversa (leaf → root).
    override_list: se fornecida, usa essa lista em vez de EXEC_ORDER/env.
    """
    if override_list:
        tables = override_list
    elif TRUNCATE_MODE == "list" and TRUNCATE_LIST:
        tables = [t.strip() for t in TRUNCATE_LIST.split(",") if t.strip()]
    else:
        # exec_order em reverso
        tables = list(reversed(EXEC_ORDER))

    # Nunca truncar tabelas app-only
    filtered = [t for t in tables if t not in APP_ONLY_TABLES]
    skipped = set(tables) - set(filtered)
    if skipped:
        log.warning("Tabelas app-only ignoradas (não serão truncadas): %s", skipped)
    return filtered


def _build_truncate_sql(tables: list[str]) -> str:
    """Gera um único TRUNCATE ... RESTART IDENTITY CASCADE."""
    quoted = ", ".join(f'"{t}"' for t in tables)
    return f"TRUNCATE {quoted} RESTART IDENTITY CASCADE;"


# ─────────────────────────────────────────────────────────────
# PONTO DE ENTRADA
# ─────────────────────────────────────────────────────────────
def truncate_supabase(
    override_tables: list[str] | None = None,
    dry_run: bool = False,
) -> None:
    """
    Trunca tabelas ETL no Supabase.

    Args:
        override_tables: Se fornecida, trunca apenas essas tabelas.
        dry_run: Se True, imprime o SQL sem executar.
    Raises:
        PermissionError: Se TRUNCATE_CONFIRM != "YES".
        ValueError: Se SUPABASE_DB_URL não configurado.
    """
    # Lê em call-time (não import-time) para que testes possam usar patch.dict(os.environ)
    truncate_enabled = os.getenv("TRUNCATE_ENABLED", "0") == "1"
    truncate_confirm = os.getenv("TRUNCATE_CONFIRM", "NO")
    supabase_db_url  = os.getenv("SUPABASE_DB_URL", "")

    if not truncate_enabled and not dry_run:
        log.info("TRUNCATE_ENABLED=0 — truncagem desabilitada. Saindo.")
        return

    if truncate_confirm != "YES" and not dry_run:
        raise PermissionError(
            "TRUNCATE_CONFIRM deve ser 'YES' para executar a truncagem. "
            "Esta operação é irreversível."
        )

    if not supabase_db_url and not dry_run:
        raise ValueError(
            "SUPABASE_DB_URL não configurado. "
            "Formato: postgresql://postgres:senha@host:5432/postgres"
        )

    tables = _resolve_tables(override_tables)
    sql = _build_truncate_sql(tables)

    log.info("Tabelas a truncar (%d):", len(tables))
    for t in tables:
        log.info("  - %s", t)

    if dry_run:
        print("\n─── SQL gerado (dry-run) ───")
        print(sql)
        print("─────────────────────────────\n")
        log.info("dry-run: nenhuma alteração executada.")
        return

    log.warning("Executando TRUNCATE — operação irreversível...")
    conn = psycopg2.connect(supabase_db_url)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        log.info("TRUNCATE concluído com sucesso. %d tabelas truncadas.", len(tables))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trunca tabelas ETL no Supabase (requer TRUNCATE_CONFIRM=YES)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o SQL gerado sem executar.",
    )
    parser.add_argument(
        "--tables",
        metavar="T1,T2,...",
        help="Lista CSV de tabelas a truncar (substitui EXEC_ORDER).",
    )
    args = parser.parse_args()

    override = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None

    try:
        truncate_supabase(override_tables=override, dry_run=args.dry_run)
    except PermissionError as exc:
        log.error("%s", exc)
        sys.exit(2)
    except Exception as exc:
        log.error("Falha fatal: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
