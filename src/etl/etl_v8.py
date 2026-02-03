#!/usr/bin/env python3
"""
ETL v8: Migração MySQL -> Supabase (Postgres)

Características principais:
- Upsert em batch com fallback por divisão até linha (evita perder 500 linhas por 1 erro)
- Conversão de tipos baseada no schema REAL do Supabase (information_schema)
- Validação opcional de FKs via cache de PKs inseridos (ETL_VALIDATE_FKS=0 para desativar)
- Deduplicação:
  - is_visitas_online.id_session (opcional via ETL_DEDUP_VISITAS_ONLINE=1)
  - UNIQUE de 1 coluna: se duplicar e coluna for nullable, remove o campo (inserção vira NULL)
- Logs:
  - etl/{ETL_ERRORS_DIR}/{table}.log (1 linha por registro que falhou no upsert)

Execução:
  python -m src.main --sql ./sql_input/SEU_DUMP.sql
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from src.utils.paths import ensure_dir, find_repo_root

BASE_DIR = Path(__file__).resolve().parent

LOGGER = None
REPO_ROOT: Path | None = None
OUTPUT_DIR: Path | None = None
ERROR_DIR: Path | None = None
MAPPING_PATH: Path | None = None

UUID_NS = uuid.UUID("2a6b2c31-0f2a-4dfd-8cde-7b4b9b3f1c5a")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)

EMAIL_RE = re.compile(r"\b([A-Za-z0-9._%+-]{1,128})@([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")
CPF_CNPJ_RE = re.compile(
    r"\b("
    r"\d{3}\.?\d{3}\.?\d{3}-?\d{2}"  # CPF
    r"|"
    r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}"  # CNPJ
    r")\b"
)

BATCH_SIZE = 500
VALIDATE_FKS = True
MAX_RETRIES = 6
RETRY_BASE_SLEEP_S = 0.8
DEDUP_VISITAS_ONLINE = False
PREFLIGHT = True
PRINT_ORDER = False
LOG_EVERY_BATCHES = 10
ONLY_TABLES_RAW = ""
ONLY_TABLES: Set[str] = set()

# Forward-compatible mapping: allow mapping to include optional/new destination columns without breaking runs.
UNKNOWN_DEST_COLS: Set[Tuple[str, str]] = set()

# Campos numéricos que devem ser >= 0 (schema geralmente tem DEFAULT 0 / checks)
MONEY_COLS = {
    "total",
    "acrescimo",
    "desconto",
    "desconto_uso",
    "sinal",
    "frete_valor",
    "subtotal",
    "valor",
}

# Fallback para resolver referência quando o Supabase não expõe FK no schema (ou foi dropada)
FK_MAP_FALLBACK = {
    "cliente_id": "is_clientes",
    "usuario_id": "is_usuarios",
    "produto_id": "is_produtos",
    "pedido_id": "is_pedidos",
    "frete_balcao_id": "is_entregas_balcoes",
    "frete_endereco_id": "is_clientes_enderecos",
    "caixa_id": "is_financeiro_caixas",
    "pdv_id": "is_financeiro_pdvs",
    "fornecedor_id": "is_financeiro_fornecedores",
    "carteira_id": "is_financeiro_carteiras",
    "funcionario_id": "is_financeiro_funcionarios",
    "centro_custo_id": "is_financeiro_centros_custo",
    "vendedor_id": "is_usuarios",
    "balcao_id": "is_entregas_balcoes",
    "orcamento_id": "is_pedidos_orcamentos",
    "item_id": "is_pedidos_itens",
    "operador_id": "is_usuarios",
    "pagamento_id": "is_pedidos_pagamentos",
    "grupo_id": "is_produtos_vars_nomes",
    "frete_id": "is_entregas_fretes",
    "original_id": "is_pedidos_pagamentos",
    "repeticao_id": "is_financeiro_repeticoes",
    "comprovante_id": "is_pedidos_pagamentos",
    "arquivo_id": "is_arquivos",
    "servico_id": "is_produtos_servicos",
    "produto_var_id": "is_produtos_vars",
    "setor_id": "is_producao_setores",
    "transportadora_id": "is_financeiro_transportadoras",
}


def log(msg: str) -> None:
    if LOGGER is not None:
        try:
            LOGGER.info(msg)
            return
        except Exception:
            pass
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def init_runtime(*, repo_root: Path | None = None, logger=None) -> None:
    """Initialize globals (paths + runtime knobs) from env and repo layout."""
    global LOGGER, REPO_ROOT, OUTPUT_DIR, ERROR_DIR, MAPPING_PATH
    global BATCH_SIZE, VALIDATE_FKS, MAX_RETRIES, RETRY_BASE_SLEEP_S
    global DEDUP_VISITAS_ONLINE, PREFLIGHT, PRINT_ORDER, LOG_EVERY_BATCHES, ONLY_TABLES_RAW, ONLY_TABLES

    REPO_ROOT = repo_root or find_repo_root()
    LOGGER = logger

    # Load local .env (NOT committed). Prefer repo root.
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv()

    OUTPUT_DIR = ensure_dir(Path(os.getenv("OUTPUT_DIR") or (REPO_ROOT / "output")))

    errors_env = os.getenv("ETL_ERROR_DIR") or os.getenv("ETL_ERRORS_DIR") or ""
    if errors_env:
        p = Path(errors_env)
        ERROR_DIR = ensure_dir(p if p.is_absolute() else (REPO_ROOT / p))
    else:
        ERROR_DIR = ensure_dir(OUTPUT_DIR / "errors")

    mapping_env = os.getenv("ETL_MAPPING_PATH") or ""
    if mapping_env:
        p = Path(mapping_env)
        MAPPING_PATH = p if p.is_absolute() else (REPO_ROOT / p)
    else:
        MAPPING_PATH = REPO_ROOT / "config" / "column_mapping.json"

    BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "500"))
    VALIDATE_FKS = os.getenv("ETL_VALIDATE_FKS", "1").lower() not in ("0", "false", "no")
    MAX_RETRIES = int(os.getenv("ETL_RETRIES", "6"))
    RETRY_BASE_SLEEP_S = float(os.getenv("ETL_RETRY_BASE_SLEEP_S", "0.8"))
    DEDUP_VISITAS_ONLINE = os.getenv("ETL_DEDUP_VISITAS_ONLINE", "0").lower() in ("1", "true", "yes", "sim")
    PREFLIGHT = os.getenv("ETL_PREFLIGHT", "1").lower() not in ("0", "false", "no")
    PRINT_ORDER = os.getenv("ETL_PRINT_ORDER", "0").lower() in ("1", "true", "yes", "sim")
    LOG_EVERY_BATCHES = int(os.getenv("ETL_LOG_EVERY_BATCHES", "10"))
    ONLY_TABLES_RAW = os.getenv("ETL_ONLY_TABLES", "").strip()
    ONLY_TABLES = {t.strip() for t in ONLY_TABLES_RAW.split(",") if t.strip()} if ONLY_TABLES_RAW else set()


def sanitize_log_msg(msg: str) -> str:
    # Keep error logs safe to share by masking common PII patterns.
    if not msg:
        return msg

    def _mask_email(m: re.Match) -> str:
        local = m.group(1) or ""
        domain = m.group(2) or ""
        if "+" in local:
            base, suffix = local.split("+", 1)
            suffix = "+" + suffix
        else:
            base, suffix = local, ""
        keep = base[:2] if len(base) >= 2 else base[:1]
        return f"{keep}***{suffix}@{domain}"

    def _mask_doc(m: re.Match) -> str:
        digits = re.sub(r"\D", "", m.group(0) or "")
        if len(digits) == 11:  # CPF
            return f"{digits[:3]}***{digits[-2:]}"
        if len(digits) == 14:  # CNPJ
            return f"{digits[:2]}***{digits[-2:]}"
        return "***"

    msg = EMAIL_RE.sub(_mask_email, msg)
    msg = CPF_CNPJ_RE.sub(_mask_doc, msg)
    return msg


def log_error(table: str, key: Any, msg: str) -> None:
    try:
        global ERROR_DIR
        if ERROR_DIR is None:
            init_runtime()
        key_str = str(key) if key is not None else "NO_KEY"
        with (ERROR_DIR / f"{table}.log").open("a", encoding="utf-8") as f:
            safe = sanitize_log_msg(msg)
            f.write(f"{key_str}: {safe[:300]}\n")
    except Exception:
        # Logging nunca deve derrubar o ETL
        pass


def uuid5_for(namespace_key: str, legacy_id: Any) -> Optional[str]:
    if legacy_id is None:
        return None
    s = str(legacy_id).strip()
    if s in ("", "0", "None", "null"):
        return None
    if UUID_RE.match(s):
        return s
    return str(uuid.uuid5(UUID_NS, f"{namespace_key}:{s}"))


def to_bool(val: Any) -> Optional[bool]:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    s = str(val).lower().strip()
    if s in ("", "none", "null"):
        return None
    return s in ("1", "true", "t", "yes", "s", "sim", "2")


def to_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    s = str(val).lower().strip()
    if s in ("", "none", "null"):
        return None
    if s == "true":
        return 1
    if s == "false":
        return 0
    try:
        return int(float(s))
    except Exception:
        return None


def to_decimal(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, bool):
        return 1.0 if val else 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s in ("", "none", "null", "None"):
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def to_money(val: Any) -> Optional[float]:
    v = to_decimal(val)
    return max(0.0, v) if v is not None else None


def to_ts(val: Any) -> Optional[str]:
    if not val:
        return None
    if isinstance(val, (dt.datetime, dt.date)):
        return val.isoformat()
    s = str(val).strip()
    if s.startswith("0000-00-00") or s in ("None", "null", ""):
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "")).isoformat()
    except Exception:
        return None


def to_date(val: Any) -> Optional[str]:
    if not val:
        return None
    if isinstance(val, dt.datetime):
        return val.date().isoformat()
    if isinstance(val, dt.date):
        return val.isoformat()
    s = str(val).strip()
    if s.startswith("0000-00-00") or s in ("None", "null", ""):
        return None
    try:
        return dt.date.fromisoformat(s[:10]).isoformat()
    except Exception:
        return None


def to_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("none", "null"):
        return None
    # Mantém string vazia ("") para satisfazer colunas NOT NULL no legado.
    return s


def short_key(key: Any) -> str:
    s = str(key) if key is not None else ""
    s = re.sub(r"[^0-9a-zA-Z]+", "", s)
    return (s[:12] or "x")


def clamp_text(val: str, *, max_len: Optional[int]) -> str:
    if max_len is None or max_len <= 0:
        return val
    return val[:max_len]


def make_unique_text(schema: "PgSchema", *, table: str, col: str, base: str, key: Any) -> str:
    max_len = schema.col_max_len(table, col)
    sk = short_key(key)

    if col.endswith("email") or col.endswith("email_log") or col == "email_log":
        if "@" in base:
            local, domain = base.split("@", 1)
            suffix = f"+dup{sk}"
            # garante que local+suffix + @domain caiba no max_len
            if max_len is not None and max_len > 0:
                keep = max_len - (len(domain) + 1)
                local = clamp_text(local, max_len=max(0, keep - len(suffix)))
            return f"{local}{suffix}@{domain}"
        suffix = f"__dup_{sk}"
        if max_len is not None and max_len > 0:
            base = clamp_text(base, max_len=max(0, max_len - len(suffix)))
        return f"{base}{suffix}"

    if col.endswith("slug") or col == "slug":
        suffix = f"-dup-{sk}"
        if max_len is not None and max_len > 0:
            base = clamp_text(base, max_len=max(0, max_len - len(suffix)))
        return f"{base}{suffix}"

    suffix = f"__dup_{sk}"
    if max_len is not None and max_len > 0:
        base = clamp_text(base, max_len=max(0, max_len - len(suffix)))
    return f"{base}{suffix}"


def get_mysql():
    import mysql.connector

    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3307")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database=os.getenv("MYSQL_DATABASE", "legacy"),
        charset="latin1",
        use_unicode=True,
        converter_class=mysql.connector.conversion.MySQLConverter,
        use_pure=True,
    )


def get_supabase():
    from supabase import create_client

    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))


def get_pg_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DBNAME", "postgres"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


def normalize_pg_type(udt_name: str, data_type: str) -> str:
    udt = (udt_name or "").lower()
    dtp = (data_type or "").lower()
    if udt == "uuid":
        return "uuid"
    if udt in ("int2", "int4", "int8"):
        return "int"
    if udt in ("numeric", "float4", "float8"):
        return "numeric"
    if udt == "bool":
        return "bool"
    if udt in ("date",):
        return "date"
    if dtp.startswith("timestamp"):
        return "timestamp"
    if udt in ("json", "jsonb"):
        return "json"
    return "text"


@dataclass(frozen=True)
class PgSchema:
    col_types: Dict[str, Dict[str, str]]
    not_null: Dict[str, Set[str]]
    has_default: Dict[str, Set[str]]
    pk_cols: Dict[str, List[str]]
    fk_refs: Dict[str, Dict[str, Tuple[str, str]]]
    unique_single_cols: Dict[str, Set[str]]
    max_len: Dict[str, Dict[str, Optional[int]]]

    def on_conflict(self, table: str) -> str:
        cols = self.pk_cols.get(table)
        return ",".join(cols) if cols else "id"

    def cache_pk_col(self, table: str) -> Optional[str]:
        cols = self.pk_cols.get(table) or []
        return cols[0] if len(cols) == 1 else None

    def col_type(self, table: str, col: str) -> Optional[str]:
        return self.col_types.get(table, {}).get(col)

    def col_max_len(self, table: str, col: str) -> Optional[int]:
        return self.max_len.get(table, {}).get(col)

    def is_not_null(self, table: str, col: str) -> bool:
        return col in self.not_null.get(table, set())

    def has_col_default(self, table: str, col: str) -> bool:
        return col in self.has_default.get(table, set())

    def fk_ref_table(self, table: str, col: str) -> Optional[str]:
        ref = self.fk_refs.get(table, {}).get(col)
        if ref:
            return ref[0]
        if col == "categoria_id":
            return "is_financeiro_categorias" if "financeiro" in table else "is_produtos_categorias"
        if col == "parent_id":
            return table
        return FK_MAP_FALLBACK.get(col) or (("is_" + col[:-3] + "s") if col.endswith("_id") else None)

    def fk_ref(self, table: str, col: str) -> Optional[Tuple[str, str]]:
        return self.fk_refs.get(table, {}).get(col)


def load_pg_schema() -> PgSchema:
    conn = get_pg_conn()
    try:
        cur = conn.cursor()

        col_types: Dict[str, Dict[str, str]] = {}
        not_null: Dict[str, Set[str]] = {}
        has_default: Dict[str, Set[str]] = {}
        max_len: Dict[str, Dict[str, Optional[int]]] = {}

        cur.execute(
            """
            SELECT table_name, column_name, data_type, udt_name, is_nullable, column_default, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
        )
        for table, col, data_type, udt_name, is_nullable, col_default, char_len in cur.fetchall():
            col_types.setdefault(table, {})[col] = normalize_pg_type(udt_name, data_type)
            if is_nullable == "NO":
                not_null.setdefault(table, set()).add(col)
            if col_default is not None:
                has_default.setdefault(table, set()).add(col)
            if char_len is not None:
                max_len.setdefault(table, {})[col] = int(char_len)

        pk_cols: Dict[str, List[str]] = {}
        cur.execute(
            """
            SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY tc.table_name, kcu.ordinal_position
            """
        )
        for table, col, _pos in cur.fetchall():
            pk_cols.setdefault(table, []).append(col)

        fk_refs: Dict[str, Dict[str, Tuple[str, str]]] = {}
        cur.execute(
            """
            SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'FOREIGN KEY'
            """
        )
        for table, col, ref_table, ref_col in cur.fetchall():
            fk_refs.setdefault(table, {})[col] = (ref_table, ref_col)

        # Unique constraints (apenas 1 coluna) para deduplicação controlada
        cur.execute(
            """
            SELECT tc.table_name, tc.constraint_name, kcu.column_name, kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'UNIQUE'
            ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
            """
        )
        unique_by_constraint: Dict[Tuple[str, str], List[str]] = {}
        for table, constraint_name, col, _pos in cur.fetchall():
            unique_by_constraint.setdefault((table, constraint_name), []).append(col)

        unique_single_cols: Dict[str, Set[str]] = {}
        for (table, _constraint), cols in unique_by_constraint.items():
            if len(cols) == 1:
                unique_single_cols.setdefault(table, set()).add(cols[0])

        return PgSchema(
            col_types=col_types,
            not_null=not_null,
            has_default=has_default,
            pk_cols=pk_cols,
            fk_refs=fk_refs,
            unique_single_cols=unique_single_cols,
            max_len=max_len,
        )
    finally:
        conn.close()


def compute_load_order(schema: PgSchema, tables: List[str]) -> List[str]:
    """
    Ordena tabelas por dependências reais de FK no Supabase (topological sort).
    Mantém ordem determinística via sort.
    """
    from collections import defaultdict, deque

    nodes = set(tables)
    deps: Dict[str, Set[str]] = {}
    for t in nodes:
        refs = set()
        for _col, (ref_table, _ref_col) in schema.fk_refs.get(t, {}).items():
            if ref_table in nodes and ref_table != t:
                refs.add(ref_table)
        deps[t] = refs

    in_deg = {t: 0 for t in nodes}
    adj: Dict[str, Set[str]] = defaultdict(set)
    for t, refs in deps.items():
        for r in refs:
            adj[r].add(t)
            in_deg[t] += 1

    q = deque(sorted([t for t, d in in_deg.items() if d == 0]))
    out: List[str] = []
    while q:
        t = q.popleft()
        out.append(t)
        for child in sorted(adj.get(t, set())):
            in_deg[child] -= 1
            if in_deg[child] == 0:
                q.append(child)

    if len(out) != len(nodes):
        out.extend(sorted(nodes - set(out)))

    return out


def preflight_fk_not_null(mysql, schema: PgSchema, mappings: Dict[str, Dict[str, str]]) -> List[Tuple[str, str, str, int, int]]:
    """
    Detecta FKs NOT NULL no Postgres que no legado aparecem como 0/NULL ou apontam para registros inexistentes.
    Retorna lista de (tabela, coluna_fk, ref_table, missing_0_null, orphan_refs).
    """
    cur = mysql.cursor()
    issues: List[Tuple[str, str, str, int, int]] = []

    for table in sorted(mappings.keys()):
        fk_cols = schema.fk_refs.get(table, {})
        if not fk_cols:
            continue
        mapping = mappings.get(table) or {}

        for pg_col, (ref_table, ref_col) in fk_cols.items():
            if not schema.is_not_null(table, pg_col):
                continue
            mysql_col = mapping.get(pg_col)
            if not mysql_col:
                continue
            parent_mapping = mappings.get(ref_table) or {}
            parent_mysql_col = parent_mapping.get(ref_col) or ref_col
            try:
                cur.execute(
                    f"SELECT SUM((`{mysql_col}` IS NULL) OR (TRIM(CAST(`{mysql_col}` AS CHAR)) IN ('', '0'))) FROM `{table}`"
                )
                missing = cur.fetchone()[0] or 0

                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM `{table}` c
                    LEFT JOIN `{ref_table}` p
                      ON c.`{mysql_col}` = p.`{parent_mysql_col}`
                    WHERE TRIM(CAST(c.`{mysql_col}` AS CHAR)) NOT IN ('', '0')
                      AND p.`{parent_mysql_col}` IS NULL
                    """
                )
                orphans = cur.fetchone()[0] or 0
            except Exception as e:
                log_error(table, pg_col, f"Preflight query failed for {pg_col}: {e}")
                continue
            missing_i = int(missing)
            orphans_i = int(orphans)
            if missing_i > 0 or orphans_i > 0:
                issues.append((table, pg_col, ref_table, missing_i, orphans_i))

    return issues


def is_transient_error(err: Exception) -> bool:
    s = str(err).lower()
    return any(
        marker in s
        for marker in (
            "getaddrinfo failed",
            "temporary failure in name resolution",
            "connection reset",
            "winerror 10054",
            "winerror 10060",
            "timed out",
            "timeout",
            "server disconnected",
            "502",
            "503",
            "504",
            "429",
        )
    )


def supa_upsert(supa, table: str, rows: List[dict], on_conflict: str) -> None:
    resp = supa.table(table).upsert(rows, on_conflict=on_conflict).execute()
    api_err = getattr(resp, "error", None)
    if api_err:
        raise Exception(str(api_err))


def execute_with_retry(fn, *, max_retries: int) -> None:
    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            fn()
            return
        except Exception as e:
            last_err = e
            if not is_transient_error(e) or attempt == max_retries - 1:
                raise
            sleep_s = RETRY_BASE_SLEEP_S * (2**attempt)
            time.sleep(min(sleep_s, 30.0))
    if last_err:
        raise last_err


def upsert_with_fallback(
    supa,
    table: str,
    batch: List[dict],
    *,
    on_conflict: str,
    cache_pk_col: Optional[str],
) -> Tuple[int, int, Set[Any]]:
    """
    Retorna (ok, err, inserted_keys_para_cache).
    Fallback: divide a lista até linha (e só então loga a linha problemática).
    """
    if not batch:
        return 0, 0, set()

    try:
        execute_with_retry(
            lambda: supa_upsert(supa, table, batch, on_conflict=on_conflict),
            max_retries=MAX_RETRIES,
        )
        inserted: Set[Any] = set()
        if cache_pk_col:
            inserted = {r.get(cache_pk_col) for r in batch if r.get(cache_pk_col) is not None}
        return len(batch), 0, inserted
    except Exception as e:
        if len(batch) == 1:
            row = batch[0]
            key = (
                row.get(cache_pk_col)
                if cache_pk_col and row.get(cache_pk_col) is not None
                else row.get("id")
                if row.get("id") is not None
                else "NO_ID"
            )
            log_error(table, key, str(e))
            return 0, 1, set()

        mid = len(batch) // 2
        ok1, err1, ins1 = upsert_with_fallback(
            supa,
            table,
            batch[:mid],
            on_conflict=on_conflict,
            cache_pk_col=cache_pk_col,
        )
        ok2, err2, ins2 = upsert_with_fallback(
            supa,
            table,
            batch[mid:],
            on_conflict=on_conflict,
            cache_pk_col=cache_pk_col,
        )
        return ok1 + ok2, err1 + err2, ins1.union(ins2)


def coerce_value(schema: PgSchema, table: str, pg_col: str, raw_val: Any) -> Any:
    pg_type = schema.col_type(table, pg_col)
    if pg_type == "uuid":
        if pg_col == "id":
            return uuid5_for(table, raw_val)
        ref_table = schema.fk_ref_table(table, pg_col)
        if ref_table:
            return uuid5_for(ref_table, raw_val)
        return uuid5_for(f"{table}:{pg_col}", raw_val)
    if pg_type == "int":
        return to_int(raw_val)
    if pg_type == "bool":
        return to_bool(raw_val)
    if pg_type == "numeric":
        return to_money(raw_val) if pg_col in MONEY_COLS else to_decimal(raw_val)
    if pg_type == "timestamp":
        return to_ts(raw_val)
    if pg_type == "date":
        return to_date(raw_val)
    if pg_type == "json":
        return raw_val
    # text / fallback
    if isinstance(raw_val, (dt.datetime, dt.date)):
        return raw_val.isoformat()
    return to_str(raw_val)


def transform_row(
    schema: PgSchema,
    *,
    table: str,
    row: dict,
    mapping: Dict[str, str],
    inserted_ids: Dict[str, Set[Any]],
    inserted_ref_values: Dict[Tuple[str, str], Set[Any]],
) -> Optional[dict]:
    out: Dict[str, Any] = {}
    pk_cols = schema.pk_cols.get(table) or ["id"]

    for pg_col, mysql_col in mapping.items():
        # If the destination schema doesn't have this column (schema/mapping drift), skip it safely.
        if schema.col_type(table, pg_col) is None:
            k = (table, pg_col)
            if k not in UNKNOWN_DEST_COLS:
                log(f"[WARN] {table}: destino nao tem coluna {pg_col}; pulando (mapping desatualizado?)")
                UNKNOWN_DEST_COLS.add(k)
            continue

        raw = row.get(mysql_col)

        # is_pedidos_fretes_envios.detalhes is TEXT but contains JSON object. When a json/jsonb column exists,
        # populate it with parsed JSON (dict) so PostgREST stores an object, not a JSON string.
        if table == "is_pedidos_fretes_envios" and pg_col == "detalhes_json":
            if isinstance(raw, (dict, list)):
                pass
            else:
                s = to_str(raw)
                if s:
                    try:
                        raw = json.loads(s)
                    except Exception:
                        raw = None
                else:
                    raw = None

        # is_pedidos_fretes_detalhes.endereco/conteudo are TEXT but store JSON objects in the legacy dump.
        # When json/jsonb columns exist, populate them with parsed JSON (dict) for queryability.
        if table in ("is_pedidos_fretes_detalhes", "is_pedidos_orcamentos_fretes_detalhes") and pg_col in (
            "endereco_json",
            "conteudo_json",
        ):
            if isinstance(raw, (dict, list)):
                pass
            else:
                s = to_str(raw)
                if s:
                    try:
                        raw = json.loads(s)
                    except Exception:
                        raw = None
                else:
                    raw = None

        # cupom vazio -> NULL (e também é FK por codigo em is_pedidos)
        if pg_col == "cupom":
            raw = to_str(raw)
            if raw == "":
                raw = None

        # Ajustes para CHECK constraints (schema Supabase)
        if table == "is_mkt_cupons" and pg_col == "tipo":
            s = (to_str(raw) or "").strip().lower()
            if s in ("%", "percent", "porcent", "porcentagem"):
                raw = "percent"
            elif s in ("$", "r$", "amount", "valor", "dinheiro"):
                raw = "amount"
            elif s in ("f", "frete", "gratis", "grátis", "free"):
                raw = "amount"
            else:
                log_error(table, row.get("id"), f"tipo inválido no legado: {s!r} -> amount")
                raw = "amount"

        if table == "is_mkt_cupons" and pg_col == "valor" and isinstance(raw, str):
            # Ex.: legacy pode vir como "8%" ao invés de 8
            raw = raw.replace("R$", "").replace("r$", "").replace("$", "").replace("%", "").strip()

        if table == "is_financeiro_lancamentos" and pg_col == "tipo":
            iv = to_int(raw)
            # No schema novo: CHECK(tipo IN (1,2)); no legado aparece 0/1
            if iv == 0:
                raw = 2
            elif iv in (1, 2):
                raw = iv
            else:
                log_error(table, row.get("id"), f"tipo inválido no legado: {raw!r} -> 1")
                raw = 1

        # Overrides de regra de sinal (não clampa para >=0 automaticamente)
        # - is_clientes_extratos.valor: pode ser negativo (crédito/débito)
        # - is_pedidos.desconto: pode vir negativo no legado; normalizamos após o parse
        if table == "is_clientes_extratos" and pg_col == "valor":
            val = to_decimal(raw)
        elif table == "is_pedidos" and pg_col == "desconto":
            val = to_decimal(raw)
        else:
            val = coerce_value(schema, table, pg_col, raw)

        # Validação de FK (só quando temos cache do pai)
        if VALIDATE_FKS and val is not None:
            fk = schema.fk_ref(table, pg_col)
            if fk:
                ref_table, ref_col = fk
                if ref_table in inserted_ids:
                    ref_pk_col = schema.cache_pk_col(ref_table)
                    if ref_pk_col and ref_col == ref_pk_col:
                        if val not in inserted_ids[ref_table]:
                            val = None
                    else:
                        ref_key = (ref_table, ref_col)
                        cache = inserted_ref_values.get(ref_key)
                        # Só valida se temos cache para esse ref_col; caso contrário, não zera (evita apagar dados válidos).
                        if cache is not None and val not in cache:
                            val = None
            else:
                ref_table = schema.fk_ref_table(table, pg_col)
                # fallback assume que referencia o PK do pai
                if ref_table and ref_table in inserted_ids and val not in inserted_ids[ref_table]:
                    val = None

        # Regras de nullabilidade/default:
        if val is None:
            if pg_col in pk_cols:
                return None
            if schema.is_not_null(table, pg_col) and not schema.has_col_default(table, pg_col):
                return None
            # omit -> DEFAULT/NULL no banco
            continue

        out[pg_col] = val

    # PK precisa existir no payload (mesmo que haja DEFAULT no banco, queremos idempotência)
    for pk in pk_cols:
        if pk not in out or out.get(pk) is None:
            return None

    if table == "is_pedidos":
        # Schema exige desconto >= 0. No legado, desconto pode vir negativo (equivalente a acréscimo).
        # Normalização: move a magnitude para acrescimo e zera desconto.
        desconto = out.get("desconto")
        if isinstance(desconto, (int, float)) and desconto < 0:
            acrescimo = out.get("acrescimo")
            acrescimo_f = float(acrescimo) if isinstance(acrescimo, (int, float)) else 0.0
            out["acrescimo"] = acrescimo_f + (-float(desconto))
            out["desconto"] = 0.0

    if table == "is_financeiro_lancamentos":
        # CHECK: no máximo 1 contraparte
        counterparty_cols = ["fornecedor_id", "pdv_id", "funcionario_id", "vendedor_id", "caixa_id"]
        present = [c for c in counterparty_cols if out.get(c) is not None]
        if len(present) > 1:
            priority = ["caixa_id", "fornecedor_id", "pdv_id", "funcionario_id", "vendedor_id"]
            keep = next((c for c in priority if out.get(c) is not None), present[0])
            for c in present:
                if c != keep:
                    out.pop(c, None)
            log_error(table, out.get("id"), f"multiple counterparties {present} -> kept {keep}")

    return out


def prune_unknown_cols(schema: PgSchema, *, table: str, row: dict) -> dict:
    """Remove chaves que nao existem no schema destino (evita erro do PostgREST)."""
    return {k: v for k, v in row.items() if schema.col_type(table, k) is not None}


def choose_on_conflict(schema: PgSchema, *, table: str, sample_row: dict) -> Optional[str]:
    """
    Decide `on_conflict` para upsert.

    - Preferimos PK quando o payload contem todas as colunas da PK.
    - Para tabelas curadas/derivadas com PK surrogate (`id`) e payload sem `id`, usamos chaves naturais/UNIQUE.
    """
    pk = schema.pk_cols.get(table) or []
    if pk and all(c in sample_row for c in pk):
        return ",".join(pk)

    # Fallbacks (fretes curados)
    if table in ("is_pedidos_fretes_retiradas", "is_pedidos_fretes_entregas"):
        # UNIQUE(envio_id) existe no schema atual
        if "envio_id" in sample_row:
            return "envio_id"
        if "pedido_id" in sample_row:
            return "pedido_id"

    if table in ("is_pedidos_fretes_detalhes_enderecos", "is_pedidos_fretes_detalhes_pacotes"):
        # UNIQUE(detalhe_id) existe no schema atual
        if "detalhe_id" in sample_row:
            return "detalhe_id"

    if table == "is_pedidos_fretes_detalhes_itens":
        # UNIQUE(detalhe_id, item_idx) existe no schema atual
        if "detalhe_id" in sample_row and "item_idx" in sample_row:
            return "detalhe_id,item_idx"

    return None


def transform_cliente(
    schema: PgSchema,
    row: dict,
    mapping: Dict[str, str],
    inserted_ids: Dict[str, Set[Any]],
    inserted_ref_values: Dict[Tuple[str, str], Set[Any]],
) -> Optional[dict]:
    out = transform_row(
        schema,
        table="is_clientes",
        row=row,
        mapping=mapping,
        inserted_ids=inserted_ids,
        inserted_ref_values=inserted_ref_values,
    )
    if not out:
        return None
    raw_tipo = to_str(row.get("tipo")) or ""
    t = raw_tipo.lower().strip()
    out["tipo"] = "PJ" if t in ("juridica", "pj") else "PF"
    if out.get("email_log"):
        out["email_log"] = str(out["email_log"]).lower().strip()
    return out


def transform_cliente_pf(row: dict) -> Optional[dict]:
    t = (to_str(row.get("tipo")) or "").lower()
    if t not in ("fisica", "pf"):
        return None
    cpf = to_str(row.get("cpf"))
    nome = to_str(row.get("nome")) or to_str(row.get("email_log")) or f"Cliente PF {row.get('id')}"
    if not cpf and not nome:
        return None
    cliente_id = uuid5_for("is_clientes", row.get("id"))
    if not cliente_id:
        return None
    return {
        "cliente_id": cliente_id,
        "nome": nome,
        "sobrenome": to_str(row.get("sobrenome")),
        "nascimento": to_ts(row.get("nascimento")),
        "cpf": cpf,
        "sexo": to_str(row.get("sexo")),
    }


def transform_cliente_pj(row: dict) -> Optional[dict]:
    t = (to_str(row.get("tipo")) or "").lower()
    if t not in ("juridica", "pj"):
        return None
    cnpj = to_str(row.get("cnpj"))
    razao = to_str(row.get("razao_social")) or to_str(row.get("fantasia")) or f"Cliente PJ {row.get('id')}"
    fantasia = to_str(row.get("fantasia"))
    if not cnpj and not razao and not fantasia:
        return None
    cliente_id = uuid5_for("is_clientes", row.get("id"))
    if not cliente_id:
        return None
    return {
        "cliente_id": cliente_id,
        "razao_social": razao,
        "fantasia": fantasia,
        "ie": to_str(row.get("ie")),
        "cnpj": cnpj,
    }


# Cache global de PKs inseridos (para validação de FK)
INSERTED_IDS: Dict[str, Set[Any]] = {}
INSERTED_REF_VALUES: Dict[Tuple[str, str], Set[Any]] = {}


def compute_ref_cache_cols(schema: PgSchema, tables: List[str]) -> Dict[str, Set[str]]:
    """
    Retorna colunas "referenciadas" por FKs (pai.col) para popular cache de validação de FK
    quando a FK não referencia o PK (ex.: is_pedidos.cupom -> is_mkt_cupons.codigo).
    """
    cols: Dict[str, Set[str]] = {}
    nodes = set(tables)
    for child in tables:
        for _child_col, (ref_table, ref_col) in schema.fk_refs.get(child, {}).items():
            if ref_table not in nodes:
                continue
            pk = schema.cache_pk_col(ref_table)
            if pk and ref_col == pk:
                continue
            cols.setdefault(ref_table, set()).add(ref_col)
    return cols


def update_ref_cache_for_batch(
    *,
    table: str,
    batch_rows: List[dict],
    inserted_pks: Set[Any],
    pk_col: Optional[str],
    ref_cache_cols: Dict[str, Set[str]],
    inserted_ref_values: Dict[Tuple[str, str], Set[Any]],
) -> None:
    if not pk_col or not inserted_pks:
        return
    cols = ref_cache_cols.get(table)
    if not cols:
        return
    for r in batch_rows:
        pk_val = r.get(pk_col)
        if pk_val is None or pk_val not in inserted_pks:
            continue
        for c in cols:
            v = r.get(c)
            if v is not None:
                inserted_ref_values.setdefault((table, c), set()).add(v)


# ORDEM TOPOLÓGICA (pais -> filhos)
EXECUTION_ORDER = [
    # Tier 0
    "is_visitas",
    "is_produtos_vars_nomes",
    "is_financeiro_centros_custo",
    "is_mensagens",
    "is_usuarios_tentativas",
    "is_entregas_fretes",
    "is_clientes",
    "is_financeiro_carteiras",
    "is_extras_status",
    "is_apps_whatsapp_msgs",
    "is_producao_setores",
    "is_entregas_balcoes",
    "is_config",
    "is_financeiro_funcionarios",
    "is_produtos_categorias",
    "is_financeiro_pdvs",
    "is_produtos_servicos",
    "is_financeiro_fornecedores",
    "is_mkt_regras",
    "is_arquivos",
    "is_paginas",
    "is_bancos",
    "is_produtos",
    "is_financeiro_categorias",
    "is_config_logs_curl",
    "is_financeiro_transportadoras",
    "is_mkt_banners",
    # Tier 1
    "is_entregas_fretes_locais",
    "is_clientes_enderecos",
    "is_mkt_cupons",
    "is_clientes_pj",
    "is_clientes_pf",
    "is_usuarios",
    "is_financeiro_conciliacoes",
    "is_produtos_categorias_extras",
    "is_produtos_vars",
    "is_produtos_skus",
    "is_produtos_imagens",
    "is_produtos_dem",
    "is_produtos_categorias_produtos",
    "is_produtos_relacoes",
    "is_produtos_avaliacoes",
    "is_entregas_fretes_produtos",
    "is_produtos_mt",
    "is_produtos_offset",
    "is_produtos_mt_regras",
    "is_produtos_fixo",
    "is_produtos_fixo_regras",
    "is_produtos_qtd",
    "is_produtos_servicos_vinculos",
    "is_produtos_dem_info",
    # Tier 2+
    "is_financeiro_notasfiscais",
    "is_mkt_cupons_produtos",
    "is_pedidos_orcamentos",
    "is_financeiro_caixas",
    "is_usuarios_acessos",
    "is_usuarios_paginas",
    "is_usuarios_historico",
    "is_financeiro_repeticoes",
    "is_pedidos_orcamentos_pagamentos",
    "is_pedidos_orcamentos_fretes_detalhes",
    "is_pedidos_orcamentos_fretes_envios",
    "is_pedidos_orcamentos_itens",
    "is_financeiro_lancamentos",
    "is_financeiro_caixas_movimentacoes",
    "is_pedidos",
    "is_financeiro_repeticoes_criadas",
    "is_pedidos_orcamentos_itens_vars",
    "is_pedidos_orcamentos_itens_servicos",
    "is_pedidos_fretes_envios",
    "is_pedidos_fretes_detalhes",
    "is_pedidos_itens",
    "is_pedidos_pagamentos",
    "is_pedidos_itens_reprovados",
    "is_pedidos_itens_vars",
    "is_pedidos_itens_servicos",
    "is_pedidos_itens_briefings",
    "is_pedidos_itens_brief_conversa",
    "is_pedidos_historico",
    "is_pedidos_itens_brief_alteracoes",
    "is_pedidos_pag_reprovados",
    "is_clientes_extratos",
    "is_pedidos_itens_briefings_anexos",
    "is_visitas_online",
]


def main() -> None:
    log("ETL v8 - Schema-aware Upsert + Fallback")

    if MAPPING_PATH is None or ERROR_DIR is None:
        init_runtime()
    if MAPPING_PATH is None or not MAPPING_PATH.exists():
        log(f"ERRO: mapping nao encontrado: {MAPPING_PATH}")
        return

    try:
        with MAPPING_PATH.open("r", encoding="utf-8") as f:
            mappings = json.load(f)
    except Exception as e:
        log(f"ERRO: não consegui ler {MAPPING_PATH.name}: {e}")
        return

    try:
        schema = load_pg_schema()
    except Exception as e:
        log(f"ERRO: não consegui carregar schema do Supabase (PG_*): {e}")
        return

    order = compute_load_order(schema, list(mappings.keys()))
    if ONLY_TABLES:
        order = [t for t in order if t in ONLY_TABLES]
        log(f"Filtro ETL_ONLY_TABLES ativo: {len(order)} tabela(s)")
    if PRINT_ORDER:
        log("Ordem de carga (topológica por FK):")
        for i, t in enumerate(order, 1):
            log(f"{i:02d}. {t}")
        return

    try:
        mysql = get_mysql()
    except Exception as e:
        log(f"ERRO MySQL: {e}")
        return

    if PREFLIGHT:
        issues = preflight_fk_not_null(mysql, schema, mappings)
        if issues:
            log("ERRO: FKs NOT NULL com valores 0/NULL ou órfãos no legado. Precisa relaxar no Supabase antes do ETL:")
            for t, col, ref, missing, orphans in issues:
                log(f"  {t}.{col} -> {ref} (missing={missing}, orphans={orphans})")
            log("")
            log("SQL sugerido (execute no Supabase antes do ETL):")
            for t, col, _ref, _missing, _orphans in issues:
                log(f'ALTER TABLE "{t}" ALTER COLUMN "{col}" DROP NOT NULL;')
            return

    try:
        cur = mysql.cursor(dictionary=True)
        supa = get_supabase()
    except Exception as e:
        log(f"ERRO Conexão: {e}")
        return

    # Inicializar cache de PKs (apenas para tabelas que aparecem no plano)
    for t in order:
        INSERTED_IDS[t] = set()
    INSERTED_IDS.setdefault("is_clientes_pf", set())
    INSERTED_IDS.setdefault("is_clientes_pj", set())
    # Tabelas derivadas (nao existem no dump; populadas a partir das tabelas raw)
    INSERTED_IDS.setdefault("is_pedidos_fretes_retiradas", set())
    INSERTED_IDS.setdefault("is_pedidos_fretes_entregas", set())
    INSERTED_IDS.setdefault("is_pedidos_fretes_detalhes_enderecos", set())
    INSERTED_IDS.setdefault("is_pedidos_fretes_detalhes_pacotes", set())
    INSERTED_IDS.setdefault("is_pedidos_fretes_detalhes_itens", set())
    INSERTED_REF_VALUES.clear()
    ref_cache_cols = compute_ref_cache_cols(schema, order)

    log(f"Migrando {len(order)} tabelas...")

    seen_sessions: Set[str] = set()
    seen_uniques: Dict[Tuple[str, str], Set[Any]] = {}

    pf_rows: List[dict] = []
    pj_rows: List[dict] = []
    frete_retirada_rows: List[dict] = []
    frete_entrega_rows: List[dict] = []
    frete_det_endereco_rows: List[dict] = []
    frete_det_pacote_rows: List[dict] = []
    frete_det_item_rows: List[dict] = []

    for table in order:
        mapping = mappings.get(table)
        if not mapping:
            continue

        on_conflict = schema.on_conflict(table)
        cache_pk_col = schema.cache_pk_col(table)

        log(f"[{table}]")
        batch_no = 0
        next_log_batch = 1

        # Colunas necessárias no SELECT
        cols = list(set(mapping.values()))
        if "id" not in cols:
            cols.append("id")

        # Campos extras para split PF/PJ
        if table == "is_clientes":
            for c in [
                "tipo",
                "nome",
                "sobrenome",
                "nascimento",
                "cpf",
                "sexo",
                "razao_social",
                "fantasia",
                "ie",
                "cnpj",
                "email_log",
            ]:
                if c not in cols:
                    cols.append(c)

        # Dedup de session
        if table == "is_visitas_online" and "id_session" not in cols:
            cols.append("id_session")

        try:
            cur.execute(f"SELECT {','.join([f'`{c}`' for c in cols])} FROM `{table}`")
        except Exception as e:
            log(f"  SKIP: {e}")
            continue

        batch: List[dict] = []
        ok_total, err_total = 0, 0

        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                try:
                    if table == "is_clientes":
                        t_row = transform_cliente(schema, row, mapping, INSERTED_IDS, INSERTED_REF_VALUES)
                        if not t_row:
                            continue

                        # email_log é UNIQUE NOT NULL: se duplicar no legado, torna único com sufixo determinístico
                        email = to_str(t_row.get("email_log")) or ""
                        if not email:
                            email = f"cliente-{short_key(row.get('id'))}@invalid.local"
                        key = (table, "email_log")
                        seen = seen_uniques.setdefault(key, set())
                        if email in seen:
                            fixed = make_unique_text(
                                schema,
                                table=table,
                                col="email_log",
                                base=email,
                                key=t_row.get("id") or row.get("id"),
                            )
                            log_error(table, t_row.get("id") or row.get("id"), f"unique fix email_log: {email} -> {fixed}")
                            email = fixed
                        seen.add(email)
                        t_row["email_log"] = email

                        pf = transform_cliente_pf(row)
                        if pf:
                            pf_rows.append(pf)
                        pj = transform_cliente_pj(row)
                        if pj:
                            pj_rows.append(pj)

                    else:
                        t_row = transform_row(
                            schema,
                            table=table,
                            row=row,
                            mapping=mapping,
                            inserted_ids=INSERTED_IDS,
                            inserted_ref_values=INSERTED_REF_VALUES,
                        )
                        if not t_row:
                            continue

                        # Derivar tabelas "modalidade" (retirada/entrega) a partir do JSON em detalhes.
                        # Essas tabelas sao para consumo (agentes/produto) e nao existem no dump.
                        if table == "is_pedidos_fretes_envios":
                            try:
                                envio_id = t_row.get("id")
                                pedido_id = t_row.get("pedido_id")
                                tipo = (to_str(row.get("tipo")) or "").lower().strip()
                                det_s = to_str(row.get("detalhes"))
                                det_obj = json.loads(det_s) if det_s else None
                                if not isinstance(det_obj, dict):
                                    det_obj = None

                                if pedido_id is None:
                                    # Existem poucos envios sem pedido_id no legado; mantemos no raw, mas nao entram nas tabelas por pedido.
                                    log_error("is_pedidos_fretes_envios", envio_id or row.get("id"), "pedido_id NULL; skip modalidade")
                                elif det_obj is None:
                                    log_error("is_pedidos_fretes_envios", envio_id or row.get("id"), "detalhes nao e JSON object; skip modalidade")
                                elif tipo == "retirada":
                                    legacy_balcao = det_obj.get("id")
                                    balcao_id = None
                                    if legacy_balcao not in (None, "", 0, "0"):
                                        balcao_id = uuid5_for("is_entregas_balcoes", legacy_balcao)
                                        # Valida contra cache do pai quando disponivel (evita FK orfa).
                                        # Em execucoes parciais (ETL_ONLY_TABLES) o cache pode nao existir; nesse caso, nao zera.
                                        if balcao_id:
                                            balcao_cache = INSERTED_IDS.get("is_entregas_balcoes")
                                            if balcao_cache is not None and balcao_id not in balcao_cache:
                                                balcao_id = None

                                    frete_retirada_rows.append(
                                        {
                                            "pedido_id": pedido_id,
                                            "envio_id": envio_id,
                                            "balcao_id": balcao_id,
                                            "balcao_titulo": to_str(det_obj.get("titulo")),
                                            "balcao_telefone": to_str(det_obj.get("telefone")),
                                            "balcao_logradouro": to_str(det_obj.get("logradouro")),
                                            "balcao_cep": to_str(det_obj.get("cep")),
                                            "balcao_complemento": to_str(det_obj.get("complemento")),
                                            "balcao_bairro": to_str(det_obj.get("bairro")),
                                            "cidade": to_str(det_obj.get("cidade")),
                                            "estado": to_str(det_obj.get("estado")),
                                            "prazo_dias": to_int(det_obj.get("prazo")),
                                            "data_snapshot": to_ts(det_obj.get("data")),
                                        }
                                    )
                                elif tipo == "entrega":
                                    frete_entrega_rows.append(
                                        {
                                            "pedido_id": pedido_id,
                                            "envio_id": envio_id,
                                            "metodo_titulo": to_str(det_obj.get("titulo")),
                                            "modulo": to_str(det_obj.get("modulo")),
                                            "prazo_dias": to_int(det_obj.get("prazo")),
                                            "valor": to_money(det_obj.get("valor")),
                                            "sucesso": to_bool(det_obj.get("sucesso")),
                                            "hash": to_str(det_obj.get("hash")),
                                            "descricao": to_str(det_obj.get("descricao")),
                                        }
                                    )
                                else:
                                    log_error("is_pedidos_fretes_envios", envio_id or row.get("id"), f"tipo inesperado: {tipo!r}")
                            except Exception as e:
                                log_error("is_pedidos_fretes_envios", t_row.get("id") or row.get("id"), f"derive modalidade error: {e}")

                        # Derivar tabelas curadas de frete_detalhes (endereco/pacote/itens) a partir de JSON em TEXT.
                        if table == "is_pedidos_fretes_detalhes":
                            try:
                                detalhe_id = t_row.get("id")
                                pedido_id = t_row.get("pedido_id")

                                end_s = to_str(row.get("endereco"))
                                end_obj = json.loads(end_s) if end_s else None
                                if not isinstance(end_obj, dict):
                                    end_obj = None

                                cont_s = to_str(row.get("conteudo"))
                                cont_obj = json.loads(cont_s) if cont_s else None
                                if not isinstance(cont_obj, dict):
                                    cont_obj = None

                                if pedido_id is None:
                                    log_error("is_pedidos_fretes_detalhes", detalhe_id or row.get("id"), "pedido_id NULL; skip derived")
                                else:
                                    frete_det_endereco_rows.append(
                                        {
                                            "detalhe_id": detalhe_id,
                                            "pedido_id": pedido_id,
                                            "destinatario_nome": to_str((end_obj or {}).get("destinatario_nome")),
                                            "destinatario_documento": to_str((end_obj or {}).get("destinatario_documento")),
                                            "destinatario_tipo": to_str((end_obj or {}).get("destinatario_tipo")),
                                            "destinatario_pais": to_str((end_obj or {}).get("destinatario_pais")),
                                            "cep": to_str((end_obj or {}).get("cep")),
                                            "logradouro": to_str((end_obj or {}).get("logradouro")),
                                            "numero": to_str((end_obj or {}).get("numero")),
                                            "complemento": to_str((end_obj or {}).get("complemento")),
                                            "bairro": to_str((end_obj or {}).get("bairro")),
                                            "cidade": to_str((end_obj or {}).get("cidade")),
                                            "estado": to_str((end_obj or {}).get("estado")),
                                        }
                                    )

                                    pacote = (cont_obj or {}).get("pacote")
                                    pacote_obj = pacote if isinstance(pacote, dict) else {}
                                    itens = (cont_obj or {}).get("itens")
                                    itens_list = itens if isinstance(itens, list) else None

                                    frete_det_pacote_rows.append(
                                        {
                                            "detalhe_id": detalhe_id,
                                            "pedido_id": pedido_id,
                                            "itens_count": len(itens_list) if itens_list is not None else None,
                                            "pacote_altura": to_decimal(pacote_obj.get("altura")),
                                            "pacote_largura": to_decimal(pacote_obj.get("largura")),
                                            "pacote_comprimento": to_decimal(pacote_obj.get("comprimento")),
                                            "pacote_peso": to_decimal(pacote_obj.get("peso")),
                                            "pacote_produtos": to_int(pacote_obj.get("produtos")),
                                            "pacote_volumes": to_int(pacote_obj.get("volumes")),
                                            "pacote_valor_declarado": to_decimal(pacote_obj.get("valor_declarado")),
                                            "pacote_produtos_ids": pacote_obj.get("produtos_ids"),
                                        }
                                    )

                                    if itens_list:
                                        for idx, it in enumerate(itens_list, 1):
                                            if not isinstance(it, dict):
                                                continue
                                            frete_det_item_rows.append(
                                                {
                                                    "detalhe_id": detalhe_id,
                                                    "item_idx": idx,
                                                    "pedido_id": pedido_id,
                                                    "entrega": to_str(it.get("entrega")),
                                                    "gratis": to_bool(it.get("gratis")),
                                                    "legacy_produto_id": to_str(it.get("id")),
                                                    "quantidade": to_int(it.get("quantidade")),
                                                    "volumes": to_int(it.get("volumes")),
                                                    "valor_declarado": to_decimal(it.get("valor_declarado")),
                                                    "altura": to_decimal(it.get("altura")),
                                                    "largura": to_decimal(it.get("largura")),
                                                    "comprimento": to_decimal(it.get("comprimento")),
                                                    "peso": to_str(it.get("peso")),
                                                    "lc": to_int(it.get("lc")),
                                                }
                                            )
                            except Exception as e:
                                log_error("is_pedidos_fretes_detalhes", t_row.get("id") or row.get("id"), f"derive frete_detalhes error: {e}")

                        if table == "is_visitas_online" and DEDUP_VISITAS_ONLINE:
                            session = to_str(row.get("id_session"))
                            if session:
                                if session in seen_sessions:
                                    continue
                                seen_sessions.add(session)

                        # Dedup de UNIQUE single-column (quando for nullable)
                        for ucol in schema.unique_single_cols.get(table, set()):
                            if ucol in (schema.pk_cols.get(table) or []):
                                continue
                            if ucol not in t_row:
                                continue
                            v = t_row.get(ucol)
                            if v is None:
                                continue
                            ukey = (table, ucol)
                            seen = seen_uniques.setdefault(ukey, set())
                            if v in seen:
                                if schema.is_not_null(table, ucol):
                                    if schema.col_type(table, ucol) == "text":
                                        row_key = t_row.get(cache_pk_col) or t_row.get("id") or row.get("id")
                                        fixed = make_unique_text(schema, table=table, col=ucol, base=str(v), key=row_key)
                                        if fixed in seen:
                                            fixed = make_unique_text(schema, table=table, col=ucol, base=str(v), key=f"{row_key}-{len(seen)}")
                                        t_row[ucol] = fixed
                                        log_error(table, row_key, f"unique fix {ucol}: {v} -> {fixed}")
                                        seen.add(fixed)
                                        continue
                                    # Não dá pra tornar único com segurança; melhor pular linha
                                    log_error(table, t_row.get(cache_pk_col) or t_row.get("id"), f"duplicate unique {ucol}={v}")
                                    t_row = None
                                    break
                                # nullable: remove o campo (vira NULL no insert)
                                t_row.pop(ucol, None)
                            else:
                                seen.add(v)

                        if not t_row:
                            continue

                    batch.append(t_row)
                except Exception as e:
                    err_total += 1
                    log_error(table, row.get("id"), f"Transform error: {e}")

            if len(batch) >= BATCH_SIZE:
                ok, err, inserted = upsert_with_fallback(
                    supa,
                    table,
                    batch,
                    on_conflict=on_conflict,
                    cache_pk_col=cache_pk_col,
                )
                ok_total += ok
                err_total += err
                batch_no += 1
                if LOG_EVERY_BATCHES > 0 and batch_no >= next_log_batch:
                    log(f"  Batch {batch_no}: +OK={ok}, +ERR={err} (tot OK={ok_total}, ERR={err_total})")
                    next_log_batch += LOG_EVERY_BATCHES
                if cache_pk_col:
                    INSERTED_IDS[table].update(inserted)
                    update_ref_cache_for_batch(
                        table=table,
                        batch_rows=batch,
                        inserted_pks=inserted,
                        pk_col=cache_pk_col,
                        ref_cache_cols=ref_cache_cols,
                        inserted_ref_values=INSERTED_REF_VALUES,
                    )
                batch = []

        if batch:
            ok, err, inserted = upsert_with_fallback(
                supa,
                table,
                batch,
                on_conflict=on_conflict,
                cache_pk_col=cache_pk_col,
            )
            ok_total += ok
            err_total += err
            batch_no += 1
            if LOG_EVERY_BATCHES > 0 and batch_no >= next_log_batch:
                log(f"  Batch {batch_no}: +OK={ok}, +ERR={err} (tot OK={ok_total}, ERR={err_total})")
                next_log_batch += LOG_EVERY_BATCHES
            if cache_pk_col:
                INSERTED_IDS[table].update(inserted)
                update_ref_cache_for_batch(
                    table=table,
                    batch_rows=batch,
                    inserted_pks=inserted,
                    pk_col=cache_pk_col,
                    ref_cache_cols=ref_cache_cols,
                    inserted_ref_values=INSERTED_REF_VALUES,
                )

        log(f"  OK={ok_total}, ERR={err_total}, Cached={len(INSERTED_IDS[table])}")

        # Inserir PF/PJ logo após clientes (reduz dependências e facilita rerun)
        if table == "is_clientes":
            # Filtrar apenas clientes que realmente foram inseridos no run (dedup pode ter removido alguns)
            if INSERTED_IDS.get("is_clientes"):
                pf_rows[:] = [r for r in pf_rows if r.get("cliente_id") in INSERTED_IDS["is_clientes"]]
                pj_rows[:] = [r for r in pj_rows if r.get("cliente_id") in INSERTED_IDS["is_clientes"]]

            for pf_table, rows_list in (("is_clientes_pf", pf_rows), ("is_clientes_pj", pj_rows)):
                if not rows_list:
                    continue
                log(f"[{pf_table}] {len(rows_list)} registros")
                pf_on_conflict = schema.on_conflict(pf_table)
                pf_cache_pk = schema.cache_pk_col(pf_table)
                for i in range(0, len(rows_list), BATCH_SIZE):
                    sub = rows_list[i : i + BATCH_SIZE]
                    ok, err, inserted = upsert_with_fallback(
                        supa,
                        pf_table,
                        sub,
                        on_conflict=pf_on_conflict,
                        cache_pk_col=pf_cache_pk,
                    )
                    log(f"  Batch {i // BATCH_SIZE + 1}: OK={ok}, ERR={err}")
                    if pf_cache_pk:
                        INSERTED_IDS[pf_table].update(inserted)

            # Evita reinserir ao final do script
            pf_rows.clear()
            pj_rows.clear()

        # Inserir tabelas de modalidade (retirada/entrega) logo após fretes_envios
        if table == "is_pedidos_fretes_envios":
            inserted_envios = INSERTED_IDS.get("is_pedidos_fretes_envios", set())
            inserted_pedidos = INSERTED_IDS.get("is_pedidos", set())
            for derived_table, rows_list in (
                ("is_pedidos_fretes_retiradas", frete_retirada_rows),
                ("is_pedidos_fretes_entregas", frete_entrega_rows),
            ):
                if not rows_list:
                    continue
                # Evita FK failure: insere apenas derivados cujos pais foram inseridos neste run.
                if inserted_envios:
                    rows_list[:] = [r for r in rows_list if r.get("envio_id") in inserted_envios]
                if inserted_pedidos:
                    rows_list[:] = [r for r in rows_list if r.get("pedido_id") in inserted_pedidos]
                rows_list[:] = [prune_unknown_cols(schema, table=derived_table, row=r) for r in rows_list]
                rows_list[:] = [r for r in rows_list if r]

                if not rows_list:
                    continue
                if derived_table not in schema.col_types:
                    log(f"[WARN] {derived_table} nao existe no schema; pulei ({len(rows_list)} linhas derivadas)")
                    continue
                sample = rows_list[0]
                d_on_conflict = choose_on_conflict(schema, table=derived_table, sample_row=sample)
                if not d_on_conflict:
                    log(f"[WARN] {derived_table}: nao consegui definir on_conflict (payload keys={sorted(sample.keys())}); pulei")
                    continue
                log(f"[{derived_table}] {len(rows_list)} registros (derivado)")
                d_cache_pk = schema.cache_pk_col(derived_table) if d_on_conflict == schema.on_conflict(derived_table) else None
                for i in range(0, len(rows_list), BATCH_SIZE):
                    sub = rows_list[i : i + BATCH_SIZE]
                    ok, err, inserted = upsert_with_fallback(
                        supa,
                        derived_table,
                        sub,
                        on_conflict=d_on_conflict,
                        cache_pk_col=d_cache_pk,
                    )
                    log(f"  Batch {i // BATCH_SIZE + 1}: OK={ok}, ERR={err}")
                    if d_cache_pk:
                        INSERTED_IDS[derived_table].update(inserted)

            frete_retirada_rows.clear()
            frete_entrega_rows.clear()

        # Inserir tabelas curadas (endereco/pacote/itens) logo apos fretes_detalhes
        if table == "is_pedidos_fretes_detalhes":
            inserted_det = INSERTED_IDS.get("is_pedidos_fretes_detalhes", set())
            inserted_pedidos = INSERTED_IDS.get("is_pedidos", set())
            for derived_table, rows_list in (
                ("is_pedidos_fretes_detalhes_enderecos", frete_det_endereco_rows),
                ("is_pedidos_fretes_detalhes_pacotes", frete_det_pacote_rows),
                ("is_pedidos_fretes_detalhes_itens", frete_det_item_rows),
            ):
                if not rows_list:
                    continue
                # Evita FK failure: insere apenas derivados cujos pais foram inseridos neste run (quando cache disponivel).
                if inserted_det:
                    rows_list[:] = [r for r in rows_list if r.get("detalhe_id") in inserted_det]
                if inserted_pedidos:
                    rows_list[:] = [r for r in rows_list if r.get("pedido_id") in inserted_pedidos]
                rows_list[:] = [prune_unknown_cols(schema, table=derived_table, row=r) for r in rows_list]
                rows_list[:] = [r for r in rows_list if r]
                if not rows_list:
                    continue
                if derived_table not in schema.col_types:
                    log(f"[WARN] {derived_table} nao existe no schema; pulei ({len(rows_list)} linhas derivadas)")
                    continue
                sample = rows_list[0]
                d_on_conflict = choose_on_conflict(schema, table=derived_table, sample_row=sample)
                if not d_on_conflict:
                    log(f"[WARN] {derived_table}: nao consegui definir on_conflict (payload keys={sorted(sample.keys())}); pulei")
                    continue
                log(f"[{derived_table}] {len(rows_list)} registros (derivado)")
                d_cache_pk = schema.cache_pk_col(derived_table) if d_on_conflict == schema.on_conflict(derived_table) else None
                for i in range(0, len(rows_list), BATCH_SIZE):
                    sub = rows_list[i : i + BATCH_SIZE]
                    ok, err, inserted = upsert_with_fallback(
                        supa,
                        derived_table,
                        sub,
                        on_conflict=d_on_conflict,
                        cache_pk_col=d_cache_pk,
                    )
                    log(f"  Batch {i // BATCH_SIZE + 1}: OK={ok}, ERR={err}")
                    if d_cache_pk:
                        INSERTED_IDS[derived_table].update(inserted)

            frete_det_endereco_rows.clear()
            frete_det_pacote_rows.clear()
            frete_det_item_rows.clear()

    log("FIM")


def run_etl_v8(*, repo_root: Path | None = None, logger=None) -> None:
    """Entry-point used by `python -m src.main`."""
    init_runtime(repo_root=repo_root, logger=logger)
    main()


if __name__ == "__main__":
    run_etl_v8()
