#!/usr/bin/env python3
"""
ETL v10 - Migracaoo MySQL -> Supabase (psycopg2 direto)
========================================================

COMO USAR:
1. Configure o .env com as credenciais
2. Execute: python etl/run.py

Variaveis de controle:
  ETL_VALIDATE_ONLY=1   -> valida mapping vs schema e sai sem escrever
  ETL_ONLY_TABLES=t1,t2 -> roda apenas as tabelas listadas
  ETL_BATCH_SIZE=2000   -> tamanho do batch de insert
"""

import os
import re
import uuid
import json
import sys
import datetime as dt
from typing import Optional, Dict, Any, Set, List, Tuple
from dotenv import load_dotenv

# Encoding fix para Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# ============================================================================
# CONFIGURACAO
# ============================================================================
UUID_NS = uuid.UUID("2a6b2c31-0f2a-4dfd-8cde-7b4b9b3f1c5a")
BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "2000"))
VALIDATE_ONLY = os.getenv("ETL_VALIDATE_ONLY", "0") == "1"
ONLY_TABLES_ENV = os.getenv("ETL_ONLY_TABLES", "").strip()
ONLY_TABLES: Set[str] = set(t.strip() for t in ONLY_TABLES_ENV.split(",") if t.strip())

# MySQL table name overrides: when Supabase table name differs from MySQL
MYSQL_TABLE_NAME_MAP = {
    # Supabase name -> MySQL name (when they differ)
    "is_pedidos_fretes_entregas": "is_pedidos_fretes_envios",
}

# Per-table column type overrides (takes precedence over global BOOL_COLS/INT_COLS)
# Fixes schema mismatches where a column name exists in multiple tables with different types
TABLE_TYPE_OVERRIDES: Dict[str, Dict[str, str]] = {
    "is_extras_status": {"visivel": "int"},                # schema: integer, not boolean
    "is_mkt_cupons": {"tipo": "str"},                      # schema: varchar ('percent'/'amount')
    "is_producao_setores": {"status": "str"},              # schema: varchar, not integer
    "is_pedidos_itens": {"status": "str"},                 # schema: varchar, not integer
    "is_pedidos_pagamentos": {"visto": "bool"},            # schema: boolean, not integer
    "is_produtos": {"valor": "str"},                       # schema: varchar (display text)
    "is_financeiro_lancamentos": {"valor": "money"},       # schema: CHECK (valor >= 0)
}

# Tables without an 'id' column in Supabase (composite PK)
TABLES_WITHOUT_ID: Set[str] = {"is_mkt_cupons_produtos"}

# ============================================================================
# HELPERS
# ============================================================================
def log(msg: str, level: str = "INFO"):
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


def uuid5_for(table: str, legacy_id: Any) -> Optional[str]:
    if not legacy_id or str(legacy_id).strip() in ("0", "None", "", "null"):
        return None
    return str(uuid.uuid5(UUID_NS, f"{table}:{legacy_id}"))


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
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s in ("", "none", "null", "None"):
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def to_money(val: Any) -> float:
    v = to_decimal(val)
    return max(0.0, v) if v is not None else 0.0


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


def to_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s not in ("None", "null") else None


def clean_string(val: Any) -> Optional[str]:
    """Limpa strings: corrige encoding (mojibake) e remove caracteres especiais."""
    s = to_str(val)
    if not s:
        return None
    # Tentar corrigir Mojibake (UTF-8 lido como Latin-1)
    try:
        s = s.encode("latin1").decode("utf-8")
    except Exception:
        pass
    # Remover caracteres indesejados
    s = re.sub(r"[_@%]", " ", s)
    return s.strip()


# ============================================================================
# CONEXOES
# ============================================================================
def get_mysql():
    import mysql.connector
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3307")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database=os.getenv("MYSQL_DATABASE", "legacy"),
        charset="utf8mb4",
        use_unicode=True,
        use_pure=True,
    )


def get_pg():
    """Connect directly to Supabase PostgreSQL via psycopg2."""
    import psycopg2
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL is required for direct PostgreSQL connection")
    conn = psycopg2.connect(db_url)
    return conn


# ============================================================================
# MAPEAMENTOS DE TIPOS E FKs
# ============================================================================

# FK: coluna -> tabela referenciada (usada para gerar UUID5)
FK_MAP = {
    "cliente_id":       "is_clientes",
    "usuario_id":       "is_usuarios",
    "produto_id":       "is_produtos",
    "pedido_id":        "is_pedidos",
    "item_id":          "is_pedidos_itens",
    "comprovante_id":   "is_pedidos_pagamentos",
    "pagamento_id":     "is_pedidos_pagamentos",
    "original_id":      "is_pedidos_pagamentos",   # self-ref (2-pass)
    "frete_balcao_id":  "is_entregas_balcoes",
    "frete_endereco_id":"is_clientes_enderecos",
    "balcao_id":        "is_entregas_balcoes",
    "frete_id":         "is_entregas_fretes",
    "grupo_id":         "is_produtos_vars_nomes",
    "funcionario_id":   "is_financeiro_funcionarios",
    "vendedor_id":      "is_usuarios",
    "operador_id":      "is_usuarios",
    # Colunas cujas tabelas referenciadas foram removidas do schema novo
    # (mantemos UUID5 para preservar dado, sem FK constraint no Supabase)
    "categoria_id":     "_orphan",   # is_financeiro_categorias removida
    "carteira_id":      "_orphan",
    "fornecedor_id":    "_orphan",
    "pdv_id":           "_orphan",
    "caixa_id":         "_orphan",
    "centro_custo_id":  "_orphan",
    "arquivo_id":       "_orphan",
    "envio_id":         "_orphan",   # UUID simples, nao FK real
    # Self-reference tratada caso a caso
    "parent_id":        None,
}

# Colunas BOOLEAN
BOOL_COLS = {
    "visivel", "arquivado", "primeira_compra", "arte", "estoque_controlar",
    "vars_obrig", "individual", "embalagem", "devolucao_completa", "pago",
    "oculto", "is_principal", "sucesso",
}

# Colunas TIMESTAMP / DATE
TS_COLS = {
    "created_at", "updated_at", "data", "ultimo_acesso", "nascimento",
    "oferta_expira", "admissao", "demissao", "aprovacao", "previsao_producao",
    "previsao_entrega", "arte_data", "data_modificado", "inicio", "fim",
    "vencimento", "data_pagto", "data_emissao", "emissao", "saida",
}

# Colunas MONEY (valores nao negativos)
MONEY_COLS = {
    "total", "acrescimo", "desconto", "desconto_uso", "sinal",
    "frete_valor", "taxa", "custo", "salario", "vale",
    "valor_arte", "min_compra", "saldo_inicial",
}

# Colunas INT (nao UUID, nao bool, nao timestamp)
INT_COLS = {
    "status", "acesso", "tipo", "origem", "repetir", "agrupar", "neutro",
    "arte_status", "visto", "categoria", "revendedor", "revenda_tipo",
    "vars_select", "vars_agrupadas", "vars_combinacao", "parcelas_qtd",
    "conciliacao_movimentacao", "conciliacao_pagto", "num",
    "vendidos", "estoque_qtde", "uso", "limite", "estoque", "cobranca",
    "cobranca_val", "prazo", "minimo_c", "limite_c", "prazo_dias",
    "salario_vencimento", "vale_vencimento",
}

# ============================================================================
# ORDEM TOPOLOGICA -- 30 tabelas ETL (novo schema)
# Respeita todas as FKs do schema_ref.sql
# ============================================================================
EXEC_ORDER = [
    # Nivel 0: sem dependencias
    "is_extras_status",            # PK INT -- tratamento especial
    "is_entregas_balcoes",
    "is_entregas_fretes",
    "is_financeiro_funcionarios",
    "is_mkt_regras",
    "is_producao_setores",
    "is_produtos",
    "is_produtos_vars_nomes",
    "is_clientes",
    # Nivel 1
    "is_usuarios",                 # dep: is_entregas_balcoes
    "is_clientes_pf",              # dep: is_clientes | conflict: cliente_id
    "is_clientes_pj",              # dep: is_clientes | conflict: cliente_id
    "is_clientes_enderecos",       # dep: is_clientes
    "is_entregas_fretes_locais",   # dep: is_entregas_fretes
    "is_produtos_categorias",      # self-ref: 2-pass para parent_id
    "is_produtos_vars",            # dep: is_produtos, is_produtos_vars_nomes
    "is_produtos_categorias_extras",  # dep: is_produtos
    # Nivel 2
    "is_mkt_cupons",               # dep: is_clientes
    "is_mkt_cupons_produtos",      # dep: is_mkt_cupons, is_produtos | PK composto
    "is_financeiro_lancamentos",   # dep: is_financeiro_funcionarios, is_usuarios
    "is_pedidos",                  # dep: is_clientes, is_usuarios, is_entregas_balcoes,
                                   #      is_clientes_enderecos, is_mkt_cupons(codigo)
    # Nivel 3
    "is_pedidos_fretes_detalhes",  # dep: is_pedidos
    "is_pedidos_fretes_entregas",  # dep: is_pedidos
    "is_pedidos_itens",            # dep: is_pedidos, is_produtos
    "is_pedidos_pagamentos",       # dep: is_clientes, is_pedidos, is_usuarios (self-ref: 2-pass)
    "is_usuarios_historico",       # dep: is_usuarios, is_clientes
    # Nivel 4
    "is_clientes_extratos",        # dep: is_clientes, is_pedidos, is_pedidos_pagamentos
    "is_pedidos_historico",        # dep: is_pedidos, is_pedidos_itens, is_extras_status, is_usuarios
    "is_pedidos_itens_reprovados", # dep: is_pedidos_itens, is_usuarios
    "is_pedidos_pag_reprovados",   # dep: is_pedidos_pagamentos, is_usuarios
]

# Tabelas com conflict_col diferente de "id"
CONFLICT_COLS: Dict[str, str] = {
    "is_clientes_pf":       "cliente_id",
    "is_clientes_pj":       "cliente_id",
    "is_mkt_cupons_produtos": "cupom_id,produto_id",
}

# Tabelas com self-referencing FK que precisam de 2-pass
SELF_REF_TABLES: Dict[str, str] = {
    "is_produtos_categorias": "parent_id",
    "is_pedidos_pagamentos":  "original_id",
}


# ============================================================================
# TRANSFORMACOES
# ============================================================================
def transform_row(
    row: dict, table: str, mapping: Dict[str, str]
) -> Optional[dict]:
    """Transforma uma row do MySQL para o formato do Supabase."""
    new_row: dict = {}
    legacy_id = row.get("id")

    # PK handling: tables without 'id' column use composite PK
    if table in TABLES_WITHOUT_ID:
        # No 'id' column in Supabase - don't generate one
        if not legacy_id:
            return None  # Still validate row exists in MySQL
    elif table == "is_extras_status":
        new_row["id"] = to_int(legacy_id)
        if new_row["id"] is None:
            return None
    elif legacy_id:
        new_row["id"] = uuid5_for(table, legacy_id)
    else:
        return None  # Sem ID valido

    # Per-table type overrides
    overrides = TABLE_TYPE_OVERRIDES.get(table, {})

    for pg_col, mysql_col in mapping.items():
        if pg_col == "id":
            continue

        val = row.get(mysql_col)

        # --- Per-table type override (highest priority) ---
        override = overrides.get(pg_col)
        if override:
            if override == "int":
                new_row[pg_col] = to_int(val)
            elif override == "bool":
                new_row[pg_col] = to_bool(val)
            elif override == "str":
                new_row[pg_col] = to_str(val)
            elif override == "money":
                new_row[pg_col] = to_money(val)
            elif override == "decimal":
                new_row[pg_col] = to_decimal(val)
            elif override == "ts":
                new_row[pg_col] = to_ts(val)
            continue

        # --- FK columns -> UUID5 ---
        if pg_col.endswith("_id"):
            # status_id e INT (references is_extras_status)
            if pg_col == "status_id":
                new_row[pg_col] = to_int(val)
                continue

            ref = FK_MAP.get(pg_col)

            if ref is None:
                # Self-reference: usa a propria tabela
                new_row[pg_col] = uuid5_for(table, val) if val else None
            elif ref == "_orphan":
                # Tabela removida do schema: gera UUID5 arbitrario para preservar dado
                orphan_table = pg_col[:-3] + "s"  # heuristica: remove _id, pluraliza
                new_row[pg_col] = uuid5_for(orphan_table, val) if val else None
            else:
                new_row[pg_col] = uuid5_for(ref, val) if val else None
            continue

        # --- String columns com limpeza profunda ---
        if any(
            x in pg_col
            for x in ("descricao", "nome", "titulo", "sobrenome", "razao_social",
                       "fantasia", "obs", "acao", "motivo")
        ):
            new_row[pg_col] = clean_string(val)
            continue

        # --- Boolean ---
        if pg_col in BOOL_COLS:
            new_row[pg_col] = to_bool(val)
            continue

        # --- Timestamp / Date ---
        if pg_col in TS_COLS:
            new_row[pg_col] = to_ts(val)
            continue

        # --- Money (nao-negativo) ---
        if pg_col in MONEY_COLS:
            new_row[pg_col] = to_money(val)
            continue

        # --- Integer explicito ---
        if pg_col in INT_COLS:
            new_row[pg_col] = to_int(val)
            continue

        # --- Decimal por padrao para "valor", "saldo", "preco", "qtde" ---
        if any(x in pg_col for x in ("valor", "preco", "saldo", "custo", "qtde",
                                      "taxa", "desconto", "acrescimo")):
            new_row[pg_col] = to_decimal(val)
            continue

        # --- Fallback ---
        new_row[pg_col] = to_str(val) if isinstance(val, str) else val

    # Limpar valores vazios problematicos
    if "cupom" in new_row and not new_row.get("cupom"):
        new_row["cupom"] = None
    if "sku" in new_row and new_row.get("sku") == "":
        new_row["sku"] = None

    return new_row


def transform_cliente(row: dict, mapping: dict) -> Optional[dict]:
    d = transform_row(row, "is_clientes", mapping)
    if d:
        t = str(row.get("tipo", "")).lower()
        d["tipo"] = "PJ" if t in ("juridica", "pj") else "PF"
        if d.get("email_log"):
            d["email_log"] = d["email_log"].lower().strip()
    return d


def transform_cliente_pf(row: dict) -> Optional[dict]:
    t = str(row.get("tipo", "")).lower()
    if t not in ("fisica", "pf"):
        return None
    cliente_id = uuid5_for("is_clientes", row.get("id"))
    if not cliente_id:
        return None
    return {
        "cliente_id": cliente_id,
        "nome":       clean_string(row.get("nome")),
        "sobrenome":  clean_string(row.get("sobrenome")),
        "nascimento": to_ts(row.get("nascimento")),
        "cpf":        to_str(row.get("cpf")),
        "sexo":       to_str(row.get("sexo")),
    }


def transform_cliente_pj(row: dict) -> Optional[dict]:
    t = str(row.get("tipo", "")).lower()
    if t not in ("juridica", "pj"):
        return None
    cliente_id = uuid5_for("is_clientes", row.get("id"))
    if not cliente_id:
        return None
    return {
        "cliente_id":   cliente_id,
        "razao_social": clean_string(row.get("razao_social")),
        "fantasia":     clean_string(row.get("fantasia")),
        "ie":           to_str(row.get("ie")),
        "cnpj":         to_str(row.get("cnpj")),
    }


# ============================================================================
# VALIDACAO DE SCHEMA
# ============================================================================
def parse_schema_tables(schema_path: str) -> Dict[str, Set[str]]:
    """Parseia schema_ref.sql e retorna {tabela: {colunas}}."""
    tables: Dict[str, Set[str]] = {}
    current: Optional[str] = None
    cols: Set[str] = set()

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                m = re.match(r"CREATE TABLE\s+(?:\w+\.)?(\w+)\s*\(", line, re.IGNORECASE)
                if m:
                    current = m.group(1)
                    cols = set()
                elif current and line.startswith(");"):
                    tables[current] = cols
                    current = None
                elif current and line and not line.startswith("--"):
                    col_m = re.match(r'"?(\w+)"?\s+\w+', line)
                    if col_m:
                        cols.add(col_m.group(1))
    except FileNotFoundError:
        log("schema_ref.sql nao encontrado -- validacao de colunas desativada", "WARN")

    return tables


def validate_mapping(mappings: dict, schema_tables: Dict[str, Set[str]]) -> bool:
    """Valida que todas as tabelas/colunas do mapping existem no schema."""
    ok = True
    schema_table_names = set(schema_tables.keys())

    log("\n--- Validacao de Mapping vs Schema ---")

    for table, col_map in mappings.items():
        if table not in schema_table_names:
            log(f"  [MISS_TABLE] {table} nao esta no schema_ref.sql", "WARN")
            continue

        schema_cols = schema_tables[table]
        for pg_col in col_map:
            if pg_col not in schema_cols:
                log(f"  [MISS_COL]  {table}.{pg_col} nao existe no schema", "ERROR")
                ok = False

    # Verificar que todas as tabelas do EXEC_ORDER tem mapping
    for table in EXEC_ORDER:
        if table not in mappings:
            log(f"  [NO_MAP]  {table} esta no EXEC_ORDER mas nao no mapping", "ERROR")
            ok = False

    if ok:
        log("  Validacao OK -- nenhum problema encontrado.")
    else:
        log("  Validacao FALHOU -- corrija os erros acima.", "ERROR")

    return ok


# ============================================================================
# ETL PRINCIPAL
# ============================================================================
def load_mappings() -> dict:
    mapping_path = os.path.join(os.path.dirname(__file__), "column_mapping.json")
    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f)


def pg_upsert(
    conn, table: str, batch: list, conflict_col: str = "id"
) -> Tuple[int, int]:
    """Batch upsert via psycopg2 execute_values -- ~100x faster than REST API."""
    from psycopg2.extras import execute_values, Json

    if not batch:
        return 0, 0

    columns = list(batch[0].keys())
    conflict_cols = [c.strip() for c in conflict_col.split(",")]
    update_cols = [c for c in columns if c not in conflict_cols]

    cols_quoted = ", ".join(f'"{c}"' for c in columns)
    conflict_quoted = ", ".join(f'"{c}"' for c in conflict_cols)

    if update_cols:
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        sql = (
            f'INSERT INTO public."{table}" ({cols_quoted}) VALUES %s '
            f'ON CONFLICT ({conflict_quoted}) DO UPDATE SET {update_set}'
        )
    else:
        sql = (
            f'INSERT INTO public."{table}" ({cols_quoted}) VALUES %s '
            f'ON CONFLICT ({conflict_quoted}) DO NOTHING'
        )

    # Detect JSONB columns (ending in _json but NOT plain "json" columns which are TEXT)
    jsonb_cols = {c for c in columns if c.endswith("_json") and c != "json"}

    # Build values list with proper type adaptation
    values = []
    for row in batch:
        row_vals = []
        for c in columns:
            v = row[c]
            if isinstance(v, (dict, list)):
                row_vals.append(Json(v))
            elif c in jsonb_cols and isinstance(v, str):
                try:
                    row_vals.append(Json(json.loads(v)))
                except (json.JSONDecodeError, TypeError):
                    row_vals.append(v)
            else:
                row_vals.append(v)
        values.append(tuple(row_vals))

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=len(values))
        conn.commit()
        return len(batch), 0
    except Exception:
        conn.rollback()
        # Fallback: individual row inserts
        ok, err = 0, 0
        for row_vals in values:
            try:
                with conn.cursor() as cur:
                    execute_values(cur, sql, [row_vals], page_size=1)
                conn.commit()
                ok += 1
            except Exception as row_e:
                conn.rollback()
                if err < 5:
                    log(f"    Row error: {row_e}", "WARN")
                err += 1
        return ok, err


def pg_flush(
    conn, table: str, batch: list, conflict_col: str, ok: int, err: int
) -> Tuple[list, int, int]:
    """Flush a batch via pg_upsert and return empty batch + accumulated totals."""
    if batch:
        ins, e = pg_upsert(conn, table, batch, conflict_col)
        ok += ins
        err += e
    return [], ok, err


def run_etl():
    log("=" * 60)
    log("ETL v10 - MySQL -> Supabase (psycopg2 direto)")
    log("=" * 60)

    # Carregar mapeamentos
    MAPPINGS = load_mappings()
    log(f"Mapeamentos carregados: {len(MAPPINGS)} tabelas")

    # Caminho do schema de referencia (um nivel acima de etl/)
    schema_path = os.path.join(os.path.dirname(__file__), "..", "schema_ref.sql")
    schema_tables = parse_schema_tables(schema_path)
    if schema_tables:
        log(f"Schema carregado: {len(schema_tables)} tabelas")

    # Modo validacao: valida e sai sem escrever
    if VALIDATE_ONLY:
        log("Modo ETL_VALIDATE_ONLY=1 -- apenas validacao, sem escrita.")
        valid = validate_mapping(MAPPINGS, schema_tables)
        sys.exit(0 if valid else 1)

    # Conectar
    try:
        mysql = get_mysql()
        cursor = mysql.cursor(dictionary=True)
        pg = get_pg()
        log("Conexoes estabelecidas (MySQL + PostgreSQL direto)")
    except Exception as e:
        log(f"ERRO de conexao: {e}", "ERROR")
        sys.exit(1)

    # Desabilitar triggers de FK para evitar erros de ordem/orphans
    # Isso permite inserir dados sem respeitar FKs durante o ETL,
    # ja que a ordem topologica garante integridade no final
    with pg.cursor() as cur:
        cur.execute("SET session_replication_role = 'replica'")
    pg.commit()
    log("FK triggers desabilitados (session_replication_role=replica)")

    # Determinar tabelas a processar
    if ONLY_TABLES:
        tables_to_process = [t for t in EXEC_ORDER if t in ONLY_TABLES]
        log(f"ETL_ONLY_TABLES ativo: {tables_to_process}")
    else:
        tables_to_process = [t for t in EXEC_ORDER if t in MAPPINGS]

    log(f"Tabelas a processar: {len(tables_to_process)}")
    log(f"Batch size: {BATCH_SIZE}")

    stats: Dict[str, Dict[str, int]] = {}
    seen_emails: Set[str] = set()
    pf_list: List[dict] = []
    pj_list: List[dict] = []

    # Dados para 2-pass (self-references)
    self_ref_data: Dict[str, List[dict]] = {t: [] for t in SELF_REF_TABLES}

    # ---- Loop principal ----
    for table in tables_to_process:
        mapping = MAPPINGS.get(table)
        if not mapping:
            log(f"Sem mapping para {table} -- skip", "WARN")
            continue

        conflict_col = CONFLICT_COLS.get(table, "id")
        mysql_table = MYSQL_TABLE_NAME_MAP.get(table, table)
        self_ref_col = SELF_REF_TABLES.get(table)
        has_id = table not in TABLES_WITHOUT_ID

        log(f"\n>>> [{table}]" + (f" (MySQL: {mysql_table})" if mysql_table != table else ""))

        # Colunas a buscar no MySQL
        cols = list(set(mapping.values()))
        if "id" not in cols:
            cols.append("id")

        # Colunas extras para clientes (PF/PJ sao derivados)
        if table == "is_clientes":
            for c in ["nome", "sobrenome", "nascimento", "cpf", "sexo",
                      "razao_social", "fantasia", "ie", "cnpj"]:
                if c not in cols:
                    cols.append(c)

        try:
            query = f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `{mysql_table}`"
            cursor.execute(query)
        except Exception as e:
            log(f"    SKIP (nao existe no MySQL): {e}", "WARN")
            stats[table] = {"ok": 0, "err": 0}
            continue

        ok, err = 0, 0
        batch: List[dict] = []

        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                try:
                    if table == "is_clientes":
                        t_row = transform_cliente(row, mapping)
                        if t_row:
                            email = t_row.get("email_log")
                            if email in seen_emails:
                                continue
                            if email:
                                seen_emails.add(email)
                            pf = transform_cliente_pf(row)
                            if pf:
                                pf_list.append(pf)
                            pj = transform_cliente_pj(row)
                            if pj:
                                pj_list.append(pj)
                    else:
                        t_row = transform_row(row, table, mapping)

                    if t_row and (not has_id or t_row.get("id") is not None):
                        # Para 2-pass: salvar valor do self-ref e zera-lo no pass 1
                        if self_ref_col and t_row.get(self_ref_col):
                            self_ref_data[table].append(
                                {"id": t_row["id"], self_ref_col: t_row[self_ref_col]}
                            )
                            t_row[self_ref_col] = None  # Pass 1: sem self-ref

                        batch.append(t_row)

                except Exception as e:
                    log(f"    Erro em row {row.get('id')}: {e}", "WARN")
                    err += 1

            if len(batch) >= BATCH_SIZE:
                batch, ok, err = pg_flush(pg, table, batch, conflict_col, ok, err)

        batch, ok, err = pg_flush(pg, table, batch, conflict_col, ok, err)
        log(f"    OK={ok}, ERR={err}")
        stats[table] = {"ok": ok, "err": err}

    # ---- is_clientes_pf e is_clientes_pj ----
    for sub_table, sub_list, sub_conflict in [
        ("is_clientes_pf",  pf_list, "cliente_id"),
        ("is_clientes_pj",  pj_list, "cliente_id"),
    ]:
        if sub_list and (not ONLY_TABLES or sub_table in ONLY_TABLES):
            log(f"\n>>> [{sub_table}] ({len(sub_list)} registros)")
            valid = [p for p in sub_list if p.get("cliente_id")]
            sub_ok, sub_err = 0, 0
            for i in range(0, len(valid), BATCH_SIZE):
                chunk = valid[i:i + BATCH_SIZE]
                ins, e = pg_upsert(pg, sub_table, chunk, sub_conflict)
                sub_ok += ins
                sub_err += e
            log(f"    OK={sub_ok}, ERR={sub_err}")
            stats[sub_table] = {"ok": sub_ok, "err": sub_err}

    # ---- Pass 2: resolver self-references via psycopg2 ----
    from psycopg2.extras import execute_batch as pg_execute_batch

    for table, col in SELF_REF_TABLES.items():
        updates = self_ref_data.get(table, [])
        if not updates:
            continue
        if ONLY_TABLES and table not in ONLY_TABLES:
            continue
        log(f"\n>>> [2-pass self-ref {table}.{col}] ({len(updates)} registros)")
        update_sql = f'UPDATE public."{table}" SET "{col}" = %s WHERE "id" = %s'
        update_vals = [(upd[col], upd["id"]) for upd in updates]
        try:
            with pg.cursor() as cur:
                pg_execute_batch(cur, update_sql, update_vals, page_size=500)
            pg.commit()
            ok2 = len(updates)
            err2 = 0
        except Exception:
            pg.rollback()
            # Fallback: individual updates
            ok2, err2 = 0, 0
            for upd in updates:
                try:
                    with pg.cursor() as cur:
                        cur.execute(update_sql, (upd[col], upd["id"]))
                    pg.commit()
                    ok2 += 1
                except Exception as e:
                    pg.rollback()
                    if err2 < 5:
                        log(f"    Erro 2-pass {upd['id']}: {e}", "WARN")
                    err2 += 1
        log(f"    OK={ok2}, ERR={err2}")

    # Reabilitar triggers de FK
    with pg.cursor() as cur:
        cur.execute("SET session_replication_role = 'origin'")
    pg.commit()
    log("FK triggers reabilitados (session_replication_role=origin)")

    # ---- Resumo final ----
    log("\n" + "=" * 60)
    log("RESUMO FINAL")
    log("=" * 60)

    total_ok  = sum(s["ok"]  for s in stats.values())
    total_err = sum(s["err"] for s in stats.values())

    critical = ["is_usuarios", "is_clientes", "is_produtos", "is_pedidos", "is_pedidos_itens"]
    log("\nTabelas Criticas:")
    for t in critical:
        if t in stats:
            s = stats[t]
            status = "OK" if s["err"] == 0 else "!!"
            log(f"  {status} {t}: {s['ok']} OK, {s['err']} ERR")

    log(f"\nTOTAL: {total_ok:,} registros OK, {total_err:,} erros")

    cursor.close()
    mysql.close()
    pg.close()
    log("\nETL COMPLETO!")


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    run_etl()
