#!/usr/bin/env python3
"""
ETL v11 — MySQL legado → Supabase (psycopg2 direto)
====================================================

Reescrito com base na análise completa do dump MySQL (438 MB).
Corrige: PF/PJ, endereços, cupons, categorias, fretes, parcelas,
         mapeamento de colunas, tipos e constraints.

COMO USAR:
  python etl/run.py

Variáveis de controle:
  ETL_VALIDATE_ONLY=1   → valida mapping vs schema e sai sem escrever
  ETL_ONLY_TABLES=t1,t2 → roda apenas as tabelas listadas
  ETL_BATCH_SIZE=2000   → tamanho do batch de insert
"""

import os
import re
import uuid
import json
import sys
import html
import time
import threading
import datetime as dt
from pathlib import Path
from typing import Optional, Dict, Any, Set, List, Tuple, Callable
from dotenv import load_dotenv
from scripts.error_log_sink import (
    build_error_event,
    ensure_run_id,
    persist_error_event,
    persist_error_events#,
    #read_json_file,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
UUID_NS = uuid.UUID("2a6b2c31-0f2a-4dfd-8cde-7b4b9b3f1c5a")
BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "2000"))
WRITE_MODE = os.getenv("ETL_WRITE_MODE", "insert").strip().lower()  # insert | upsert
VALIDATE_ONLY = os.getenv("ETL_VALIDATE_ONLY", "0") == "1"
ONLY_TABLES_ENV = os.getenv("ETL_ONLY_TABLES", "").strip()
ONLY_TABLES: Set[str] = set(t.strip() for t in ONLY_TABLES_ENV.split(",") if t.strip())
MAPPING_PATH = Path(__file__).resolve().parent / "column_mapping.json"
SCHEMA_PATH_ENV = (os.getenv("ETL_SCHEMA_PATH", "") or "").strip()
ERROR_REPORT_PATH = Path(
    os.getenv("ETL_ERRORS_PATH", os.path.join(os.getenv("LOGS_DIR", "./logs"), "etl_errors.json"))
).resolve()
MAX_CAPTURED_ERRORS = int(os.getenv("ETL_MAX_ERROR_REPORT_ROWS", "10000"))
RUN_ID = ensure_run_id()

# MySQL → Supabase: nomes de tabela diferentes
MYSQL_TABLE_NAME_MAP = {
    "is_pedidos_fretes_entregas": "is_pedidos_fretes_envios",
}

# Tabelas derivadas: lidas da mesma fonte MySQL mas gravadas em tabelas separadas
DERIVED_TABLES = {"is_clientes_pf", "is_clientes_pj"}

# Tabelas sem coluna 'id' no Supabase (PK composta)
TABLES_WITHOUT_ID: Set[str] = {"is_mkt_cupons_produtos"}

TECHNICAL_TEXT_COLUMNS: Set[str] = {
    "email_log",
    "uid",
    "url",
    "link",
    "json",
    "hash",
    "senha_log",
    "sku",
    "gtin",
    "mpn",
    "ncm",
}

HUMAN_TEXT_COLUMNS: Set[str] = {
    "nome",
    "sobrenome",
    "razao_social",
    "fantasia",
    "titulo",
    "descricao",
    "obs",
    "obs_interna",
    "motivo",
    "regra",
    "cargo",
    "bairro",
    "cidade",
    "estado",
    "logradouro",
    "complemento",
    "metodo_titulo",
}

NAME_LIKE_COLUMNS: Set[str] = {"nome", "sobrenome", "razao_social", "fantasia", "titulo"}

ETL_ERRORS: List[dict] = []
_ETL_ERRORS_LOCK = threading.Lock()  # thread-safe para execução paralela futura

# ============================================================================
# LOGGING
# ============================================================================
_start_time = dt.datetime.now()


def log(msg: str, level: str = "INFO"):
    elapsed = dt.datetime.now() - _start_time
    mm, ss = divmod(int(elapsed.total_seconds()), 60)
    hh, mm = divmod(mm, 60)
    print(f"[{hh:02d}:{mm:02d}:{ss:02d}] [{level:5s}] {msg}", flush=True)


def _short_error_message(message: str, max_len: int = 320) -> str:
    text = (message or "").strip().replace("\n", " ")
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


def _probable_constraint(message: str) -> Optional[str]:
    patterns = [
        r'constraint "([^"]+)"',
        r"for key '([^']+)'",
        r"violates check constraint \"([^\"]+)\"",
        r"foreign key constraint \"([^\"]+)\"",
    ]
    for pattern in patterns:
        m = re.search(pattern, message, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def record_etl_error(
    table: str,
    legacy_id: Any,
    message: str,
    stage: str = "insert",
    probable: Optional[str] = None,
) -> None:
    with _ETL_ERRORS_LOCK:
        if len(ETL_ERRORS) >= MAX_CAPTURED_ERRORS:
            return
        ETL_ERRORS.append(
            {
                "table": table,
                "legacy_id": None if legacy_id in (None, "", "None") else str(legacy_id),
                "stage": stage,
                "probable_constraint": probable or _probable_constraint(message),
                "message": _short_error_message(message),
            }
        )


def write_etl_error_report(total_errors: int) -> dict:
    ERROR_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": RUN_ID,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "total_errors": int(total_errors),
        "captured_errors": len(ETL_ERRORS),
        "errors": ETL_ERRORS,
    }
    ERROR_REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    log(f"Error report saved at {ERROR_REPORT_PATH}", "WARN")
    return payload


def persist_etl_error_report(total_errors: int, stats: Dict[str, Dict[str, int]]) -> dict | None:
    if total_errors <= 0 and not ETL_ERRORS:
        return None

    payload = write_etl_error_report(total_errors)
    events = [
        build_error_event(
            run_id=RUN_ID,
            script_name="etl/run.py",
            step_name="run_etl",
            phase="etl",
            event_type="etl_error_summary",
            message=f"ETL finished with {total_errors} row-level error(s)",
            error_class="RuntimeError" if total_errors > 0 else None,
            details={
                "stats": stats,
                "error_report_path": str(ERROR_REPORT_PATH),
                "error_report": payload,
            },
        )
    ]
    for item in ETL_ERRORS:
        events.append(
            build_error_event(
                run_id=RUN_ID,
                script_name="etl/run.py",
                step_name=item.get("stage"),
                phase="etl",
                event_type="etl_row_error",
                message=item.get("message") or "ETL row error",
                table_name=item.get("table"),
                legacy_id=item.get("legacy_id"),
                probable_constraint=item.get("probable_constraint"),
                details={"row_error": item},
            )
        )
    persist_error_events(events)
    return payload


# ============================================================================
# CONVERSORES DE TIPO
# ============================================================================
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
    # Strip non-numeric suffix (e.g. "8%" → "8")
    s = re.sub(r"[^0-9.,\-]", "", s)
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def to_money(val: Any) -> float:
    """Converte para decimal não-negativo (constraints CHECK >= 0)."""
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


EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"(\+?\d[\d\s().\-]{7,}\d)")
CONTROL_CHARS_RE = re.compile(r"[\x00-\x1F\x7F]")
ASTERISK_NOISE_RE = re.compile(r"\*{3,}")
ESCAPED_QUOTE_RE = re.compile(r"\\+'")
MOJIBAKE_HINT_RE = re.compile(r"(Ã.|Â.|â.|�)")


def _fix_mojibake(text: str) -> str:
    if not MOJIBAKE_HINT_RE.search(text):
        return text
    try:
        candidate = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if candidate and candidate != text:
            return candidate
    except Exception:
        pass
    return text


def clean_human_text(value: Any, field_name: str = "") -> Optional[str]:
    s = to_str(value)
    if not s:
        return None
    s = _fix_mojibake(s)
    s = html.unescape(s)
    s = ESCAPED_QUOTE_RE.sub("'", s).replace("\\\"", "\"")
    s = CONTROL_CHARS_RE.sub(" ", s)
    s = ASTERISK_NOISE_RE.sub(" ", s)
    if field_name in NAME_LIKE_COLUMNS:
        s = EMAIL_RE.sub(" ", s)
        s = PHONE_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip(" -_/|,;.")
    return s or None


def safe_str(val: Any, field_name: str = "") -> Optional[str]:
    if field_name and field_name in HUMAN_TEXT_COLUMNS and field_name not in TECHNICAL_TEXT_COLUMNS:
        return clean_human_text(val, field_name=field_name)
    return to_str(val)


# ============================================================================
# CONEXÕES
# ============================================================================
def get_mysql():
    import mysql.connector
    config = {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", "3307")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "root"),
        "database": os.getenv("MYSQL_DATABASE", "nblgrafica_app"),
        "charset": "utf8mb4",
        "use_unicode": True,
        "use_pure": True,
    }
    try:
        return mysql.connector.connect(**config)
    except mysql.connector.Error as exc:
        if getattr(exc, "errno", None) == 1049 and config["database"] != "nblgrafica_app":
            fallback = {**config, "database": "nblgrafica_app"}
            log(
                f"MySQL database '{config['database']}' inexistente; fallback para '{fallback['database']}'",
                "WARN",
            )
            return mysql.connector.connect(**fallback)
        raise


def get_pg():
    import psycopg2
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL is required")
    return psycopg2.connect(db_url)


# ============================================================================
# FK MAP: pg_col → tabela referenciada para gerar UUID5
# ============================================================================
FK_MAP = {
    "cliente_id":        "is_clientes",
    "usuario_id":        "is_usuarios",
    "produto_id":        "is_produtos",
    "pedido_id":         "is_pedidos",
    "item_id":           "is_pedidos_itens",
    "comprovante_id":    "is_pedidos_pagamentos",
    "pagamento_id":      "is_pedidos_pagamentos",
    "original_id":       "is_pedidos_pagamentos",
    "frete_balcao_id":   "is_entregas_balcoes",
    "frete_endereco_id": "is_clientes_enderecos",
    "balcao_id":         "is_entregas_balcoes",
    "frete_id":          "is_entregas_fretes",
    "grupo_id":          "is_produtos_vars_nomes",
    "funcionario_id":    "is_financeiro_funcionarios",
    "vendedor_id":       "is_usuarios",
    "cupom_id":          "is_mkt_cupons",
    # Orphan FKs (tabelas que não existem no Supabase)
    "categoria_id":      "_orphan",
    "carteira_id":       "_orphan",
    "fornecedor_id":     "_orphan",
    "pdv_id":            "_orphan",
    "caixa_id":          "_orphan",
    "centro_custo_id":   "_orphan",
    "envio_id":          "_orphan",
    # Self-reference
    "parent_id":         None,
    "status_id":         "_int",  # references is_extras_status(id) which is INT
}

# Cache de IDs legados válidos por tabela referenciada.
# Preenchido no precheck / início do ETL para evitar violação de FK.
VALID_FK_IDS: Dict[str, Set[str]] = {}

# ============================================================================
# SCHEMA TYPE INFO POR TABELA
# Baseado na análise do schema_atualizado_grafica.sql (Supabase)
# ============================================================================

# Colunas BOOLEAN no Supabase
BOOL_COLS = {
    "visivel", "arquivado", "primeira_compra", "arte", "estoque_controlar",
    "vars_obrig", "devolucao_completa", "pago", "oculto", "sucesso",
}

# Colunas TIMESTAMP/DATE
TS_COLS = {
    "created_at", "data", "ultimo_acesso", "nascimento",
    "oferta_expira", "admissao", "demissao", "previsao_producao",
    "previsao_entrega", "arte_data", "data_modificado", "inicio", "fim",
    "data_pagto", "data_emissao",
}

# Colunas MONEY (CHECK >= 0)
MONEY_COLS = {
    "total", "acrescimo", "desconto", "desconto_uso", "sinal",
    "frete_valor", "taxa", "custo", "salario", "vale",
    "valor_arte", "min_compra",
    "arte_valor",  # is_pedidos_itens.arte_valor NOT NULL DEFAULT 0
}

# Colunas INT genéricas
INT_COLS = {
    "acesso", "origem", "repetir", "agrupar", "neutro",
    "arte_status", "visto", "categoria", "revendedor", "revenda_tipo",
    "vars_select", "vars_agrupadas", "vars_combinacao", "parcelas_qtd",
    "conciliacao_movimentacao", "conciliacao_pagto", "num",
    "vendidos", "estoque_qtde", "uso", "limite", "estoque", "cobranca",
    "cobranca_val", "minimo_c", "limite_c", "prazo_dias",
    "salario_vencimento", "vale_vencimento", "comissao_tipo", "comissao_valor",
    "categoria_relatorio",
}

# Overrides por tabela (quando o tipo global está errado para uma tabela específica)
TABLE_TYPE_OVERRIDES: Dict[str, Dict[str, str]] = {
    "is_extras_status":            {"visivel": "int", "id": "int_pk"},
    "is_mkt_cupons":               {"tipo": "cupom_tipo", "valor": "money"},  # valor NOT NULL CHECK >= 0
    "is_producao_setores":         {"status": "str"},
    "is_pedidos_itens":            {"erp_id": "int", "status": "str", "visto": "int"},
    "is_pedidos_pagamentos":       {"erp_id": "int", "visto": "bool"},
    "is_produtos":                 {"erp_id": "int", "valor": "str", "visivel": "bool", "prazo": "str"},
    "is_financeiro_funcionarios":  {"erp_id": "int"},
    "is_financeiro_lancamentos":   {"erp_id": "int", "valor": "money", "tipo": "finance_tipo", "status": "int"},
    "is_entregas_fretes":          {"prazo": "int", "tipo": "int"},
    "is_entregas_balcoes":         {"prazo": "str"},
    "is_pedidos":                  {"erp_id": "int", "frete_tipo": "str", "origem": "int"},
    "is_pedidos_historico":        {"erp_id": "int"},  # status_id: FK_MAP "_int" trata 0→None corretamente
    "is_clientes":                 {"erp_id": "int", "status": "int", "retirada": "int", "revendedor": "int", "pdv": "int"},
    "is_usuarios":                 {"status": "int", "acesso": "int"},
    "is_mkt_regras":               {"tipo": "int"},
    "is_produtos_categorias":      {"status": "int"},
}


# ============================================================================
# MAPEAMENTO DE COLUNAS: Supabase_col → MySQL_col
# Cada tabela lista EXATAMENTE as colunas do Supabase e de qual coluna MySQL vêm.
# ============================================================================
COLUMN_MAPPING: Dict[str, Dict[str, str]] = {
    "is_extras_status": {
        "id": "id", "nome": "nome", "num": "num", "visivel": "visivel",
    },
    "is_entregas_balcoes": {
        "id": "id", "titulo": "titulo", "telefone": "telefone",
        "logradouro": "logradouro", "cep": "cep", "complemento": "complemento",
        "bairro": "bairro", "cidade": "cidade", "estado": "estado",
        "custo": "custo", "prazo": "prazo", "created_at": "data",
        "arquivado": "arquivado",
    },
    "is_entregas_fretes": {
        "id": "id", "titulo": "titulo", "descricao": "descricao",
        "prazo": "prazo", "min_km": "min_km", "max_km": "max_km",
        "taxa": "taxa", "min_compra": "min_compra", "tipo": "tipo",
        "minimo_peso": "minimo_peso", "limite_peso": "limite_peso",
        "minimo_c": "minimo_c", "limite_c": "limite_c",
    },
    "is_financeiro_funcionarios": {
        "erp_id": "id", "id": "id", "nome": "nome", "sobrenome": "sobrenome",
        "nascimento": "nascimento", "cpf": "cpf", "rg": "rg",
        "sexo": "sexo", "telefone": "telefone", "celular": "celular",
        "cep": "cep", "logradouro": "logradouro", "numero": "numero",
        "bairro": "bairro", "complemento": "complemento",
        "cidade": "cidade", "estado": "estado",
        "admissao": "admissao", "demissao": "demissao",
        "salario": "salario", "salario_vencimento": "salario_vencimento",
        "vale": "vale", "vale_vencimento": "vale_vencimento",
        "cargo": "cargo", "obs": "obs",
    },
    "is_mkt_regras": {
        "id": "id", "desconto": "desconto", "regra": "regra",
        "uso": "uso", "tipo": "tipo", "created_at": "data",
    },
    "is_producao_setores": {
        "id": "id", "nome": "nome", "status": "status",
    },
    "is_produtos": {
        "erp_id": "id", "id": "id", "url": "url", "titulo": "titulo",
        "sku": "sku", "gtin": "gtin", "mpn": "mpn", "ncm": "ncm",
        "descricao_curta": "descricao_curta", "descricao_html": "descricao_html",
        "meta_title": "meta_title", "meta_description": "meta_description",
        "valor_arte": "valor_arte", "visivel": "visivel", "arte": "arte",
        "vendidos": "vendidos", "estoque_controlar": "estoque_controlar",
        "estoque_qtde": "estoque_qtde", "estoque_condicao": "estoque_condicao",
        "oferta_expira": "oferta_expira", "oferta_condicao": "oferta_condicao",
        "mostrar": "mostrar", "entrega": "entrega", "arquivado": "arquivado",
        "video": "video", "categoria_relatorio": "categoria_relatorio",
        "created_at": "data", "gabarito": "gabarito", "material": "material",
        "revestimento": "revestimento", "acabamento": "acabamento",
        "extras": "extras", "formato": "formato", "prazo": "prazo",
        "cores": "cores", "selo": "selo", "valor": "valor",
        "redirect_301": "redirect_301", "brdraw": "brdraw",
        "revenda_tipo": "revenda_tipo", "revenda_desconto": "revenda_desconto",
        "vars_select": "vars_select", "vars_obrig": "vars_obrig",
        "vars_agrupadas": "vars_agrupadas", "vars_combinacao": "vars_combinacao",
    },
    "is_produtos_vars_nomes": {
        "id": "id", "nome": "nome", "texto_exibicao": "texto_exibicao",
    },
    "is_clientes": {
        "erp_id": "id", "id": "id", "saldo": "saldo", "tipo": "tipo",
        "telefone": "telefone", "celular": "celular",
        "email_log": "email_log", "senha_log": "senha_log",
        "ultimo_acesso": "ultimo_acesso", "ip": "ip", "status": "status",
        "retirada": "retirada", "retirada_limite": "retirada_limite",
        "revendedor": "revendedor", "pdv": "pdv",
        "wpp_verificado": "wpp_verificado", "logotipo": "logotipo",
        "pagarme_id": "pagarme_id", "created_at": "data",
    },
    "is_usuarios": {
        "id": "id", "foto": "foto", "nome": "nome", "sobrenome": "sobrenome",
        "email_log": "email_log", "senha_log": "senha_log",
        "acesso": "acesso", "hora_de": "hora_de", "hora_ate": "hora_ate",
        "status": "status", "ultimo_acesso": "ultimo_acesso",
        "created_at": "data", "balcao_id": "balcao",
        "pdv_id": "pdv", "comissao_tipo": "comissao_tipo",
        "comissao_valor": "comissao_valor",
    },
    "is_clientes_pf": {
        "cliente_id": "id", "nome": "nome", "sobrenome": "sobrenome",
        "nascimento": "nascimento", "cpf": "cpf", "sexo": "sexo",
    },
    "is_clientes_pj": {
        "cliente_id": "id", "razao_social": "razao_social",
        "fantasia": "fantasia", "ie": "ie", "cnpj": "cnpj",
    },
    "is_clientes_enderecos": {
        "id": "id", "cliente_id": "cliente", "titulo": "titulo",
        "cep": "cep", "logradouro": "logradouro", "numero": "numero",
        "bairro": "bairro", "complemento": "complemento",
        "cidade": "cidade", "estado": "estado",
    },
    "is_entregas_fretes_locais": {
        "id": "id", "frete_id": "frete", "estado": "estado",
        "cidade": "cidade", "bairro": "bairro",
        "cep_inicio": "cep_inicio", "cep_fim": "cep_fim",
    },
    "is_produtos_categorias": {
        "id": "id", "parent_id": "pai", "slug": "slug", "chave": "chave",
        "titulo": "titulo", "title": "title", "description": "description",
        "descricao": "descricao", "status": "status",
    },
    "is_produtos_vars": {
        "id": "id", "produto_id": "produto", "grupo_id": "grupo",
        "opcao": "opcao", "nome": "nome", "valor": "valor",
        "estoque": "estoque", "cobranca": "cobranca",
        "foto": "foto", "cobranca_val": "cobranca_val",
    },
    "is_produtos_categorias_extras": {
        "id": "id", "produto_id": "produto", "categoria": "categoria",
        "subcategoria": "subcategoria", "secao": "secao",
        "subsecao": "subsecao", "subsubsecao": "subsubsecao",
    },
    "is_mkt_cupons": {
        "id": "id", "cliente_id": "cliente", "codigo": "codigo",
        "tipo": "tipo", "valor": "valor", "uso": "uso", "limite": "limite",
        "inicio": "inicio", "fim": "fim", "pedido_min": "pedido_min",
        "primeira_compra": "primeira_compra", "arquivado": "arquivado",
    },
    "is_mkt_cupons_produtos": {
        "cupom_id": "cupom", "produto_id": "produto",
    },
    "is_financeiro_lancamentos": {
        "erp_id": "id", "id": "id", "descricao": "descricao", "valor": "valor",
        "data": "data", "data_pagto": "data_pagto", "data_emissao": "data_emissao",
        "categoria_id": "categoria", "obs": "obs", "anexo": "anexo",
        "carteira_id": "carteira", "tipo": "tipo", "status": "status",
        "fornecedor_id": "fornecedor", "pdv_id": "pdv",
        "funcionario_id": "funcionario", "vendedor_id": "vendedor",
        "caixa_id": "caixa", "centro_custo_id": "centro_custo",
        "origem": "origem", "uid": "uid", "agrupar": "agrupar",
        "conciliacao": "conciliacao",
        "conciliacao_movimentacao": "conciliacao_movimentacao",
        "conciliacao_pagto": "conciliacao_pagto",
        "neutro": "neutro", "repetir": "repetir",
    },
    "is_pedidos": {
        "erp_id": "id", "id": "id", "cliente_id": "cliente", "usuario_id": "usuario",
        "total": "total", "acrescimo": "acrescimo", "desconto": "desconto",
        "desconto_uso": "desconto_uso", "sinal": "sinal",
        "frete_valor": "frete_valor", "frete_tipo": "frete_tipo",
        "frete_rastreio": "frete_rastreio",
        "frete_balcao_id": "frete_balcao", "frete_endereco_id": "frete_endereco",
        "origem": "origem", "obs": "obs", "obs_interna": "obs_interna",
        "nf": "nf", "cupom": "cupom", "json": "json",
        "pdv_id": "pdv", "caixa_id": "caixa",
        "devolucao_completa": "devolucao_completa", "created_at": "data",
    },
    "is_pedidos_fretes_detalhes": {
        "id": "id", "pedido_id": "pedido",
        "endereco_json": "endereco", "conteudo_json": "conteudo",
    },
    # is_pedidos_fretes_entregas → MySQL is_pedidos_fretes_envios
    # MySQL: id, pedido, tipo, detalhes (JSON)
    # Supabase: id, pedido_id, envio_id, metodo_titulo, modulo, prazo_dias, valor, sucesso, hash, descricao, created_at
    "is_pedidos_fretes_entregas": {
        "id": "id", "pedido_id": "pedido",
    },
    "is_pedidos_itens": {
        "erp_id": "id", "id": "id", "pedido_id": "pedido", "produto_id": "produto",
        "descricao": "descricao", "status": "status", "qtde": "qtde",
        "valor": "valor", "arte_valor": "arte_valor", "arte_tipo": "arte_tipo",
        "arte_status": "arte_status", "arte_arquivo": "arte_arquivo",
        "arte_data": "arte_data", "arte_nome": "arte_nome",
        "pago": "pago", "rastreio": "rastreio",
        "previsao_producao": "previsao_producao",
        "previsao_entrega": "previsao_entrega",
        "previa": "previa", "origem": "origem", "arquivado": "arquivado",
        "data_modificado": "data_modificado", "created_at": "data",
        "ftp": "ftp", "produto_detalhes": "produto_detalhes",
        "formato": "formato", "formato_detalhes": "formato_detalhes",
        "visto": "visto", "vars_raw": "vars", "vars_detalhes": "vars_detalhes",
        "json": "json", "categoria": "categoria", "revendedor": "revendedor",
    },
    "is_pedidos_pagamentos": {
        "erp_id": "id", "id": "id", "cliente_id": "cliente", "pedido_id": "pedido",
        "forma": "forma", "condicao": "condicao", "valor": "valor",
        "status": "status", "link": "link", "visto": "visto",
        "saldo_anterior": "saldo_anterior", "saldo_atual": "saldo_atual",
        "usuario_id": "usuario", "obs": "obs", "uid": "uid",
        "oculto": "oculto", "pdv_id": "pdv", "caixa_id": "caixa",
        "original_id": "original", "bandeira": "bandeira",
        "parcelas_raw": "parcelas", "parcelas_qtd": "parcelas",
        "created_at": "data",
    },
    "is_usuarios_historico": {
        "id": "id", "usuario_id": "usuario", "cliente_id": "cliente",
        "acao": "acao", "created_at": "data",
    },
    "is_clientes_extratos": {
        "id": "id", "cliente_id": "cliente", "pedido_id": "pedido",
        "pagamento_id": "pagamento", "saldo_antes": "saldo_antes",
        "saldo_depois": "saldo_depois", "descricao": "descricao",
        "obs": "obs", "valor": "valor", "created_at": "data",
    },
    "is_pedidos_historico": {
        "erp_id": "id", "id": "id", "pedido_id": "pedido", "item_id": "item",
        "status_id": "status", "usuario_id": "usuario",
        "obs": "obs", "created_at": "data",
    },
    "is_pedidos_itens_reprovados": {
        "id": "id", "item_id": "item", "motivo": "motivo",
        "usuario_id": "usuario", "created_at": "data",
    },
    "is_pedidos_pag_reprovados": {
        "id": "id", "comprovante_id": "comprovante", "motivo": "motivo",
        "usuario_id": "usuario", "created_at": "data",
    },
}


# ============================================================================
# ORDEM TOPOLÓGICA — respeita FKs do Supabase
# ============================================================================
SOURCE_TABLE_OVERRIDES: Dict[str, str] = {
    "is_clientes_pf": "is_clientes",
    "is_clientes_pj": "is_clientes",
    "is_mkt_cupons_produtos": "is_mkt_cupons",
}

# Tabelas cuja transformação não usa leitura 1:1 de colunas de origem do mapping.
SOURCE_COLUMN_VALIDATION_SKIP: Set[str] = {
    "is_mkt_cupons_produtos",
}


def load_column_mapping(path: Path = MAPPING_PATH) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"column_mapping.json not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("column_mapping.json must be an object")
    return payload


# Fonte canônica: etl/column_mapping.json
COLUMN_MAPPING = load_column_mapping()


# ============================================================================
# ORDEM TOPOLÓGICA — blocos de execução com integridade referencial estrita
#
# Regra: um bloco inteiro DEVE ser concluído antes do próximo iniciar.
# Tabelas dentro do mesmo bloco não têm dependências entre si → podem
# rodar em paralelo (ThreadPoolExecutor, habilitado por ETL_PARALLEL_BLOCKS=1).
#
# Auto-relacionamentos (parent_id / original_id) são tratados via
# SET session_replication_role = 'replica' nos processadores dedicados,
# eliminando a necessidade de 2-pass UPDATE.
# ============================================================================
EXEC_BLOCKS: List[List[str]] = [
    # ── Bloco 0 — sem dependências externas ──────────────────────────────
    [
        "is_clientes",
        "is_entregas_balcoes",
        "is_entregas_fretes",
        "is_extras_status",
        "is_financeiro_funcionarios",
        "is_mkt_regras",
        "is_producao_setores",
        "is_produtos",
        "is_produtos_vars_nomes",
        "is_produtos_categorias",      # self-ref parent_id → via replication_role
    ],
    # ── Bloco 1 — dependem do Bloco 0 ───────────────────────────────────
    [
        "is_clientes_enderecos",       # FK → is_clientes
        "is_clientes_pf",              # derivado de is_clientes
        "is_clientes_pj",              # derivado de is_clientes
        "is_mkt_cupons",               # FK → is_clientes
        "is_entregas_fretes_locais",   # FK → is_entregas_fretes
        "is_produtos_categorias_extras",  # FK → is_produtos
        "is_produtos_vars",            # FK → is_produtos, is_produtos_vars_nomes
        "is_usuarios",                 # FK → is_entregas_balcoes
    ],
    # ── Bloco 2 — dependem do Bloco 1 ───────────────────────────────────
    [
        "is_mkt_cupons_produtos",      # FK → is_mkt_cupons, is_produtos
        "is_usuarios_historico",       # FK → is_usuarios, is_clientes
        "is_financeiro_lancamentos",   # FK → is_financeiro_funcionarios, is_usuarios
        "is_pedidos",                  # FK → is_clientes, is_usuarios, is_mkt_cupons
    ],
    # ── Bloco 3 — dependem do Bloco 2 ───────────────────────────────────
    [
        "is_pedidos_fretes_detalhes",  # FK → is_pedidos
        "is_pedidos_fretes_entregas",  # FK → is_pedidos
        "is_pedidos_itens",            # FK → is_pedidos, is_produtos
        "is_pedidos_pagamentos",       # FK → is_pedidos, is_clientes; self-ref original_id
    ],
    # ── Bloco 4 — dependem do Bloco 3 ───────────────────────────────────
    [
        "is_clientes_extratos",        # FK → is_clientes, is_pedidos, is_pedidos_pagamentos
        "is_pedidos_historico",        # FK → is_pedidos, is_pedidos_itens, is_extras_status
        "is_pedidos_itens_reprovados", # FK → is_pedidos_itens
        "is_pedidos_pag_reprovados",   # FK → is_pedidos_pagamentos
    ],
]

# EXEC_ORDER linear derivado dos blocos — mantido para compatibilidade retroativa
# (validate_mapping_contract, testes, RESUMO FINAL)
EXEC_ORDER: List[str] = [t for block in EXEC_BLOCKS for t in block]

CONFLICT_COLS: Dict[str, str] = {
    "is_clientes_pf": "cliente_id",
    "is_clientes_pj": "cliente_id",
    "is_mkt_cupons_produtos": "cupom_id,produto_id",
}

# Documentação: tabelas com auto-referência FK.
# Tratadas via SET session_replication_role = 'replica' nos processadores
# _process_categorias e _process_pagamentos — NÃO usa 2-pass UPDATE.
SELF_REF_TABLES: Dict[str, str] = {
    "is_produtos_categorias": "parent_id",   # resolvido via session_replication_role
    "is_pedidos_pagamentos":  "original_id", # resolvido via session_replication_role
}

# Colunas NOT NULL que DEVEM ser não-nulas após transform.
# Linhas onde qualquer dessas colunas resolve para None são DESCARTADAS.
# Basado em constraints NOT NULL sem DEFAULT nas tabelas Supabase.
REQUIRED_NONNULL_COLS: Dict[str, Set[str]] = {
    # frete_id uuid NOT NULL → is_entregas_fretes
    "is_entregas_fretes_locais":    {"frete_id"},
    # produto_id uuid NOT NULL → is_produtos
    "is_produtos_categorias_extras": {"produto_id"},
    # pedido_id uuid NOT NULL → is_pedidos
    "is_pedidos_fretes_detalhes":   {"pedido_id"},
    "is_pedidos_itens":             {"pedido_id"},
    # motivo text NOT NULL (sem default)
    "is_pedidos_itens_reprovados":  {"motivo"},
    "is_pedidos_pag_reprovados":    {"motivo"},
}

# Fallbacks para colunas NOT NULL (com DEFAULT no schema Supabase) que podem vir nulas
# do MySQL. Aplicados APÓS transform_row e ANTES do insert para evitar violações NOT NULL.
# Regra: usar o DEFAULT do schema Supabase como fallback, nunca inventar valores.
NONNULL_FALLBACKS: Dict[str, Dict[str, Any]] = {
    # senha_log NOT NULL, acesso NOT NULL
    "is_usuarios":              {"senha_log": "", "acesso": 0},
    # saldo NOT NULL DEFAULT 0
    "is_clientes":              {"saldo": 0.0},
    # status NOT NULL (override int, pode ser nulo no legado)
    "is_financeiro_lancamentos": {"status": 0},
    # saldo_antes/saldo_depois/valor NOT NULL
    "is_clientes_extratos":     {"saldo_antes": 0.0, "saldo_depois": 0.0, "valor": 0.0},
    # status NOT NULL DEFAULT 1
    "is_produtos_categorias":   {"status": 1},
    # visivel NOT NULL DEFAULT true; arte/arquivado/estoque_controlar NOT NULL DEFAULT false;
    # vendidos/estoque_qtde NOT NULL DEFAULT 0
    "is_produtos": {
        "visivel": True,
        "arte": False,
        "arquivado": False,
        "vendidos": 0,
        "estoque_controlar": False,
        "estoque_qtde": 0,
    },
    # uso NOT NULL DEFAULT 0; pedido_min NOT NULL DEFAULT 0;
    # primeira_compra NOT NULL DEFAULT false; arquivado NOT NULL DEFAULT false
    "is_mkt_cupons": {
        "uso": 0,
        "pedido_min": 0.0,
        "primeira_compra": False,
        "arquivado": False,
    },
    # devolucao_completa NOT NULL DEFAULT false
    "is_pedidos":               {"devolucao_completa": False},
    # pago NOT NULL DEFAULT false; arquivado NOT NULL DEFAULT false
    "is_pedidos_itens":         {"pago": False, "arquivado": False},
    # visto NOT NULL DEFAULT false; oculto NOT NULL DEFAULT false
    "is_pedidos_pagamentos":    {"visto": False, "oculto": False},
}

# IDs válidos de is_extras_status (1-35 apenas; 0, 9999, 41, 46 são valores legados inválidos)
VALID_EXTRAS_STATUS_IDS: frozenset = frozenset(range(1, 36))

# Relações opcionais em is_financeiro_lancamentos. O legado pode preencher várias
# simultaneamente; o schema atual aceita essa combinação e o ETL deve preservá-la.
_FINANCE_RELATION_COLS = [
    "funcionario_id", "vendedor_id", "categoria_id",
    "carteira_id", "fornecedor_id", "pdv_id", "caixa_id", "centro_custo_id",
]

# Produção impõe chk_is_financeiro_lancamentos_one_counterparty, mas o conjunto
# exato de colunas coberto pela constraint não está versionado no repositório.
# Para evitar apagar relações em massa, aplicamos reparos progressivos apenas nas
# rows que o banco realmente rejeita.
_FINANCE_CONSTRAINT_REPAIR_GROUPS = [
    ["funcionario_id", "vendedor_id", "fornecedor_id", "pdv_id"],
    ["funcionario_id", "vendedor_id", "fornecedor_id", "pdv_id", "carteira_id"],
    ["funcionario_id", "vendedor_id", "fornecedor_id", "pdv_id", "carteira_id", "caixa_id"],
    [
        "funcionario_id",
        "vendedor_id",
        "fornecedor_id",
        "pdv_id",
        "carteira_id",
        "caixa_id",
        "centro_custo_id",
    ],
    _FINANCE_RELATION_COLS,
]


def _mutate_pedidos_historico(row: dict) -> dict:
    """Coerce status_id inválido (fora de 1-35) para None antes do insert."""
    sid = row.get("status_id")
    if sid is not None and sid not in VALID_EXTRAS_STATUS_IDS:
        row["status_id"] = None
    return row


def _repair_finance_constraint_row(row: dict) -> List[dict]:
    """Gera variantes progressivas para rows rejeitadas pela constraint financeira."""
    repaired_rows: List[dict] = []
    seen: set[Tuple[Any, ...]] = set()
    for group in _FINANCE_CONSTRAINT_REPAIR_GROUPS:
        nonnull = [col for col in group if row.get(col) is not None]
        if len(nonnull) <= 1:
            continue
        repaired = row.copy()
        for col in nonnull[1:]:
            repaired[col] = None
        signature = tuple(repaired.get(col) for col in _FINANCE_RELATION_COLS)
        if signature not in seen:
            repaired_rows.append(repaired)
            seen.add(signature)
    return repaired_rows


def _repair_row_for_insert(table: str, row: dict, message: str) -> List[dict]:
    if table != "is_financeiro_lancamentos":
        return []
    if "chk_is_financeiro_lancamentos_one_counterparty" not in message:
        return []
    return _repair_finance_constraint_row(row)


# Mutadores pós-transform: corrigem valores antes do validator e do insert.
POST_TRANSFORM_MUTATORS: Dict[str, Callable[[dict], dict]] = {
    "is_pedidos_historico":       _mutate_pedidos_historico,
}

# Validadores pós-transform: filtram rows que violam constraints do Supabase
# antes do insert (evita FK/CHECK violations → batch retry cascade).
POST_TRANSFORM_VALIDATORS: Dict[str, Callable[[dict], bool]] = {}


# ============================================================================
# TRANSFORMAÇÕES
# ============================================================================

def resolve_schema_path() -> Path:
    if SCHEMA_PATH_ENV:
        return Path(SCHEMA_PATH_ENV).resolve()
    candidates = [
        Path(__file__).resolve().parent.parent / "schema_ref.sql",
        Path(__file__).resolve().parents[2] / ".vscode" / "schema_atualizado_grafica.sql",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("schema file not found (set ETL_SCHEMA_PATH)")


def load_target_schema_columns(schema_path: Optional[Path] = None) -> Dict[str, Set[str]]:
    path = schema_path or resolve_schema_path()
    sql = path.read_text(encoding="utf-8", errors="replace")
    tables: Dict[str, Set[str]] = {}
    pattern = re.compile(r"CREATE TABLE public\.(\w+) \((.*?)\);", re.IGNORECASE | re.DOTALL)
    for table_name, body in pattern.findall(sql):
        cols: Set[str] = set()
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("CONSTRAINT"):
                continue
            match = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)\s", line)
            if match:
                cols.add(match.group(1))
        tables[table_name] = cols
    return tables


def load_source_table_columns(mysql_cursor, table_name: str) -> Set[str]:
    mysql_cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
        """,
        (table_name,),
    )
    return {str(row["column_name"]) for row in mysql_cursor.fetchall()}


def resolve_source_table(table: str) -> str:
    if table in SOURCE_TABLE_OVERRIDES:
        return SOURCE_TABLE_OVERRIDES[table]
    return MYSQL_TABLE_NAME_MAP.get(table, table)


def validate_mapping_contract(mysql_cursor) -> List[str]:
    errors: List[str] = []
    schema_tables = load_target_schema_columns()
    source_columns_cache: Dict[str, Set[str]] = {}

    for table in EXEC_ORDER:
        mapping = COLUMN_MAPPING.get(table)
        if not mapping:
            errors.append(f"{table}: missing mapping entry")
            continue

        schema_cols = schema_tables.get(table)
        if not schema_cols:
            errors.append(f"{table}: missing in target schema")
        else:
            missing_target_cols = sorted(set(mapping.keys()) - schema_cols)
            if missing_target_cols:
                errors.append(
                    f"{table}: mapped target cols missing in schema -> {', '.join(missing_target_cols)}"
                )

        source_table = resolve_source_table(table)
        if source_table not in source_columns_cache:
            try:
                source_columns_cache[source_table] = load_source_table_columns(mysql_cursor, source_table)
            except Exception as exc:
                errors.append(f"{table}: source table '{source_table}' unavailable ({exc})")
                source_columns_cache[source_table] = set()
        source_cols = source_columns_cache[source_table]
        if table not in SOURCE_COLUMN_VALIDATION_SKIP:
            missing_source_cols = sorted(set(mapping.values()) - source_cols)
            if missing_source_cols:
                errors.append(
                    f"{table}: mapped source cols missing in '{source_table}' -> {', '.join(missing_source_cols)}"
                )

    return errors


def _legacy_fk_exists(ref_table: str, legacy_id: Any) -> bool:
    """Retorna True se o legacy_id existe na tabela de referência no MySQL."""
    if legacy_id is None:
        return False
    s = str(legacy_id).strip()
    if s in ("", "0", "None", "null"):
        return False
    valid = VALID_FK_IDS.get(ref_table)
    if valid is None:
        # Sem cache para essa tabela: fallback permissivo.
        return True
    return s in valid


def build_valid_fk_ids(cursor) -> Dict[str, Set[str]]:
    """
    Carrega IDs legados válidos de todas as tabelas referenciadas por FK_MAP.
    Usado para anular FKs inválidas antes do insert no Supabase.
    """
    tables = sorted({t for t in FK_MAP.values() if t and not str(t).startswith("_")})
    result: Dict[str, Set[str]] = {}

    for table in tables:
        mysql_table = MYSQL_TABLE_NAME_MAP.get(table, table)
        try:
            cursor.execute(f"SELECT `id` FROM `{mysql_table}`")
            ids = {str(r.get('id')).strip() for r in cursor.fetchall() if r.get('id') is not None}
            result[table] = ids
            log(f"[FK-CACHE] {table}: {len(ids):,} ids")
        except Exception as exc:
            log(f"[FK-CACHE] {table}: falhou ({exc})", "WARN")
            result[table] = set()

    return result


def transform_column(
    pg_col: str, mysql_col: str, val: Any, table: str,
    overrides: Dict[str, str],
) -> Any:
    """Transforma um valor de coluna MySQL para o formato Supabase."""

    # --- Override por tabela ---
    ov = overrides.get(pg_col)
    if ov:
        if ov == "int" or ov == "int_pk":
            return to_int(val)
        if ov == "bool":
            return to_bool(val)
        if ov == "str":
            if pg_col in HUMAN_TEXT_COLUMNS and pg_col not in TECHNICAL_TEXT_COLUMNS:
                return clean_human_text(val, field_name=pg_col)
            return to_str(val)
        if ov == "money":
            return to_money(val)
        if ov == "decimal":
            return to_decimal(val)
        if ov == "ts":
            return to_ts(val)
        if ov == "finance_tipo":
            # MySQL: 1=receita→Supabase 1, 0=despesa→Supabase 2.
            # Supabase CHECK: tipo = ANY (ARRAY[1, 2]).
            normalized = to_int(val)
            if normalized == 1:
                return 1
            if normalized in (0, 2):  # 0=despesa legado→2; 2 já é válido
                return 2
            raise ValueError(f"is_financeiro_lancamentos.tipo inválido no legado: {val!r}")
        if ov == "cupom_tipo":
            # MySQL: '$' → 'amount', '%' → 'percent', 'f' → 'amount' (frete grátis)
            s = to_str(val)
            if s in ("$", "f"):
                return "amount"
            if s == "%":
                return "percent"
            if s and s.lower() in ("percent", "amount"):
                return s.lower()
            return "amount"  # default
        if pg_col in HUMAN_TEXT_COLUMNS and pg_col not in TECHNICAL_TEXT_COLUMNS:
            return clean_human_text(val, field_name=pg_col)
        return to_str(val)

    # --- FK columns → UUID5 ---
    if pg_col.endswith("_id") and pg_col != "id":
        ref = FK_MAP.get(pg_col)
        if ref == "_int":
            v = to_int(val)
            return v if v else None  # 0 = sentinel "no status" → NULL
        if ref == "_orphan":
            orphan_table = pg_col.replace("_id", "s")
            return uuid5_for(orphan_table, val) if val else None
        if ref is None:
            # Self-reference
            return uuid5_for(table, val) if val else None
        if not _legacy_fk_exists(ref, val):
            return None
        return uuid5_for(ref, val) if val else None

    # --- Boolean ---
    if pg_col in BOOL_COLS:
        return to_bool(val)

    # --- Timestamp ---
    if pg_col in TS_COLS:
        return to_ts(val)

    # --- Money ---
    if pg_col in MONEY_COLS:
        return to_money(val)

    # --- Integer ---
    if pg_col in INT_COLS:
        return to_int(val)

    # --- Decimal pattern ---
    if any(x in pg_col for x in ("valor", "saldo", "custo", "qtde", "taxa", "desconto")):
        return to_decimal(val)

    # --- JSONB columns ---
    if pg_col.endswith("_json"):
        return to_str(val)  # Will be handled as Json in upsert

    # --- Default: string ---
    if isinstance(val, str):
        if pg_col in HUMAN_TEXT_COLUMNS and pg_col not in TECHNICAL_TEXT_COLUMNS:
            return clean_human_text(val, field_name=pg_col)
        return to_str(val)
    return val


def transform_row(
    row: dict, table: str, mapping: Dict[str, str],
) -> Optional[dict]:
    """Transforma uma row MySQL para formato Supabase."""
    new_row: dict = {}
    legacy_id = row.get("id")
    new_row["__legacy_id"] = legacy_id
    overrides = TABLE_TYPE_OVERRIDES.get(table, {})

    # PK
    if table in TABLES_WITHOUT_ID:
        pass  # Sem coluna id
    elif overrides.get("id") == "int_pk":
        new_row["id"] = to_int(legacy_id)
        if new_row["id"] is None:
            return None
    elif legacy_id:
        new_row["id"] = uuid5_for(table, legacy_id)
    else:
        return None

    for pg_col, mysql_col in mapping.items():
        if pg_col == "id":
            continue
        val = row.get(mysql_col)
        new_row[pg_col] = transform_column(pg_col, mysql_col, val, table, overrides)

    return new_row


# ============================================================================
# TRANSFORMAÇÕES ESPECIAIS
# ============================================================================

def transform_cliente(row: dict) -> Optional[dict]:
    """Transforma is_clientes: normaliza tipo e email."""
    mapping = COLUMN_MAPPING["is_clientes"]
    d = transform_row(row, "is_clientes", mapping)
    if not d:
        return None
    # Normalizar tipo: 'fisica' → 'PF', 'juridica' → 'PJ'
    t = str(row.get("tipo", "")).lower().strip()
    if t in ("juridica", "pj"):
        d["tipo"] = "PJ"
    elif t in ("fisica", "pf"):
        d["tipo"] = "PF"
    else:
        raise ValueError(f"is_clientes.tipo inválido no legado para id={row.get('id')}: {t!r}")
    # Normalizar email
    if d.get("email_log"):
        d["email_log"] = d["email_log"].lower().strip()
    return d


def transform_cliente_pf(row: dict) -> Optional[dict]:
    """Extrai dados PF de is_clientes."""
    t = str(row.get("tipo", "")).lower().strip()
    if t not in ("fisica", "pf"):
        return None
    cid = uuid5_for("is_clientes", row.get("id"))
    if not cid:
        return None
    nome = safe_str(row.get("nome"), "nome")
    if not nome:
        return None  # CHECK constraint: nome IS NOT NULL
    return {
        "cliente_id": cid,
        "nome": nome,
        "sobrenome": safe_str(row.get("sobrenome"), "sobrenome"),
        "nascimento": to_ts(row.get("nascimento")),
        "cpf": to_str(row.get("cpf")),
        "sexo": to_str(row.get("sexo")),
    }


def transform_cliente_pj(row: dict) -> Optional[dict]:
    """Extrai dados PJ de is_clientes."""
    t = str(row.get("tipo", "")).lower().strip()
    if t not in ("juridica", "pj"):
        return None
    cid = uuid5_for("is_clientes", row.get("id"))
    if not cid:
        return None
    razao = safe_str(row.get("razao_social"), "razao_social")
    if not razao:
        # Fallback para não perder PJ legada com razão social vazia.
        razao = (
            safe_str(row.get("nome"), "nome")
            or safe_str(row.get("fantasia"), "fantasia")
            or safe_str(row.get("cnpj"))
        )
    if not razao:
        return None  # CHECK constraint: razao_social IS NOT NULL
    return {
        "cliente_id": cid,
        "razao_social": razao,
        "fantasia": safe_str(row.get("fantasia"), "fantasia"),
        "ie": to_str(row.get("ie")),
        "cnpj": to_str(row.get("cnpj")),
    }


def transform_cliente_endereco_from_cliente(row: dict) -> Optional[dict]:
    """Cria endereço padrão a partir dos campos de endereço em is_clientes."""
    cid = uuid5_for("is_clientes", row.get("id"))
    if not cid:
        return None
    cep = to_str(row.get("cep"))
    logradouro = to_str(row.get("logradouro"))
    # Só cria se tiver algum dado de endereço
    if not cep and not logradouro:
        return None
    # UUID determinístico: "addr:<cliente_id>"
    addr_id = str(uuid.uuid5(UUID_NS, f"is_clientes_enderecos:addr_{row.get('id')}"))
    return {
        "id": addr_id,
        "cliente_id": cid,
        "titulo": "Principal",
        "cep": cep,
        "logradouro": to_str(row.get("logradouro")),
        "numero": to_str(row.get("numero")),
        "bairro": to_str(row.get("bairro")),
        "complemento": to_str(row.get("complemento")),
        "cidade": to_str(row.get("cidade")),
        "estado": to_str(row.get("estado")),
    }


def transform_cupom_produtos(row: dict) -> List[dict]:
    """Extrai is_mkt_cupons_produtos a partir do campo 'produtos' CSV em is_mkt_cupons."""
    produtos_str = to_str(row.get("produtos"))
    if not produtos_str:
        return []
    cupom_id = uuid5_for("is_mkt_cupons", row.get("id"))
    if not cupom_id:
        return []
    results = []
    for p in produtos_str.split(","):
        p = p.strip()
        if p:
            produto_id = uuid5_for("is_produtos", p)
            if produto_id:
                results.append({"cupom_id": cupom_id, "produto_id": produto_id})
    return results


def transform_pedidos_fretes_entregas(row: dict) -> Optional[dict]:
    """
    Transforma is_pedidos_fretes_envios (MySQL) → is_pedidos_fretes_entregas (Supabase).
    MySQL: id, pedido, tipo, detalhes (JSON string)
    Supabase: id, pedido_id, envio_id, metodo_titulo, modulo, prazo_dias, valor, sucesso, hash, descricao, created_at
    """
    legacy_id = row.get("id")
    if not legacy_id:
        return None
    new_id = uuid5_for("is_pedidos_fretes_entregas", legacy_id)
    pedido_id = uuid5_for("is_pedidos", row.get("pedido"))
    if not pedido_id or not _legacy_fk_exists("is_pedidos", row.get("pedido")):
        return None  # pedido órfão — skip silencioso

    tipo = to_str(row.get("tipo"))
    detalhes_raw = to_str(row.get("detalhes"))

    result = {
        "id": new_id,
        "pedido_id": pedido_id,
        "envio_id": None,
        "metodo_titulo": safe_str(tipo, "titulo"),
        "modulo": None,
        "prazo_dias": None,
        "valor": None,
        "sucesso": None,
        "hash": None,
        "descricao": detalhes_raw[:500] if detalhes_raw else None,
        "created_at": None,
    }

    # Tentar parsear JSON detalhes
    if detalhes_raw:
        try:
            d = json.loads(detalhes_raw)
            if isinstance(d, dict):
                result["metodo_titulo"] = safe_str(d.get("titulo") or tipo, "titulo")
                result["modulo"] = safe_str(d.get("modulo"))
                result["prazo_dias"] = to_int(d.get("prazo")) or to_int(d.get("prazo_dias"))
                result["valor"] = to_decimal(d.get("custo")) or to_decimal(d.get("valor"))
                result["sucesso"] = to_bool(d.get("sucesso") if "sucesso" in d else d.get("status"))
                result["hash"] = safe_str(d.get("hash"))
                result["created_at"] = to_ts(d.get("data") or d.get("created_at"))
        except (json.JSONDecodeError, TypeError):
            pass

    if result["prazo_dias"] is not None and result["prazo_dias"] < 0:
        result["prazo_dias"] = None
    if result["valor"] is not None and result["valor"] < 0:
        result["valor"] = None

    return result


def transform_pedido(row: dict, cupom_codigos: Set[str]) -> Optional[dict]:
    """
    Transforma is_pedidos com tratamento especial para cupom FK.
    is_pedidos.cupom referencia is_mkt_cupons(codigo) — precisa existir.
    """
    mapping = COLUMN_MAPPING["is_pedidos"]
    d = transform_row(row, "is_pedidos", mapping)
    if not d:
        return None
    # Validar FK cupom → is_mkt_cupons(codigo)
    cupom = d.get("cupom")
    if cupom and cupom not in cupom_codigos:
        d["cupom"] = None  # Evitar violação de FK
    elif not cupom:
        d["cupom"] = None
    return d


def transform_pagamento(row: dict) -> Optional[dict]:
    """
    Transforma is_pedidos_pagamentos.
    MySQL 'parcelas' (varchar) → Supabase 'parcelas_raw' + 'parcelas_qtd'.
    """
    mapping = COLUMN_MAPPING["is_pedidos_pagamentos"]
    overrides = TABLE_TYPE_OVERRIDES.get("is_pedidos_pagamentos", {})

    new_row: dict = {}
    legacy_id = row.get("id")
    if not legacy_id:
        return None
    new_row["id"] = uuid5_for("is_pedidos_pagamentos", legacy_id)

    for pg_col, mysql_col in mapping.items():
        if pg_col == "id":
            continue
        val = row.get(mysql_col)

        if pg_col == "parcelas_raw":
            new_row["parcelas_raw"] = to_str(val)
            continue
        if pg_col == "parcelas_qtd":
            # Extrair número de parcelas do campo 'parcelas'
            s = to_str(val)
            if s:
                m = re.search(r"(\d+)", s)
                parcelas_qtd = int(m.group(1)) if m else None
                new_row["parcelas_qtd"] = parcelas_qtd if parcelas_qtd and parcelas_qtd >= 1 else None
            else:
                new_row["parcelas_qtd"] = None
            continue
        if pg_col == "valor":
            new_row["valor"] = to_decimal(val) or 0
            continue
        if pg_col == "status":
            new_row["status"] = to_int(val) or 0
            continue

        new_row[pg_col] = transform_column(pg_col, mysql_col, val, "is_pedidos_pagamentos", overrides)

    return new_row


# ============================================================================
# CATEGORIAS: slug → ID lookup para parent_id
# ============================================================================

def build_categoria_slug_map(cursor) -> Dict[str, int]:
    """
    is_produtos_categorias.pai no MySQL referencia a coluna CHAVE da categoria pai
    (ex: 'promocional-2088'), não um ID inteiro.
    Precisamos de um mapa chave → id para resolver parent_id.
    """
    cursor.execute("SELECT `id`, `chave` FROM `is_produtos_categorias`")
    slug_map = {}
    for row in cursor.fetchall():
        chave = row.get("chave") or ""
        if chave:
            slug_map[chave] = row["id"]
    return slug_map


def transform_categoria(row: dict, slug_map: Dict[str, int]) -> Optional[dict]:
    """Transforma is_produtos_categorias resolvendo parent_id via chave."""
    legacy_id = row.get("id")
    if not legacy_id:
        return None

    new_row = {
        "id": uuid5_for("is_produtos_categorias", legacy_id),
        "slug": to_str(row.get("slug")),
        "chave": to_str(row.get("chave")),
        "titulo": safe_str(row.get("titulo")),
        "title": to_str(row.get("title")),
        "description": to_str(row.get("description")),
        "descricao": to_str(row.get("descricao")),
        "status": to_int(row.get("status")),
    }

    # Resolver parent_id: MySQL 'pai' referencia CHAVE da categoria pai.
    pai_slug = to_str(row.get("pai"))
    if pai_slug and pai_slug != "0":
        parent_legacy_id = slug_map.get(pai_slug)
        if parent_legacy_id:
            new_row["parent_id"] = uuid5_for("is_produtos_categorias", parent_legacy_id)
        else:
            new_row["parent_id"] = None
    else:
        new_row["parent_id"] = None

    return new_row


# ============================================================================
# UPSERT via psycopg2
# ============================================================================
def pg_upsert(
    conn, table: str, batch: list, conflict_col: str = "id",
) -> Tuple[int, int]:
    """Batch upsert via psycopg2 execute_values."""
    from psycopg2.extras import execute_values, Json

    if not batch:
        return 0, 0

    columns = [c for c in batch[0].keys() if not c.startswith("__")]
    conflict_cols = [c.strip() for c in conflict_col.split(",")]
    update_cols = [c for c in columns if c not in conflict_cols]

    cols_quoted = ", ".join(f'"{c}"' for c in columns)
    conflict_quoted = ", ".join(f'"{c}"' for c in conflict_cols)

    if WRITE_MODE == "upsert" and update_cols:
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        sql = (
            f'INSERT INTO public."{table}" ({cols_quoted}) VALUES %s '
            f'ON CONFLICT ({conflict_quoted}) DO UPDATE SET {update_set}'
        )
    else:
        # Modo padrão (insert): usado no nightly após truncate.
        # É significativamente mais rápido que upsert com DO UPDATE.
        sql = f'INSERT INTO public."{table}" ({cols_quoted}) VALUES %s'

    jsonb_cols = {c for c in columns if c.endswith("_json")}

    def _row_to_values(row: dict) -> Tuple[Any, ...]:
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
        return tuple(row_vals)

    values = [_row_to_values(row) for row in batch]

    def _execute_batch(batch_values: List[Tuple[Any, ...]]) -> None:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch_values, page_size=len(batch_values))

    row_error_logs = {"count": 0}

    def _retry_with_split(rows_and_values: List[Tuple[dict, Tuple[Any, ...]]]) -> Tuple[int, int]:
        if not rows_and_values:
            return 0, 0

        batch_values = [row_vals for _, row_vals in rows_and_values]
        try:
            _execute_batch(batch_values)
            conn.commit()
            return len(rows_and_values), 0
        except Exception as split_err:
            conn.rollback()
            if len(rows_and_values) == 1:
                source_row, _ = rows_and_values[0]
                legacy_id = source_row.get("__legacy_id")
                repaired_rows = _repair_row_for_insert(table, source_row, str(split_err))
                for repaired_row in repaired_rows:
                    try:
                        _execute_batch([_row_to_values(repaired_row)])
                        conn.commit()
                        log(
                            f"    Row repaired ({table}): {_probable_constraint(str(split_err)) or 'constraint'}",
                            "WARN",
                        )
                        return 1, 0
                    except Exception as repair_err:
                        conn.rollback()
                        split_err = repair_err
                msg = str(split_err)[:200]
                if row_error_logs["count"] < 3 and msg and "Row error:" not in msg:
                    log(f"    Row error: {msg}", "WARN")
                    row_error_logs["count"] += 1
                record_etl_error(table, legacy_id, str(split_err), stage="row_insert")
                return 0, 1

            mid = len(rows_and_values) // 2
            left_ok, left_err = _retry_with_split(rows_and_values[:mid])
            right_ok, right_err = _retry_with_split(rows_and_values[mid:])
            return left_ok + right_ok, left_err + right_err

    try:
        _execute_batch(values)
        conn.commit()
        return len(batch), 0
    except Exception as batch_err:
        conn.rollback()
        # Log do erro do batch
        err_str = str(batch_err)
        if len(err_str) > 300:
            err_str = err_str[:300] + "..."
        log(f"    Batch error ({table}): {err_str}", "WARN")
        record_etl_error(table, None, str(batch_err), stage="batch_insert")
        # Fallback: split em blocos menores para evitar commit por linha quando há poucos erros.
        return _retry_with_split(list(zip(batch, values)))


def pg_flush(conn, table, batch, conflict_col, ok, err):
    if batch:
        ins, e = pg_upsert(conn, table, batch, conflict_col)
        ok += ins
        err += e
    return [], ok, err


def _apply_self_ref_updates(
    conn,
    table: str,
    ref_col: str,
    updates: List[Tuple[str, str]],
) -> Tuple[int, int]:
    """Aplica FKs auto-referentes em uma segunda fase, sem exigir superuser."""
    from psycopg2.extras import execute_values

    if not updates:
        return 0, 0

    deduped: Dict[str, str] = {}
    for row_id, ref_id in updates:
        if row_id and ref_id:
            deduped[str(row_id)] = str(ref_id)

    if not deduped:
        return 0, 0

    sql = f'''
        UPDATE public."{table}" AS target
        SET "{ref_col}" = src.ref_id
        FROM (VALUES %s) AS src(id, ref_id)
        JOIN public."{table}" AS parent ON parent."id" = src.ref_id
        WHERE target."id" = src.id
          AND target."{ref_col}" IS DISTINCT FROM src.ref_id
    '''

    applied = 0
    items = list(deduped.items())

    for i in range(0, len(items), BATCH_SIZE):
        chunk = items[i:i + BATCH_SIZE]
        with conn.cursor() as cur:
            execute_values(
                cur,
                sql,
                chunk,
                template="(%s::uuid, %s::uuid)",
                page_size=len(chunk),
            )
            applied += max(cur.rowcount, 0)
        conn.commit()

    skipped = len(items) - applied
    if skipped:
        log(
            f"  [WARN] {table}: {skipped:,} auto-relacionamentos nÃ£o puderam ser atualizados",
            "WARN",
        )
    return applied, skipped


def _set_replication_role(pg, role: str) -> None:
    """Ativa/desativa checagem de FK e triggers para a sessão PostgreSQL corrente.

    role='replica' → FK checks + triggers desativados.
                     Seguro para tabelas com auto-relacionamento (self-ref FKs)
                     quando todos os registros já estão sendo carregados no mesmo
                     lote e a tabela pai está garantidamente na mesma sessão.
    role='origin'  → comportamento padrão (restaurar SEMPRE após a carga).

    Requer: usuário PostgreSQL com privilégio de superuser ou BYPASSRLS.
    Nota: SET session_replication_role afeta toda a SESSÃO, não apenas a transação.
    """
    assert role in ("replica", "origin"), f"role inválido: {role!r}"
    with pg.cursor() as cur:
        cur.execute(f"SET session_replication_role = '{role}'")
    pg.commit()
    log(f"  [FK-BYPASS] session_replication_role → '{role}'")


# ============================================================================
# ETL PRINCIPAL
# ============================================================================
def run_precheck(mysql_cursor) -> bool:
    """
    Valida transformações sem escrita no Supabase.

    DRY-RUN: mostra estrutura de blocos, contagem de linhas por tabela no MySQL,
    colunas NOT NULL críticas e auto-referências. Retorna True quando todas as
    verificações de transformação críticas passam.
    """
    log("PRECHECK: validando transformações (sem escrita)...")
    log("")
    log("─" * 70)
    log("DRY-RUN — SIMULAÇÃO DE CARGA POR BLOCO TOPOLÓGICO")
    log("─" * 70)
    for block_num, block_tables in enumerate(EXEC_BLOCKS):
        log(f"  BLOCO {block_num} ({len(block_tables)} tabelas):")
        for table in block_tables:
            source = SOURCE_TABLE_OVERRIDES.get(table) or MYSQL_TABLE_NAME_MAP.get(table, table)
            if table in DERIVED_TABLES:
                count_str = "derivado de is_clientes"
            else:
                try:
                    mysql_cursor.execute(f"SELECT COUNT(*) c FROM `{source}`")
                    row = mysql_cursor.fetchone()
                    count_str = f"{row['c']:,} linhas" if row else "?"
                except Exception:
                    count_str = "tabela não encontrada"
            nonnull = list(REQUIRED_NONNULL_COLS.get(table, set()))
            self_ref = SELF_REF_TABLES.get(table)
            flags = []
            if nonnull:
                flags.append(f"NOT NULL: {nonnull}")
            if self_ref:
                flags.append(f"self-ref({self_ref}) via session_replication_role")
            suffix = " │ " + " │ ".join(flags) if flags else ""
            log(f"    {table:<42s} → {count_str}{suffix}")
    log("─" * 70)
    log("")
    passed = True

    mapping_errors = validate_mapping_contract(mysql_cursor)
    if mapping_errors:
        passed = False
        for item in mapping_errors[:30]:
            log(f"PRECHECK FAIL [mapping]: {item}", "ERROR")
        if len(mapping_errors) > 30:
            log(f"PRECHECK FAIL [mapping]: ... +{len(mapping_errors) - 30} erros", "ERROR")

    mysql_cursor.execute("SELECT * FROM `is_clientes` ORDER BY `id`")
    rows = mysql_cursor.fetchall()
    seen_emails: Dict[str, int] = {}
    kept = pf = pj = dropped_split = 0
    for row in rows:
        try:
            base = transform_cliente(row)
        except Exception:
            dropped_split += 1
            continue
        if not base:
            continue
        email = (base.get("email_log") or "").strip().lower()
        if email:
            seen_emails[email] = seen_emails.get(email, 0) + 1
        kept += 1
        if transform_cliente_pf(row):
            pf += 1
        elif transform_cliente_pj(row):
            pj += 1
        else:
            dropped_split += 1
    dup_emails = sum(1 for _, n in seen_emails.items() if n > 1)
    log(
        f"PRECHECK clientes: source={len(rows):,} kept={kept:,} PF={pf:,} "
        f"PJ={pj:,} split_drop={dropped_split:,} dup_emails={dup_emails:,}"
    )
    if dropped_split > 0:
        passed = False
        log("PRECHECK FAIL: clientes sem classificação PF/PJ após transformação", "ERROR")

    expected_pf_min = int(os.getenv("PRECHECK_MIN_CLIENTES_PF", "4589"))
    expected_pj_min = int(os.getenv("PRECHECK_MIN_CLIENTES_PJ", "3021"))
    if pf < expected_pf_min:
        passed = False
        log(f"PRECHECK FAIL: PF abaixo do mínimo esperado ({pf} < {expected_pf_min})", "ERROR")
    if pj < expected_pj_min:
        passed = False
        log(f"PRECHECK FAIL: PJ abaixo do mínimo esperado ({pj} < {expected_pj_min})", "ERROR")

    mysql_cursor.execute(
        "SELECT `id`,`tipo`,`valor`,`produtos`,`cliente`,`codigo`,`uso`,`limite`,`inicio`,`fim`,`pedido_min`,`primeira_compra`,`arquivado` FROM `is_mkt_cupons`"
    )
    cup_rows = mysql_cursor.fetchall()
    cupom_tipo_invalid = 0
    cupom_links = 0
    for row in cup_rows:
        transformed = transform_row(row, "is_mkt_cupons", COLUMN_MAPPING["is_mkt_cupons"])
        if not transformed or transformed.get("tipo") not in ("amount", "percent"):
            cupom_tipo_invalid += 1
        cupom_links += len(transform_cupom_produtos(row))
    log(
        f"PRECHECK cupons: source={len(cup_rows):,} tipo_invalid={cupom_tipo_invalid:,} links_produtos={cupom_links:,}"
    )
    if cupom_tipo_invalid > 0:
        passed = False
        log("PRECHECK FAIL: mapeamento de tipo de cupons inválido", "ERROR")

    mysql_cursor.execute("SELECT `id`,`chave`,`pai` FROM `is_produtos_categorias`")
    cat_rows = mysql_cursor.fetchall()
    chave_map = {str(r.get("chave")).strip(): r.get("id") for r in cat_rows if r.get("chave")}
    missing_parent = with_parent = 0
    for row in cat_rows:
        pai = str(row.get("pai") or "").strip()
        if pai and pai != "0":
            with_parent += 1
            if pai not in chave_map:
                missing_parent += 1
    log(f"PRECHECK categorias: source={len(cat_rows):,} com_parent={with_parent:,} parent_missing={missing_parent:,}")
    if missing_parent > 0:
        passed = False
        log("PRECHECK FAIL: categorias com pai não resolvido por chave", "ERROR")

    mysql_cursor.execute(
        """
        SELECT `id`,`tipo`,`valor`,`status`,`categoria`,`carteira`,`fornecedor`,
               `pdv`,`funcionario`,`vendedor`,`caixa`,`centro_custo`
        FROM `is_financeiro_lancamentos`
        """
    )
    fin_rows = mysql_cursor.fetchall()
    fin_tipo_invalid = 0
    fin_without_relations = 0
    fin_with_multiple_relations = 0
    for row in fin_rows:
        try:
            mapped_tipo = transform_column(
                "tipo",
                "tipo",
                row.get("tipo"),
                "is_financeiro_lancamentos",
                TABLE_TYPE_OVERRIDES.get("is_financeiro_lancamentos", {}),
            )
            if mapped_tipo not in (1, 2):
                fin_tipo_invalid += 1
            transformed = transform_row(row, "is_financeiro_lancamentos", COLUMN_MAPPING["is_financeiro_lancamentos"])
            if transformed:
                relations = [c for c in _FINANCE_RELATION_COLS if transformed.get(c) is not None]
                if not relations:
                    fin_without_relations += 1
                elif len(relations) > 1:
                    fin_with_multiple_relations += 1
        except Exception:
            fin_tipo_invalid += 1
    log(
        "PRECHECK financeiro: "
        f"source={len(fin_rows):,} "
        f"tipo_invalid={fin_tipo_invalid:,} "
        f"sem_relacoes={fin_without_relations:,} "
        f"multiplas_relacoes={fin_with_multiple_relations:,}"
    )
    if fin_tipo_invalid > 0:
        passed = False
        log("PRECHECK FAIL: tipo financeiro inválido após transformação", "ERROR")
    expected_fin_min = int(os.getenv("PRECHECK_MIN_FINANCEIRO_LANCAMENTOS", "97985"))
    if len(fin_rows) < expected_fin_min:
        passed = False
        log(
            f"PRECHECK FAIL: financeiro_lancamentos abaixo do mínimo esperado ({len(fin_rows)} < {expected_fin_min})",
            "ERROR",
        )

    mysql_cursor.execute("SELECT `id`,`parcelas` FROM `is_pedidos_pagamentos`")
    pag_rows = mysql_cursor.fetchall()
    parcelas_invalid = 0
    for row in pag_rows:
        transformed = transform_pagamento({"id": row.get("id"), "parcelas": row.get("parcelas")})
        qty = transformed.get("parcelas_qtd") if transformed else None
        if qty is not None and qty < 1:
            parcelas_invalid += 1
    log(f"PRECHECK pagamentos: source={len(pag_rows):,} parcelas_invalid={parcelas_invalid:,}")
    if parcelas_invalid > 0:
        passed = False
        log("PRECHECK FAIL: parcelas_qtd inválido (<1)", "ERROR")

    mysql_cursor.execute("SELECT COUNT(*) c FROM `is_clientes_enderecos`")
    end_table = mysql_cursor.fetchone()["c"]
    mysql_cursor.execute(
        "SELECT COUNT(*) c FROM `is_clientes` WHERE TRIM(COALESCE(`cep`,''))<>'' OR TRIM(COALESCE(`logradouro`,''))<>''"
    )
    end_derived = mysql_cursor.fetchone()["c"]
    log(f"PRECHECK endereços: tabela={end_table:,} derivados_de_clientes={end_derived:,}")

    if passed:
        log("PRECHECK OK: transformações críticas validadas.")
    return passed


def run_etl():
    global VALID_FK_IDS
    ETL_ERRORS.clear()
    etl_start = time.monotonic()
    mysql = None
    cursor = None
    pg = None
    total_ok = 0
    total_err = 0
    report_payload = None

    log("=" * 70)
    log("ETL v12 — MySQL legado → Supabase │ Blocos Topológicos + FK-Bypass")
    log("=" * 70)
    log(f"Blocos: {len(EXEC_BLOCKS)} │ Tabelas: {len(EXEC_ORDER)} │ Batch: {BATCH_SIZE} │ WriteMode: {WRITE_MODE}")

    if VALIDATE_ONLY:
        try:
            mysql = get_mysql()
            cursor = mysql.cursor(dictionary=True)
            VALID_FK_IDS = build_valid_fk_ids(cursor)
            log("Modo ETL_VALIDATE_ONLY=1 — executando PRECHECK (dry-run) e encerrando.")
            ok = run_precheck(cursor)
            cursor.close()
            mysql.close()
            if ok:
                log("")
                log("PRECHECK OK — nenhuma linha foi escrita no Supabase.")
            sys.exit(0 if ok else 1)
        except Exception as e:
            log(f"PRECHECK erro: {e}", "ERROR")
            sys.exit(1)

    # ── Conexões ────────────────────────────────────────────────────────────
    try:
        mysql = get_mysql()
        cursor = mysql.cursor(dictionary=True)
        log("Conexão MySQL OK")
        VALID_FK_IDS = build_valid_fk_ids(cursor)
        mapping_errors = validate_mapping_contract(cursor)
        if mapping_errors:
            for item in mapping_errors[:30]:
                log(f"Startup mapping validation FAIL: {item}", "ERROR")
            if len(mapping_errors) > 30:
                log(f"  ... +{len(mapping_errors) - 30} erros adicionais", "ERROR")
            raise RuntimeError("mapping validation failed before load")
        pg = get_pg()
        log("Conexão PostgreSQL OK")
    except Exception as e:
        log(f"ERRO de conexão: {e}", "ERROR")
        sys.exit(1)

    # ── Tabelas a processar ──────────────────────────────────────────────────
    if ONLY_TABLES:
        tables_to_process_set: Set[str] = ONLY_TABLES
        log(f"ETL_ONLY_TABLES: {sorted(tables_to_process_set)}")
    else:
        tables_to_process_set = set(EXEC_ORDER)

    # ── Estado compartilhado entre blocos ────────────────────────────────────
    stats: Dict[str, Dict[str, int]] = {}
    seen_emails: Dict[str, int] = {}
    pf_list: List[dict] = []        # populado em Bloco 0 (is_clientes) → consumido em Bloco 1
    pj_list: List[dict] = []        # populado em Bloco 0 (is_clientes) → consumido em Bloco 1
    addr_list: List[dict] = []      # populado em Bloco 0 (is_clientes) → consumido em Bloco 1
    cupons_produtos_list: List[dict] = []  # populado em Bloco 1 → consumido em Bloco 2
    cupom_codigos: Set[str] = set()
    categoria_slug_map: Dict[str, int] = {}

    try:
        categoria_slug_map = build_categoria_slug_map(cursor)
        log(f"Categorias slug map: {len(categoria_slug_map)} entradas")
    except Exception:
        log("Aviso: não foi possível carregar slug map de categorias", "WARN")

    try:
        cursor.execute("SELECT `codigo` FROM `is_mkt_cupons`")
        for row in cursor.fetchall():
            c = row.get("codigo")
            if c:
                cupom_codigos.add(str(c).strip())
        log(f"Cupons códigos carregados: {len(cupom_codigos)}")
    except Exception:
        log("Aviso: não foi possível carregar códigos de cupons", "WARN")

    # ── LOOP DE BLOCOS TOPOLÓGICOS ────────────────────────────────────────────
    for block_num, block_tables in enumerate(EXEC_BLOCKS):
        tables_in_block = [t for t in block_tables if t in tables_to_process_set]
        if not tables_in_block:
            continue

        block_start = time.monotonic()
        log("")
        log("═" * 70)
        log(f"BLOCO {block_num} │ {len(tables_in_block)} tabela(s): {', '.join(tables_in_block)}")
        log("═" * 70)

        for table in tables_in_block:
            mapping = COLUMN_MAPPING.get(table)
            if not mapping:
                log(f"  [{table}] Sem mapping — skip", "WARN")
                continue

            conflict_col = CONFLICT_COLS.get(table, "id")
            mysql_table = MYSQL_TABLE_NAME_MAP.get(table, table)

            log("")
            log(f"{'─'*50}")
            log(f"TABELA: {table}" + (f" (MySQL: {mysql_table})" if mysql_table != table else ""))
            log(f"{'─'*50}")

            # ── Processadores especializados ──────────────────────────────
            if table == "is_clientes":
                ok, err = _process_clientes(cursor, pg, seen_emails, pf_list, pj_list, addr_list)

            elif table == "is_clientes_pf":
                ok, err = _process_derived_pf(pg, pf_list)

            elif table == "is_clientes_pj":
                ok, err = _process_derived_pj(pg, pj_list)

            elif table == "is_clientes_enderecos":
                ok, err = _process_clientes_enderecos(cursor, pg, addr_list)

            elif table == "is_mkt_cupons":
                ok, err = _process_mkt_cupons(cursor, pg, cupons_produtos_list)

            elif table == "is_mkt_cupons_produtos":
                ok, err = _process_mkt_cupons_produtos(pg, cupons_produtos_list)

            elif table == "is_produtos_categorias":
                # self-ref parent_id → usa session_replication_role
                ok, err = _process_categorias(cursor, pg, categoria_slug_map)

            elif table == "is_pedidos_fretes_entregas":
                ok, err = _process_fretes_entregas(cursor, pg)

            elif table == "is_pedidos":
                ok, err = _process_pedidos(cursor, pg, cupom_codigos)

            elif table == "is_pedidos_pagamentos":
                # self-ref original_id → usa session_replication_role
                ok, err = _process_pagamentos(cursor, pg)

            else:
                # Processador genérico (sem self-ref)
                ok, err = _process_generic(
                    cursor, pg, table, mysql_table, mapping, conflict_col, None, {}
                )

            stats[table] = {"ok": ok, "err": err}

        # ── Resumo do bloco ───────────────────────────────────────────────
        block_elapsed = time.monotonic() - block_start
        block_ok  = sum(stats.get(t, {}).get("ok",  0) for t in tables_in_block)
        block_err = sum(stats.get(t, {}).get("err", 0) for t in tables_in_block)
        block_inserted  = block_ok
        block_rejected  = block_err
        log("")
        log(
            f"BLOCO {block_num} CONCLUÍDO │ "
            f"inseridas={block_inserted:,} │ "
            f"rejeitadas={block_rejected:,} │ "
            f"{block_elapsed:.1f}s"
        )

    # ── RESUMO FINAL ──────────────────────────────────────────────────────────
    total_elapsed = time.monotonic() - etl_start
    log("")
    log("=" * 70)
    log("RESUMO FINAL")
    log("=" * 70)

    total_ok  = sum(s["ok"]  for s in stats.values())
    total_err = sum(s["err"] for s in stats.values())

    for block_num, block_tables in enumerate(EXEC_BLOCKS):
        in_stats = [t for t in block_tables if t in stats]
        if not in_stats:
            continue
        log(f"  ── Bloco {block_num} ──")
        for t in in_stats:
            s = stats[t]
            icon = "✓" if s["err"] == 0 else "✗"
            log(f"    {icon} {t:<42s}  inseridas={s['ok']:>7,}  rejeitadas={s['err']:>5,}")

    log("")
    log(f"  TOTAL: {total_ok:,} inseridas │ {total_err:,} rejeitadas │ {total_elapsed:.1f}s")

    cursor.close()
    mysql.close()
    pg.close()

    if total_err > 0 or ETL_ERRORS:
        report_payload = persist_etl_error_report(total_err, stats)
        if total_err > 0:
            persist_error_event(
                run_id=RUN_ID,
                script_name="etl/run.py",
                step_name="run_etl",
                phase="etl",
                event_type="etl_failure",
                message=f"ETL failed with {total_err} row-level errors",
                error_class="RuntimeError",
                details={
                    "stats": stats,
                    "total_ok": total_ok,
                    "total_err": total_err,
                    "error_report_path": str(ERROR_REPORT_PATH),
                    "error_report": report_payload,
                },
            )

    if total_err > 0:
        log(f"  FAIL-FAST: {total_err} erros encontrados", "ERROR")
        raise RuntimeError(f"ETL failed with {total_err} row-level errors")

    log("")
    log("ETL COMPLETO!")


# ============================================================================
# PROCESSADORES POR TABELA
# ============================================================================

def _process_clientes(cursor, pg, seen_emails, pf_list, pj_list, addr_list):
    """Processa is_clientes + deriva PF, PJ e endereços."""
    # Buscar TODAS as colunas necessárias do MySQL
    cols = [
        "id", "saldo", "tipo", "telefone", "celular", "email_log", "senha_log",
        "ultimo_acesso", "data", "ip", "status", "retirada", "retirada_limite",
        "revendedor", "pdv", "wpp_verificado", "logotipo", "pagarme_id",
        # Campos PF
        "nome", "sobrenome", "nascimento", "cpf", "sexo",
        # Campos PJ
        "razao_social", "fantasia", "ie", "cnpj",
        # Campos endereço
        "cep", "logradouro", "numero", "bairro", "complemento", "cidade", "estado",
    ]
    cursor.execute(f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `is_clientes`")

    ok, err = 0, 0
    batch: List[dict] = []

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            try:
                t_row = transform_cliente(row)
                if not t_row:
                    continue
                email = t_row.get("email_log")
                if email:
                    count = seen_emails.get(email, 0)
                    if count > 0:
                        # Mantém todos os clientes sem quebrar UNIQUE(email_log)
                        t_row["email_log"] = f"{email}__dup_{row.get('id')}"
                    seen_emails[email] = count + 1

                batch.append(t_row)

                # PF ou PJ
                pf = transform_cliente_pf(row)
                pj = transform_cliente_pj(row)
                if pf:
                    pf_list.append(pf)
                elif pj:
                    pj_list.append(pj)
                else:
                    raise ValueError(
                        f"cliente sem split PF/PJ (id={row.get('id')}, tipo={row.get('tipo')!r})"
                    )

                # Endereço padrão
                addr = transform_cliente_endereco_from_cliente(row)
                if addr:
                    addr_list.append(addr)
            except Exception as e:
                if err < 5:
                    log(f"    Err cliente {row.get('id')}: {e}", "WARN")
                record_etl_error("is_clientes", row.get("id"), str(e), stage="transform")
                err += 1

        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, "is_clientes", batch, "id", ok, err)

    batch, ok, err = pg_flush(pg, "is_clientes", batch, "id", ok, err)
    log(f"  → OK={ok:,}  ERR={err:,}  (PF={len(pf_list)}, PJ={len(pj_list)}, ADDR={len(addr_list)})")
    return ok, err


def _process_clientes_enderecos(cursor, pg, addr_from_clientes):
    """Processa is_clientes_enderecos (tabela MySQL) + endereços derivados de is_clientes."""
    mapping = COLUMN_MAPPING["is_clientes_enderecos"]
    cols = list(set(mapping.values()))
    if "id" not in cols:
        cols.append("id")

    try:
        cursor.execute(f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `is_clientes_enderecos`")
    except Exception as e:
        log(f"    Skip (MySQL table missing): {e}", "WARN")
        # Apenas inserir os endereços derivados
        ok, err = 0, 0
        for i in range(0, len(addr_from_clientes), BATCH_SIZE):
            chunk = addr_from_clientes[i:i + BATCH_SIZE]
            ins, e = pg_upsert(pg, "is_clientes_enderecos", chunk, "id")
            ok += ins
            err += e
        log(f"  → OK={ok:,} (derivados)  ERR={err:,}")
        return ok, err

    ok, err = 0, 0
    batch: List[dict] = []

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            try:
                t_row = transform_row(row, "is_clientes_enderecos", mapping)
                if t_row and t_row.get("id"):
                    batch.append(t_row)
            except Exception as e:
                record_etl_error("is_clientes_enderecos", row.get("id"), str(e), stage="transform")
                err += 1

        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, "is_clientes_enderecos", batch, "id", ok, err)

    batch, ok, err = pg_flush(pg, "is_clientes_enderecos", batch, "id", ok, err)

    # Adicionar endereços derivados de is_clientes (deduplicados por id)
    dedup_addr = []
    seen_addr_ids: Set[str] = set()
    for addr in addr_from_clientes:
        addr_id = addr.get("id")
        if not addr_id or addr_id in seen_addr_ids:
            continue
        seen_addr_ids.add(addr_id)
        dedup_addr.append(addr)
    addr_ok = 0
    for i in range(0, len(dedup_addr), BATCH_SIZE):
        chunk = dedup_addr[i:i + BATCH_SIZE]
        ins, e = pg_upsert(pg, "is_clientes_enderecos", chunk, "id")
        addr_ok += ins
        err += e

    log(f"  → OK={ok:,} (tabela) + {addr_ok:,} (derivados)  ERR={err:,}")
    return ok + addr_ok, err


def _process_mkt_cupons(cursor, pg, cupons_produtos_list):
    """Processa is_mkt_cupons + extrai produtos para junction table."""
    mapping = COLUMN_MAPPING["is_mkt_cupons"]
    cols = list(set(mapping.values()))
    if "id" not in cols:
        cols.append("id")
    # Precisamos do campo 'produtos' para extrair cupons_produtos
    if "produtos" not in cols:
        cols.append("produtos")

    cursor.execute(f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `is_mkt_cupons`")

    ok, err = 0, 0
    batch: List[dict] = []

    for row in cursor.fetchall():
        try:
            t_row = transform_row(row, "is_mkt_cupons", mapping)
            if t_row and t_row.get("id"):
                batch.append(t_row)
                # Extrair produtos
                prods = transform_cupom_produtos(row)
                cupons_produtos_list.extend(prods)
        except Exception as e:
            if err < 5:
                log(f"    Err cupom {row.get('id')}: {e}", "WARN")
            record_etl_error("is_mkt_cupons", row.get("id"), str(e), stage="transform")
            err += 1

    batch, ok, err = pg_flush(pg, "is_mkt_cupons", batch, "id", ok, err)
    log(f"  → OK={ok:,}  ERR={err:,}  (produtos_links={len(cupons_produtos_list)})")
    return ok, err


def _process_mkt_cupons_produtos(pg, cupons_produtos_list):
    """Insere is_mkt_cupons_produtos (junction table)."""
    if not cupons_produtos_list:
        log("  → Nenhum registro")
        return 0, 0

    # Deduplicar
    seen = set()
    unique = []
    for row in cupons_produtos_list:
        key = (row["cupom_id"], row["produto_id"])
        if key not in seen:
            seen.add(key)
            unique.append(row)

    ok, err = 0, 0
    for i in range(0, len(unique), BATCH_SIZE):
        chunk = unique[i:i + BATCH_SIZE]
        ins, e = pg_upsert(pg, "is_mkt_cupons_produtos", chunk, "cupom_id,produto_id")
        ok += ins
        err += e

    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_categorias(cursor, pg, slug_map):
    """Processa is_produtos_categorias com resolução de parent_id via chave.

    Usa SET session_replication_role = 'replica' para inserir parent_id diretamente,
    sem necessidade de 2-pass UPDATE. transform_categoria() já resolve o parent_id
    via slug_map (chave → legacy_id → UUID5). Todos os registros são inseridos em
    uma única passagem com as referências já resolvidas.

    Deduplica slugs: is_produtos_categorias.slug tem constraint UNIQUE; MySQL pode
    ter slugs duplicados. O segundo registro com slug igual tem o slug zerado (NULL)
    para evitar violação da constraint e o consequente batch retry cascade.
    """
    cursor.execute("SELECT * FROM `is_produtos_categorias`")

    ok, err = 0, 0
    batch: List[dict] = []
    self_ref_updates: List[Tuple[str, str]] = []
    _ = self_ref_updates
    seen_slugs: Set[str] = set()

    for row in cursor.fetchall():
        try:
            t_row = transform_categoria(row, slug_map)
            if t_row and t_row.get("id"):
                # Deduplicar slug — UNIQUE constraint
                slug = t_row.get("slug")
                if slug:
                    if slug in seen_slugs:
                        t_row["slug"] = None  # null out duplicate slug
                    else:
                        seen_slugs.add(slug)
                # status NOT NULL DEFAULT 1
                if t_row.get("status") is None:
                    t_row["status"] = 1
                parent_id = t_row.get("parent_id")
                if parent_id:
                    self_ref_updates.append((t_row["id"], parent_id))
                    t_row["parent_id"] = None
                batch.append(t_row)  # parent_id já resolvido por transform_categoria
        except Exception as e:
            if err < 3:
                log(f"    Err cat {row.get('id')}: {e}", "WARN")
            record_etl_error("is_produtos_categorias", row.get("id"), str(e), stage="transform")
            err += 1

    if not batch:
        log(f"  → OK=0  ERR={err:,}  (batch vazio)")
        return 0, err

    # Desativar FK checks para permitir inserção com parent_id (self-ref) em passagem única
    batch, ok, err = pg_flush(pg, "is_produtos_categorias", batch, "id", ok, err)
    if self_ref_updates:
        updated, skipped = _apply_self_ref_updates(
            pg,
            "is_produtos_categorias",
            "parent_id",
            self_ref_updates,
        )
        log(f"  â†’ parent_id aplicado: OK={updated:,}  SKIP={skipped:,}")

    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_fretes_entregas(cursor, pg):
    """Processa is_pedidos_fretes_envios (MySQL) → is_pedidos_fretes_entregas (Supabase)."""
    try:
        cursor.execute("SELECT * FROM `is_pedidos_fretes_envios`")
    except Exception as e:
        log(f"    Skip: {e}", "WARN")
        return 0, 0

    ok, err = 0, 0
    batch: List[dict] = []

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            try:
                t_row = transform_pedidos_fretes_entregas(row)
                if t_row:
                    batch.append(t_row)
            except Exception as e:
                record_etl_error("is_pedidos_fretes_entregas", row.get("id"), str(e), stage="transform")
                err += 1
        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, "is_pedidos_fretes_entregas", batch, "id", ok, err)

    batch, ok, err = pg_flush(pg, "is_pedidos_fretes_entregas", batch, "id", ok, err)
    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_pedidos(cursor, pg, cupom_codigos):
    """Processa is_pedidos com validação de FK cupom."""
    mapping = COLUMN_MAPPING["is_pedidos"]
    cols = list(set(mapping.values()))
    if "id" not in cols:
        cols.append("id")

    cursor.execute(f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `is_pedidos`")

    ok, err = 0, 0
    batch: List[dict] = []
    self_ref_updates: List[Tuple[str, str]] = []
    processed = 0
    next_progress = 20_000

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        processed += len(rows)
        for row in rows:
            try:
                t_row = transform_pedido(row, cupom_codigos)
                if t_row and t_row.get("id"):
                    # cliente_id NOT NULL — skip orphan pedidos
                    if t_row.get("cliente_id") is None:
                        record_etl_error("is_pedidos", row.get("id"),
                            "cliente_id is None (cliente not found) — skipping",
                            stage="required_nonnull_skip")
                        err += 1
                        continue
                    batch.append(t_row)
            except Exception as e:
                if err < 5:
                    log(f"    Err pedido {row.get('id')}: {e}", "WARN")
                record_etl_error("is_pedidos", row.get("id"), str(e), stage="transform")
                err += 1
        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, "is_pedidos", batch, "id", ok, err)
        if processed >= next_progress:
            log(f"  … progress is_pedidos: lidos={processed:,} OK={ok:,} ERR={err:,}")
            next_progress += 20_000

    batch, ok, err = pg_flush(pg, "is_pedidos", batch, "id", ok, err)
    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_derived_pf(pg, pf_list: List[dict]) -> Tuple[int, int]:
    """Insere registros de is_clientes_pf derivados de is_clientes (Bloco 1).

    pf_list foi populado durante _process_clientes no Bloco 0.
    """
    if not pf_list:
        log("  → OK=0  ERR=0  (pf_list vazia)")
        return 0, 0
    valid = [p for p in pf_list if p.get("cliente_id")]
    ok, err = 0, 0
    for i in range(0, len(valid), BATCH_SIZE):
        chunk = valid[i:i + BATCH_SIZE]
        ins, e = pg_upsert(pg, "is_clientes_pf", chunk, "cliente_id")
        ok += ins
        err += e
    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_derived_pj(pg, pj_list: List[dict]) -> Tuple[int, int]:
    """Insere registros de is_clientes_pj derivados de is_clientes (Bloco 1).

    pj_list foi populado durante _process_clientes no Bloco 0.
    """
    if not pj_list:
        log("  → OK=0  ERR=0  (pj_list vazia)")
        return 0, 0
    valid = [p for p in pj_list if p.get("cliente_id")]
    ok, err = 0, 0
    for i in range(0, len(valid), BATCH_SIZE):
        chunk = valid[i:i + BATCH_SIZE]
        ins, e = pg_upsert(pg, "is_clientes_pj", chunk, "cliente_id")
        ok += ins
        err += e
    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_pagamentos(cursor, pg):
    """Processa is_pedidos_pagamentos com parcelas split e original_id (self-ref).

    Usa SET session_replication_role = 'replica' ANTES de iniciar os inserts para
    permitir que original_id (auto-referência) seja gravado diretamente, sem 2-pass.
    A restauração para 'origin' ocorre no bloco finally, garantindo que FK checks
    sejam re-ativados mesmo em caso de erro.
    """
    mapping = COLUMN_MAPPING["is_pedidos_pagamentos"]
    cols = list(set(mapping.values()))
    if "id" not in cols:
        cols.append("id")

    cursor.execute(f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `is_pedidos_pagamentos`")

    ok, err = 0, 0
    batch: List[dict] = []
    self_ref_updates: List[Tuple[str, str]] = []
    processed = 0
    next_progress = 20_000

    # Desativar FK checks para suportar original_id (self-ref) em passagem única
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        processed += len(rows)
        for row in rows:
            try:
                    t_row = transform_pagamento(row)
                    if t_row and t_row.get("id"):
                        # cliente_id NOT NULL — skip orphan pagamentos
                        if t_row.get("cliente_id") is None:
                            record_etl_error(
                                "is_pedidos_pagamentos", row.get("id"),
                                "cliente_id is None — skipping",
                                stage="required_nonnull_skip",
                            )
                            err += 1
                            continue
                        # forma NOT NULL — fallback
                        if t_row.get("forma") is None:
                            t_row["forma"] = ""
                        original_id = t_row.get("original_id")
                        if original_id:
                            self_ref_updates.append((t_row["id"], original_id))
                            t_row["original_id"] = None
                        # original_id: mantido no row (FK disabled via replication_role)
                        batch.append(t_row)
            except Exception as e:
                if err < 5:
                    log(f"    Err pag {row.get('id')}: {e}", "WARN")
                record_etl_error("is_pedidos_pagamentos", row.get("id"), str(e), stage="transform")
                err += 1
        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, "is_pedidos_pagamentos", batch, "id", ok, err)
        if processed >= next_progress:
                log(f"  … progress is_pedidos_pagamentos: lidos={processed:,} OK={ok:,} ERR={err:,}")
                next_progress += 20_000

    batch, ok, err = pg_flush(pg, "is_pedidos_pagamentos", batch, "id", ok, err)

    if self_ref_updates:
        updated, skipped = _apply_self_ref_updates(
            pg,
            "is_pedidos_pagamentos",
            "original_id",
            self_ref_updates,
        )
        log(f"  â†’ original_id aplicado: OK={updated:,}  SKIP={skipped:,}")

    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_generic(cursor, pg, table, mysql_table, mapping, conflict_col, self_ref_col, self_ref_data):
    """Processa tabela genérica."""
    cols = list(set(mapping.values()))
    if "id" not in cols and table not in TABLES_WITHOUT_ID:
        cols.append("id")

    try:
        cursor.execute(f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `{mysql_table}`")
    except Exception as e:
        log(f"    Skip (MySQL): {e}", "WARN")
        return 0, 0

    required_nonnull = REQUIRED_NONNULL_COLS.get(table, set())
    fallbacks = NONNULL_FALLBACKS.get(table, {})

    ok, err = 0, 0
    batch: List[dict] = []
    has_id = table not in TABLES_WITHOUT_ID
    skipped = 0
    processed = 0
    next_progress = 20_000

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        processed += len(rows)
        for row in rows:
            try:
                t_row = transform_row(row, table, mapping)
                if t_row and (not has_id or t_row.get("id") is not None):
                    # Skip rows where required NOT NULL columns are None after transform
                    missing = [c for c in required_nonnull if t_row.get(c) is None]
                    if missing:
                        record_etl_error(table, row.get("id"),
                            f"Required NOT NULL columns are None: {missing}",
                            stage="required_nonnull_skip")
                        skipped += 1
                        continue
                    # Apply NOT NULL fallbacks for scalar columns
                    for col, default in fallbacks.items():
                        if t_row.get(col) is None:
                            t_row[col] = default
                    # Mutate rows to coerce invalid FK/CHECK values before insert
                    _mutator = POST_TRANSFORM_MUTATORS.get(table)
                    if _mutator:
                        t_row = _mutator(t_row)
                    # Pre-filter rows that would violate Supabase CHECK constraints
                    _validator = POST_TRANSFORM_VALIDATORS.get(table)
                    if _validator and not _validator(t_row):
                        record_etl_error(table, row.get("id"),
                            "violação de constraint (pre-filter)",
                            stage="pre-filter")
                        skipped += 1
                        continue
                    if self_ref_col and t_row.get(self_ref_col):
                        self_ref_data[table].append(
                            {"id": t_row["id"], self_ref_col: t_row[self_ref_col]}
                        )
                        t_row[self_ref_col] = None
                    batch.append(t_row)
            except Exception as e:
                record_etl_error(table, row.get("id"), str(e), stage="transform")
                err += 1
        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, table, batch, conflict_col, ok, err)
        if processed >= next_progress:
            log(f"  … progress {table}: lidos={processed:,} OK={ok:,} ERR={err:,}")
            next_progress += 20_000

    batch, ok, err = pg_flush(pg, table, batch, conflict_col, ok, err)
    skip_msg = f"  skipped={skipped:,}" if skipped else ""
    log(f"  → OK={ok:,}  ERR={err:,}{skip_msg}")
    return ok, err


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    run_etl()
