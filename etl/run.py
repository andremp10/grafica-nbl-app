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
import datetime as dt
from typing import Optional, Dict, Any, Set, List, Tuple

from dotenv import load_dotenv

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
UUID_NS = uuid.UUID("2a6b2c31-0f2a-4dfd-8cde-7b4b9b3f1c5a")
BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "2000"))
VALIDATE_ONLY = os.getenv("ETL_VALIDATE_ONLY", "0") == "1"
ONLY_TABLES_ENV = os.getenv("ETL_ONLY_TABLES", "").strip()
ONLY_TABLES: Set[str] = set(t.strip() for t in ONLY_TABLES_ENV.split(",") if t.strip())

# MySQL → Supabase: nomes de tabela diferentes
MYSQL_TABLE_NAME_MAP = {
    "is_pedidos_fretes_entregas": "is_pedidos_fretes_envios",
}

# Tabelas derivadas: lidas da mesma fonte MySQL mas gravadas em tabelas separadas
DERIVED_TABLES = {"is_clientes_pf", "is_clientes_pj"}

# Tabelas sem coluna 'id' no Supabase (PK composta)
TABLES_WITHOUT_ID: Set[str] = {"is_mkt_cupons_produtos"}

# ============================================================================
# LOGGING
# ============================================================================
_start_time = dt.datetime.now()


def log(msg: str, level: str = "INFO"):
    elapsed = dt.datetime.now() - _start_time
    mm, ss = divmod(int(elapsed.total_seconds()), 60)
    hh, mm = divmod(mm, 60)
    print(f"[{hh:02d}:{mm:02d}:{ss:02d}] [{level:5s}] {msg}", flush=True)


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


def safe_str(val: Any) -> Optional[str]:
    """Limpa strings sem destruir caracteres válidos como _, @, %."""
    s = to_str(val)
    if not s:
        return None
    # Tentar corrigir mojibake (UTF-8 lido como Latin-1)
    try:
        fixed = s.encode("latin1").decode("utf-8")
        if fixed != s:
            s = fixed
    except Exception:
        pass
    return s.strip() or None


# ============================================================================
# CONEXÕES
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
    "is_mkt_cupons":               {"tipo": "cupom_tipo", "valor": "decimal"},
    "is_producao_setores":         {"status": "str"},
    "is_pedidos_itens":            {"status": "str", "visto": "int"},
    "is_pedidos_pagamentos":       {"visto": "bool"},
    "is_produtos":                 {"valor": "str", "visivel": "bool", "prazo": "str"},
    "is_financeiro_lancamentos":   {"valor": "money", "tipo": "int", "status": "int"},
    "is_entregas_fretes":          {"prazo": "int", "tipo": "int"},
    "is_entregas_balcoes":         {"prazo": "str"},
    "is_pedidos":                  {"frete_tipo": "str", "origem": "int"},
    "is_pedidos_historico":        {"status_id": "int"},
    "is_clientes":                 {"status": "int", "retirada": "int", "revendedor": "int", "pdv": "int"},
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
        "id": "id", "nome": "nome", "sobrenome": "sobrenome",
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
        "id": "id", "url": "url", "titulo": "titulo",
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
        "id": "id", "saldo": "saldo", "tipo": "tipo",
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
        "id": "id", "descricao": "descricao", "valor": "valor",
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
        "id": "id", "cliente_id": "cliente", "usuario_id": "usuario",
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
        "id": "id", "pedido_id": "pedido", "produto_id": "produto",
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
        "id": "id", "cliente_id": "cliente", "pedido_id": "pedido",
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
        "id": "id", "pedido_id": "pedido", "item_id": "item",
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
EXEC_ORDER = [
    # Nível 0: sem dependências
    "is_extras_status",
    "is_entregas_balcoes",
    "is_entregas_fretes",
    "is_financeiro_funcionarios",
    "is_mkt_regras",
    "is_producao_setores",
    "is_produtos",
    "is_produtos_vars_nomes",
    "is_clientes",
    # Nível 1: dependem do nível 0
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

CONFLICT_COLS: Dict[str, str] = {
    "is_clientes_pf": "cliente_id",
    "is_clientes_pj": "cliente_id",
    "is_mkt_cupons_produtos": "cupom_id,produto_id",
}

SELF_REF_TABLES: Dict[str, str] = {
    "is_produtos_categorias": "parent_id",
    "is_pedidos_pagamentos": "original_id",
}


# ============================================================================
# TRANSFORMAÇÕES
# ============================================================================

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
            return to_str(val)
        if ov == "money":
            return to_money(val)
        if ov == "decimal":
            return to_decimal(val)
        if ov == "ts":
            return to_ts(val)
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
        return to_str(val)

    # --- FK columns → UUID5 ---
    if pg_col.endswith("_id") and pg_col != "id":
        ref = FK_MAP.get(pg_col)
        if ref == "_int":
            return to_int(val)
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
    return to_str(val) if isinstance(val, str) else val


def transform_row(
    row: dict, table: str, mapping: Dict[str, str],
) -> Optional[dict]:
    """Transforma uma row MySQL para formato Supabase."""
    new_row: dict = {}
    legacy_id = row.get("id")
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
    d["tipo"] = "PJ" if t in ("juridica", "pj") else "PF"
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
    nome = safe_str(row.get("nome"))
    if not nome:
        return None  # CHECK constraint: nome IS NOT NULL
    return {
        "cliente_id": cid,
        "nome": nome,
        "sobrenome": safe_str(row.get("sobrenome")),
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
    razao = safe_str(row.get("razao_social"))
    if not razao:
        # Fallback para não perder PJ legada com razão social vazia.
        razao = safe_str(row.get("nome")) or safe_str(row.get("fantasia")) or safe_str(row.get("cnpj"))
    if not razao:
        return None  # CHECK constraint: razao_social IS NOT NULL
    return {
        "cliente_id": cid,
        "razao_social": razao,
        "fantasia": safe_str(row.get("fantasia")),
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
        "is_principal": True,
        "created_at": to_ts(row.get("data")),
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
    if not pedido_id:
        return None

    tipo = to_str(row.get("tipo"))
    detalhes_raw = to_str(row.get("detalhes"))

    result = {
        "id": new_id,
        "pedido_id": pedido_id,
        "envio_id": None,
        "metodo_titulo": tipo,
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
                result["metodo_titulo"] = d.get("titulo") or tipo
                result["prazo_dias"] = to_int(d.get("prazo")) or to_int(d.get("prazo_dias"))
                result["valor"] = to_decimal(d.get("custo")) or to_decimal(d.get("valor"))
        except (json.JSONDecodeError, TypeError):
            pass

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
                new_row["parcelas_qtd"] = int(m.group(1)) if m else None
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

    jsonb_cols = {c for c in columns if c.endswith("_json")}

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
    except Exception as batch_err:
        conn.rollback()
        # Log do erro do batch
        err_str = str(batch_err)
        if len(err_str) > 300:
            err_str = err_str[:300] + "..."
        log(f"    Batch error ({table}): {err_str}", "WARN")
        # Fallback: row-by-row
        ok, err = 0, 0
        for row_vals in values:
            try:
                with conn.cursor() as cur:
                    execute_values(cur, sql, [row_vals], page_size=1)
                conn.commit()
                ok += 1
            except Exception as row_e:
                conn.rollback()
                if err < 3:
                    log(f"    Row error: {str(row_e)[:200]}", "WARN")
                err += 1
        return ok, err


def pg_flush(conn, table, batch, conflict_col, ok, err):
    if batch:
        ins, e = pg_upsert(conn, table, batch, conflict_col)
        ok += ins
        err += e
    return [], ok, err


# ============================================================================
# ETL PRINCIPAL
# ============================================================================
def run_precheck(mysql_cursor) -> bool:
    """
    Valida transformações sem escrever no Supabase.
    Retorna True se passar, False em inconsistência crítica.
    """
    log("PRECHECK: validando transformações (sem escrita)...")
    passed = True

    # 1) Clientes PF/PJ + deduplicação por email (mesma regra do ETL)
    mysql_cursor.execute("SELECT * FROM `is_clientes` ORDER BY `id`")
    rows = mysql_cursor.fetchall()
    seen_emails: Dict[str, int] = {}
    kept = pf = pj = dropped_split = 0

    for row in rows:
        base = transform_cliente(row)
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
    log(f"PRECHECK clientes: source={len(rows):,} kept={kept:,} PF={pf:,} PJ={pj:,} split_drop={dropped_split:,} dup_emails={dup_emails:,}")
    if dropped_split > 0:
        passed = False
        log("PRECHECK FAIL: clientes sem classificação PF/PJ após transformação", "ERROR")

    # 2) Cupons: tipo e produtos CSV
    mysql_cursor.execute("SELECT `id`,`tipo`,`valor`,`produtos`,`cliente`,`codigo`,`uso`,`limite`,`inicio`,`fim`,`pedido_min`,`primeira_compra`,`arquivado` FROM `is_mkt_cupons`")
    cup_rows = mysql_cursor.fetchall()
    tipo_invalid = 0
    links = 0
    for row in cup_rows:
        t = transform_row(row, "is_mkt_cupons", COLUMN_MAPPING["is_mkt_cupons"])
        if not t or t.get("tipo") not in ("amount", "percent"):
            tipo_invalid += 1
        links += len(transform_cupom_produtos(row))
    log(f"PRECHECK cupons: source={len(cup_rows):,} tipo_invalid={tipo_invalid:,} links_produtos={links:,}")
    if tipo_invalid > 0:
        passed = False
        log("PRECHECK FAIL: mapeamento de tipo de cupons inválido", "ERROR")

    # 3) Categorias: pai -> chave
    mysql_cursor.execute("SELECT `id`,`chave`,`pai` FROM `is_produtos_categorias`")
    cat_rows = mysql_cursor.fetchall()
    chave_map = {str(r.get('chave')).strip(): r.get("id") for r in cat_rows if r.get("chave")}
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

    # 4) Endereços: tabela + derivados
    mysql_cursor.execute("SELECT COUNT(*) c FROM `is_clientes_enderecos`")
    end_table = mysql_cursor.fetchone()["c"]
    mysql_cursor.execute("SELECT COUNT(*) c FROM `is_clientes` WHERE TRIM(COALESCE(`cep`,''))<>'' OR TRIM(COALESCE(`logradouro`,''))<>''")
    end_derived = mysql_cursor.fetchone()["c"]
    log(f"PRECHECK endereços: tabela={end_table:,} derivados_de_clientes={end_derived:,}")

    if passed:
        log("PRECHECK OK: transformações críticas validadas.")
    return passed


def run_etl():
    global VALID_FK_IDS
    log("=" * 70)
    log("ETL v11 — MySQL legado → Supabase (psycopg2 direto)")
    log("=" * 70)

    if VALIDATE_ONLY:
        # O precheck conecta apenas no MySQL e valida as transformações críticas.
        try:
            mysql = get_mysql()
            cursor = mysql.cursor(dictionary=True)
            VALID_FK_IDS = build_valid_fk_ids(cursor)
            log("Modo ETL_VALIDATE_ONLY=1 — executando PRECHECK e encerrando.")
            ok = run_precheck(cursor)
            cursor.close()
            mysql.close()
            sys.exit(0 if ok else 1)
        except Exception as e:
            log(f"PRECHECK erro: {e}", "ERROR")
            sys.exit(1)

    # Conectar
    try:
        mysql = get_mysql()
        cursor = mysql.cursor(dictionary=True)
        log("Conexão MySQL OK")
        VALID_FK_IDS = build_valid_fk_ids(cursor)
        pg = get_pg()
        log("Conexão PostgreSQL OK")
    except Exception as e:
        log(f"ERRO de conexão: {e}", "ERROR")
        sys.exit(1)

    # Tabelas a processar
    if ONLY_TABLES:
        tables_to_process = [t for t in EXEC_ORDER if t in ONLY_TABLES]
        log(f"ETL_ONLY_TABLES: {tables_to_process}")
    else:
        tables_to_process = list(EXEC_ORDER)

    log(f"Tabelas: {len(tables_to_process)} | Batch: {BATCH_SIZE}")

    stats: Dict[str, Dict[str, int]] = {}
    seen_emails: Dict[str, int] = {}
    pf_list: List[dict] = []
    pj_list: List[dict] = []
    addr_list: List[dict] = []
    cupons_produtos_list: List[dict] = []
    cupom_codigos: Set[str] = set()
    categoria_slug_map: Dict[str, int] = {}

    # Pre-load: slug map para categorias
    try:
        categoria_slug_map = build_categoria_slug_map(cursor)
        log(f"Categorias slug map: {len(categoria_slug_map)} entradas")
    except Exception:
        log("Aviso: não foi possível carregar slug map de categorias", "WARN")

    # Pre-load: cupom codigos para validar FK is_pedidos.cupom
    try:
        cursor.execute("SELECT `codigo` FROM `is_mkt_cupons`")
        for row in cursor.fetchall():
            c = row.get("codigo")
            if c:
                cupom_codigos.add(str(c).strip())
        log(f"Cupons códigos carregados: {len(cupom_codigos)}")
    except Exception:
        log("Aviso: não foi possível carregar códigos de cupons", "WARN")

    # Self-ref data para 2-pass
    self_ref_data: Dict[str, List[dict]] = {t: [] for t in SELF_REF_TABLES}

    # ---- LOOP PRINCIPAL ----
    for table in tables_to_process:
        mapping = COLUMN_MAPPING.get(table)
        if not mapping:
            log(f"  [{table}] Sem mapping — skip", "WARN")
            continue

        # Tabelas derivadas de is_clientes
        if table in DERIVED_TABLES:
            continue  # Processadas junto com is_clientes

        conflict_col = CONFLICT_COLS.get(table, "id")
        mysql_table = MYSQL_TABLE_NAME_MAP.get(table, table)
        self_ref_col = SELF_REF_TABLES.get(table)

        log("")
        log(f"{'─'*50}")
        log(f"TABELA: {table}" + (f" (MySQL: {mysql_table})" if mysql_table != table else ""))
        log(f"{'─'*50}")

        # ---- Tabelas com lógica especial ----
        if table == "is_clientes":
            ok, err = _process_clientes(cursor, pg, seen_emails, pf_list, pj_list, addr_list)
            stats[table] = {"ok": ok, "err": err}
            continue

        if table == "is_clientes_enderecos":
            ok, err = _process_clientes_enderecos(cursor, pg, addr_list)
            stats[table] = {"ok": ok, "err": err}
            continue

        if table == "is_mkt_cupons":
            ok, err = _process_mkt_cupons(cursor, pg, cupons_produtos_list)
            stats[table] = {"ok": ok, "err": err}
            continue

        if table == "is_mkt_cupons_produtos":
            ok, err = _process_mkt_cupons_produtos(pg, cupons_produtos_list)
            stats[table] = {"ok": ok, "err": err}
            continue

        if table == "is_produtos_categorias":
            ok, err = _process_categorias(cursor, pg, categoria_slug_map)
            stats[table] = {"ok": ok, "err": err}
            continue

        if table == "is_pedidos_fretes_entregas":
            ok, err = _process_fretes_entregas(cursor, pg)
            stats[table] = {"ok": ok, "err": err}
            continue

        if table == "is_pedidos":
            ok, err = _process_pedidos(cursor, pg, cupom_codigos)
            stats[table] = {"ok": ok, "err": err}
            continue

        if table == "is_pedidos_pagamentos":
            ok, err = _process_pagamentos(cursor, pg, self_ref_data)
            stats[table] = {"ok": ok, "err": err}
            continue

        # ---- Tabela genérica ----
        ok, err = _process_generic(cursor, pg, table, mysql_table, mapping, conflict_col, self_ref_col, self_ref_data)
        stats[table] = {"ok": ok, "err": err}

    # ---- PF e PJ ----
    for sub_table, sub_list, sub_conflict in [
        ("is_clientes_pf", pf_list, "cliente_id"),
        ("is_clientes_pj", pj_list, "cliente_id"),
    ]:
        if sub_list and (not ONLY_TABLES or sub_table in ONLY_TABLES):
            log("")
            log(f"{'─'*50}")
            log(f"TABELA: {sub_table} ({len(sub_list)} registros derivados)")
            log(f"{'─'*50}")
            valid = [p for p in sub_list if p.get("cliente_id")]
            sub_ok, sub_err = 0, 0
            for i in range(0, len(valid), BATCH_SIZE):
                chunk = valid[i:i + BATCH_SIZE]
                ins, e = pg_upsert(pg, sub_table, chunk, sub_conflict)
                sub_ok += ins
                sub_err += e
            log(f"  → OK={sub_ok:,}  ERR={sub_err:,}")
            stats[sub_table] = {"ok": sub_ok, "err": sub_err}

    # ---- Pass 2: self-references ----
    from psycopg2.extras import execute_batch as pg_execute_batch

    for table, col in SELF_REF_TABLES.items():
        updates = self_ref_data.get(table, [])
        if not updates:
            continue
        if ONLY_TABLES and table not in ONLY_TABLES:
            continue
        log("")
        log(f"  [2-pass] {table}.{col}: {len(updates)} registros")
        sql = f'UPDATE public."{table}" SET "{col}" = %s WHERE "id" = %s'
        vals = [(u[col], u["id"]) for u in updates]
        try:
            with pg.cursor() as cur:
                pg_execute_batch(cur, sql, vals, page_size=500)
            pg.commit()
            log(f"  → 2-pass OK={len(updates):,}")
        except Exception:
            pg.rollback()
            ok2, err2 = 0, 0
            for u in updates:
                try:
                    with pg.cursor() as cur:
                        cur.execute(sql, (u[col], u["id"]))
                    pg.commit()
                    ok2 += 1
                except Exception as e:
                    pg.rollback()
                    if err2 < 3:
                        log(f"    2-pass err: {str(e)[:200]}", "WARN")
                    err2 += 1
            log(f"  → 2-pass OK={ok2:,}  ERR={err2:,}")

    # ---- RESUMO FINAL ----
    log("")
    log("=" * 70)
    log("RESUMO FINAL")
    log("=" * 70)

    total_ok = sum(s["ok"] for s in stats.values())
    total_err = sum(s["err"] for s in stats.values())

    for t in EXEC_ORDER:
        if t in stats:
            s = stats[t]
            icon = "✓" if s["err"] == 0 else "✗"
            log(f"  {icon} {t:<40s}  OK={s['ok']:>7,}  ERR={s['err']:>5,}")

    log("")
    log(f"  TOTAL: {total_ok:,} OK | {total_err:,} ERR")

    cursor.close()
    mysql.close()
    pg.close()

    if total_err > 0:
        log(f"  ⚠ {total_err} erros encontrados — verifique os logs acima", "WARN")

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
                if pf:
                    pf_list.append(pf)
                pj = transform_cliente_pj(row)
                if pj:
                    pj_list.append(pj)

                # Endereço padrão
                addr = transform_cliente_endereco_from_cliente(row)
                if addr:
                    addr_list.append(addr)
            except Exception as e:
                if err < 5:
                    log(f"    Err cliente {row.get('id')}: {e}", "WARN")
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
            except Exception:
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
    """Processa is_produtos_categorias com resolução de parent_id via chave."""
    cursor.execute("SELECT * FROM `is_produtos_categorias`")

    ok, err = 0, 0
    batch: List[dict] = []

    for row in cursor.fetchall():
        try:
            t_row = transform_categoria(row, slug_map)
            if t_row and t_row.get("id"):
                # Self-ref: parent_id já resolvido em transform_categoria
                batch.append(t_row)
        except Exception as e:
            if err < 3:
                log(f"    Err cat {row.get('id')}: {e}", "WARN")
            err += 1

    # Insert sem parent_id primeiro, depois update
    for row in batch:
        row["_parent"] = row.pop("parent_id", None)
        row["parent_id"] = None

    batch, ok, err = pg_flush(pg, "is_produtos_categorias", batch, "id", ok, err)

    # Pass 2: set parent_id
    from psycopg2.extras import execute_batch as pg_exec_batch
    log(f"  → OK={ok:,}  ERR={err:,}")

    # Reload and update parents
    cursor.execute("SELECT `id`, `pai` FROM `is_produtos_categorias`")
    parent_updates = []
    for row in cursor.fetchall():
        pai_chave = to_str(row.get("pai"))
        if pai_chave and pai_chave != "0":
            parent_legacy_id = slug_map.get(pai_chave)
            if parent_legacy_id:
                my_uuid = uuid5_for("is_produtos_categorias", row["id"])
                parent_uuid = uuid5_for("is_produtos_categorias", parent_legacy_id)
                parent_updates.append((parent_uuid, my_uuid))

    if parent_updates:
        sql = 'UPDATE public."is_produtos_categorias" SET "parent_id" = %s WHERE "id" = %s'
        try:
            with pg.cursor() as cur:
                pg_exec_batch(cur, sql, parent_updates, page_size=500)
            pg.commit()
            log(f"  → parent_id atualizado: {len(parent_updates)} registros")
        except Exception as e:
            pg.rollback()
            log(f"  → parent_id ERRO: {str(e)[:200]}", "WARN")

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
            except Exception:
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
                    batch.append(t_row)
            except Exception as e:
                if err < 5:
                    log(f"    Err pedido {row.get('id')}: {e}", "WARN")
                err += 1
        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, "is_pedidos", batch, "id", ok, err)
        if processed >= next_progress:
            log(f"  … progress is_pedidos: lidos={processed:,} OK={ok:,} ERR={err:,}")
            next_progress += 20_000

    batch, ok, err = pg_flush(pg, "is_pedidos", batch, "id", ok, err)
    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


def _process_pagamentos(cursor, pg, self_ref_data):
    """Processa is_pedidos_pagamentos com parcelas split e self-ref original_id."""
    mapping = COLUMN_MAPPING["is_pedidos_pagamentos"]
    cols = list(set(mapping.values()))
    if "id" not in cols:
        cols.append("id")

    cursor.execute(f"SELECT {','.join(f'`{c}`' for c in cols)} FROM `is_pedidos_pagamentos`")

    ok, err = 0, 0
    batch: List[dict] = []
    processed = 0
    next_progress = 20_000

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        processed += len(rows)
        for row in rows:
            try:
                t_row = transform_pagamento(row)
                if t_row and t_row.get("id"):
                    # Self-ref 2-pass para original_id
                    if t_row.get("original_id"):
                        self_ref_data["is_pedidos_pagamentos"].append(
                            {"id": t_row["id"], "original_id": t_row["original_id"]}
                        )
                        t_row["original_id"] = None
                    batch.append(t_row)
            except Exception as e:
                if err < 5:
                    log(f"    Err pag {row.get('id')}: {e}", "WARN")
                err += 1
        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, "is_pedidos_pagamentos", batch, "id", ok, err)
        if processed >= next_progress:
            log(f"  … progress is_pedidos_pagamentos: lidos={processed:,} OK={ok:,} ERR={err:,}")
            next_progress += 20_000

    batch, ok, err = pg_flush(pg, "is_pedidos_pagamentos", batch, "id", ok, err)
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

    ok, err = 0, 0
    batch: List[dict] = []
    has_id = table not in TABLES_WITHOUT_ID
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
                    if self_ref_col and t_row.get(self_ref_col):
                        self_ref_data[table].append(
                            {"id": t_row["id"], self_ref_col: t_row[self_ref_col]}
                        )
                        t_row[self_ref_col] = None
                    batch.append(t_row)
            except Exception:
                err += 1
        if len(batch) >= BATCH_SIZE:
            batch, ok, err = pg_flush(pg, table, batch, conflict_col, ok, err)
        if processed >= next_progress:
            log(f"  … progress {table}: lidos={processed:,} OK={ok:,} ERR={err:,}")
            next_progress += 20_000

    batch, ok, err = pg_flush(pg, table, batch, conflict_col, ok, err)
    log(f"  → OK={ok:,}  ERR={err:,}")
    return ok, err


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    run_etl()
