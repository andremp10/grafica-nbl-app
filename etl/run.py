#!/usr/bin/env python3
"""
ETL v8 - Migração Definitiva MySQL → Supabase
==============================================

COMO USAR:
1. Configure o .env com as credenciais
2. Execute: python run.py

Este script:
- Aplica correções de constraints automaticamente
- Carrega tabelas na ordem topológica correta
- Trata erros individualmente por registro
- Mostra progresso e resumo final
"""

import os
import uuid
import json
import datetime as dt
from typing import Optional, Dict, Any, Set, List, Tuple
from dotenv import load_dotenv
import sys

# Encoding fix para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
UUID_NS = uuid.UUID("2a6b2c31-0f2a-4dfd-8cde-7b4b9b3f1c5a")
BATCH_SIZE = 200  # Tamanho do batch para insert

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
    if val is None: return None
    if isinstance(val, bool): return val
    if isinstance(val, (int, float)): return val != 0
    s = str(val).lower().strip()
    if s in ("", "none", "null"): return None
    return s in ("1", "true", "t", "yes", "s", "sim", "2")

def to_int(val: Any) -> Optional[int]:
    if val is None: return None
    if isinstance(val, bool): return 1 if val else 0
    if isinstance(val, int): return val
    if isinstance(val, float): return int(val)
    s = str(val).lower().strip()
    if s in ("", "none", "null"): return None
    if s == "true": return 1
    if s == "false": return 0
    try:
        return int(float(s))
    except:
        return None

def to_decimal(val: Any) -> Optional[float]:
    if val is None: return None
    if isinstance(val, (int, float)): return float(val)
    s = str(val).strip()
    if s in ("", "none", "null", "None"): return None
    try:
        return float(s.replace(",", "."))
    except:
        return None

def to_money(val: Any) -> Optional[float]:
    v = to_decimal(val)
    return max(0.0, v) if v is not None else 0.0

def to_ts(val: Any) -> Optional[str]:
    if not val: return None
    if isinstance(val, (dt.datetime, dt.date)):
        return val.isoformat()
    s = str(val).strip()
    if s.startswith("0000-00-00") or s in ("None", "null", ""): return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "")).isoformat()
    except:
        return None

def to_str(val: Any) -> Optional[str]:
    if val is None: return None
    s = str(val).strip()
    return s if s and s not in ("None", "null") else None

def clean_string(val: Any) -> Optional[str]:
    """Limpa strings: corrige encoding (mojibake) e remove caracteres especiais."""
    s = to_str(val)
    if not s: return None
    
    # 1. Tentar corrigir Mojibake (UTF-8 lido como Latin-1)
    # Ex: MANUTENÃ§Ã£O -> MANUTENÇÃO
    try:
        s = s.encode('latin1').decode('utf-8')
    except:
        pass  # Se falhar, mantém original
        
    # 2. Remover caracteres indesejados (manter letras, números, espaços, pontos, traços, barras)
    # Regex allow list: A-Z a-z 0-9 space . - / and Portuguese accents
    # Mas como regex simples, vamos focar em remover o óbvio lixo
    # User pediu remove %, @, _
    import re
    s = re.sub(r'[_@%]', ' ', s)
    
    return s.strip()

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
        charset='utf8mb4',
        use_unicode=True,
        use_pure=True,
    )

def get_supabase():
    from supabase import create_client
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# ============================================================================
# MAPEAMENTOS
# ============================================================================

# FK: coluna → tabela referenciada
FK_MAP = {
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
    "transportadora_id": "is_financeiro_transportadoras",
    "status_id": "is_extras_status",
    "categoria_id": "is_financeiro_categorias",  # Default, override abaixo
    "parent_id": None,  # Self-reference, tratado caso a caso
}

# Colunas BOOLEAN no Supabase
BOOL_COLS = {
    "visivel", "arquivado", "primeira_compra", "arte", "estoque_controlar",
    "vars_obrig", "individual", "embalagem", "devolucao_completa", "pago",
    "oculto", "neutro", "is_principal", "visto"
}

# Colunas TIMESTAMP
TS_COLS = {
    "created_at", "updated_at", "data", "ultimo_acesso", "nascimento",
    "oferta_expira", "admissao", "demissao", "aprovacao", "previsao_producao",
    "previsao_entrega", "arte_data", "data_modificado", "abertura_data",
    "fechamento_data", "inicio", "fim", "vencimento", "data_pagto", 
    "data_emissao", "emissao", "saida"
}

# Colunas MONEY (valores não negativos)
MONEY_COLS = {
    "total", "acrescimo", "desconto", "desconto_uso", "sinal", 
    "frete_valor", "subtotal", "taxa", "custo", "salario", "vale",
    "valor_arte", "preco_metro", "preco_min", "preco_atual", "preco_anterior",
    "preco_unit", "preco_base", "encaixe", "apartir", "desconto_valor",
    "saldo_inicial", "abertura_valor", "fechamento_valor"
}

# ============================================================================
# ORDEM TOPOLÓGICA - Baseada na análise de FKs do schema
# ============================================================================
# REGRA: Uma tabela só pode ser carregada DEPOIS de todas as suas dependências

EXEC_ORDER = [
    # === NÍVEL 0: Tabelas sem dependências (base) ===
    "is_extras_status",           # PK integer, sem FK
    "is_bancos",                  # Sem FK
    "is_config",                  # Sem FK
    "is_apps_whatsapp_msgs",      # Sem FK
    "is_mensagens",               # Sem FK
    "is_paginas",                 # Sem FK
    "is_producao_setores",        # Sem FK
    "is_produtos_servicos",       # Sem FK
    "is_produtos_vars_nomes",     # Sem FK
    "is_financeiro_carteiras",    # Sem FK
    "is_financeiro_categorias",   # Sem FK
    "is_financeiro_centros_custo",# Sem FK
    "is_financeiro_pdvs",         # Sem FK
    "is_entregas_balcoes",        # Sem FK
    "is_entregas_fretes",         # Sem FK
    "is_visitas",                 # Sem FK
    
    # === NÍVEL 1: Dependem apenas do nível 0 ===
    "is_arquivos",                # Sem FK obrigatória
    "is_usuarios",                # FK opcional: balcao_id, pdv_id
    "is_produtos_categorias",     # FK opcional: parent_id (self-ref)
    "is_financeiro_funcionarios", # Sem FK
    "is_financeiro_fornecedores", # Sem FK
    "is_financeiro_transportadoras", # Sem FK
    "is_mkt_banners",             # Sem FK
    "is_mkt_regras",              # Sem FK
    "is_entregas_fretes_locais",  # FK: frete_id
    
    # === NÍVEL 2: Dependem de nível 0+1 ===
    "is_clientes",                # Sem FK obrigatória
    "is_produtos",                # Sem FK obrigatória
    "is_usuarios_tentativas",     # Sem FK
    
    # === NÍVEL 3: Dependem de clientes/produtos/usuarios ===
    "is_clientes_enderecos",      # FK: cliente_id
    "is_clientes_pf",             # FK: cliente_id
    "is_clientes_pj",             # FK: cliente_id
    "is_usuarios_acessos",        # FK: usuario_id
    "is_usuarios_historico",      # FK: usuario_id, cliente_id (nullable)
    "is_mkt_cupons",              # FK: cliente_id
    "is_produtos_categorias_extras", # FK: produto_id
    "is_produtos_vars",           # FK: produto_id, grupo_id
    "is_produtos_dem",            # FK: produto_id
    "is_produtos_dem_info",       # FK: produto_id
    "is_produtos_fixo",           # FK: produto_id
    "is_produtos_fixo_regras",    # FK: produto_id
    "is_produtos_mt",             # FK: produto_id
    "is_produtos_mt_regras",      # FK: produto_id
    "is_produtos_offset",         # FK: produto_id
    "is_produtos_qtd",            # FK: produto_id
    "is_produtos_avaliacoes",     # FK: produto_id, cliente_id
    "is_financeiro_caixas",       # FK: operador_id(usuario), pdv_id
    
    # === NÍVEL 4: Dependem de nível 3 ===
    "is_financeiro_caixas_movimentacoes", # FK: caixa_id
    "is_financeiro_repeticoes",   # FK: multiple
    "is_financeiro_lancamentos",  # FK: multiple
    "is_financeiro_repeticoes_criadas", # FK: repeticao_id
    "is_financeiro_notasfiscais", # FK: fornecedor_id, transportadora_id
    "is_financeiro_conciliacoes", # FK: arquivo_id
    
    # === NÍVEL 5: Pedidos (dependem de clientes, usuarios, caixas) ===
    "is_pedidos",                 # FK: cliente_id, usuario_id, caixa_id
    "is_pedidos_orcamentos",      # FK: cliente_id, usuario_id
    
    # === NÍVEL 6: Itens de pedido ===
    "is_pedidos_itens",           # FK: pedido_id, produto_id
    "is_pedidos_pagamentos",      # FK: cliente_id, pedido_id
    "is_pedidos_historico",       # FK: pedido_id, usuario_id, status_id
    "is_pedidos_fretes_detalhes", # FK: pedido_id
    "is_pedidos_fretes_envios",   # FK: pedido_id
    "is_pedidos_orcamentos_itens",# FK: orcamento_id, produto_id
    "is_pedidos_orcamentos_pagamentos", # FK: cliente_id, orcamento_id
    "is_pedidos_orcamentos_fretes_detalhes", # FK: orcamento_id
    "is_pedidos_orcamentos_fretes_envios", # FK: orcamento_id
    
    # === NÍVEL 7: Sub-itens ===
    "is_pedidos_itens_briefings", # FK: item_id
    "is_pedidos_itens_brief_conversa", # FK: item_id
    "is_pedidos_itens_brief_alteracoes", # FK: item_id
    "is_pedidos_itens_reprovados",# FK: item_id, usuario_id
    "is_pedidos_pag_reprovados",  # FK: comprovante_id, usuario_id
    "is_clientes_extratos",       # FK: cliente_id, pedido_id, pagamento_id
    
    # === NÍVEL 8: Logs (baixa prioridade) ===
    "is_config_logs_curl",
    "is_visitas_online",
]

# ============================================================================
# TRANSFORMAÇÕES
# ============================================================================

def transform_row(row: dict, table: str, mapping: Dict[str, str]) -> Optional[dict]:
    """Transforma uma row do MySQL para o formato do Supabase."""
# ============================================================================
# REGRAS DE CATEGORIZAÇÃO (PATCH MIGRATION 005)
# ============================================================================
CAT_RULES = {
    "AGUA": "d7b04caa-404f-5c37-a9d7-fde38ed1f758",
    "PAPEL": "217b5ba5-6e58-5614-821d-595c3db12f63",
    "PRO_SHEILA": "d9d062b7-9175-52eb-9e79-a8833f4155b2",
    "PRO_HORACIO": "c4f57a2e-90c4-585b-875a-e7740cb60c2d",
    "PRO_MAKLEN": "ca0aa9ff-9d75-5b91-be7e-15d0afda61b0",
    "PRO_GERAL": "fbafcd44-3c3c-5279-8645-a211a69e7335",
}

def apply_categorization_rules(row: dict) -> dict:
    """Aplica regras de categorização para preencher categorias nulas."""
    if row.get("categoria_id"):
        return row
    
    desc = str(row.get("descricao", "")).upper()
    
    if "CAGECE" in desc or "COELCE" in desc:
        row["categoria_id"] = CAT_RULES["AGUA"]
    elif any(x in desc for x in ["PAPEL", "COUCHE", "PAPEIS"]):
        row["categoria_id"] = CAT_RULES["PAPEL"]
    elif "PRO-LABORE SHEILA" in desc:
        row["categoria_id"] = CAT_RULES["PRO_SHEILA"]
    elif "PRO-LABORE HORACIO" in desc:
        row["categoria_id"] = CAT_RULES["PRO_HORACIO"]
    elif "PRO-LABORE MAKLEN" in desc:
        row["categoria_id"] = CAT_RULES["PRO_MAKLEN"]
    elif "PRO-LABORE" in desc:
        row["categoria_id"] = CAT_RULES["PRO_GERAL"]
        
    return row

def transform_row(row: dict, table: str, mapping: dict) -> Optional[dict]:
    """Transforma uma row do MySQL para o formato do Supabase."""
    new_row = {}
    legacy_id = row.get("id")
    
    # ID especial para is_extras_status (INT, não UUID)
    if table == "is_extras_status":
        new_row["id"] = to_int(legacy_id)
        if new_row["id"] is None:
            return None
    elif legacy_id:
        new_row["id"] = uuid5_for(table, legacy_id)
    else:
        return None  # Sem ID válido
    
    for pg_col, mysql_col in mapping.items():
        if pg_col == "id":
            continue
        
        val = row.get(mysql_col)
        
        # FK columns → UUID
        if pg_col.endswith("_id"):
            ref = FK_MAP.get(pg_col)
            if not ref:
                if pg_col == "categoria_id":
                    ref = "is_financeiro_categorias" if "financeiro" in table else "is_produtos_categorias"
                elif pg_col == "parent_id":
                    ref = table  # Self-reference
                else:
                    ref = "is_" + pg_col[:-3] + "s"
            
            # status_id é INT, não UUID
            if pg_col == "status_id":
                new_row[pg_col] = to_int(val)
            else:
                new_row[pg_col] = uuid5_for(ref, val) if val else None
        
        # String columns cleaning
        elif any(x in pg_col for x in ("descricao", "nome", "titulo", "sobrenome", "razao_social", "fantasia", "obs", "observacoes")):
            new_row[pg_col] = clean_string(val)
        
        # Boolean
        elif pg_col in BOOL_COLS:
            new_row[pg_col] = to_bool(val)
        
        # Timestamp
        elif pg_col in TS_COLS:
            new_row[pg_col] = to_ts(val)
        
        # Money (non-negative numeric)
        elif pg_col in MONEY_COLS:
            new_row[pg_col] = to_money(val)
        
        # Decimal patterns
        elif any(x in pg_col for x in ("valor", "preco", "saldo", "custo", "qtde")):
            new_row[pg_col] = to_decimal(val)
        
        # Int patterns
        elif pg_col in ("status", "acesso", "tipo", "origem", "num", "ordem", 
                       "estoque", "l", "a", "c", "revendedor", "pdv", "target",
                       "cobranca", "cobranca_val", "visto", "revendedor", 
                       "categoria", "vendidos", "estoque_qtde", "uso", "limite",
                       "arte_status", "parcelas_qtd", "time"):
            new_row[pg_col] = to_int(val)
        
        # String true/false → int
        elif isinstance(val, str) and val.lower() in ("true", "false"):
            new_row[pg_col] = to_int(val)
        
        # Default: manter valor
        else:
            new_row[pg_col] = to_str(val) if isinstance(val, str) else val
    
    # --- AUTO-CATEGORIZATION PATCH ---
    if table == "is_financeiro_lancamentos":
        new_row = apply_categorization_rules(new_row)
    
    # Limpar valores vazios problemáticos
    if "cupom" in new_row and not new_row.get("cupom"):
        new_row["cupom"] = None
    if "sku" in new_row and new_row.get("sku") == "":
        new_row["sku"] = None
    
    return new_row

def transform_cliente(row: dict, mapping: dict) -> dict:
    d = transform_row(row, "is_clientes", mapping)
    if d:
        t = str(row.get("tipo", "")).lower()
        d["tipo"] = "PJ" if t in ("juridica", "pj") else "PF"
        if d.get("email_log"):
            d["email_log"] = d["email_log"].lower().strip()
    return d

def transform_cliente_pf(row: dict) -> Optional[dict]:
    t = str(row.get("tipo", "")).lower()
    if t not in ("fisica", "pf"): return None
    cliente_id = uuid5_for("is_clientes", row.get("id"))
    if not cliente_id: return None
    return {
        "cliente_id": cliente_id,
        "nome": clean_string(row.get("nome")),
        "sobrenome": clean_string(row.get("sobrenome")),
        "nascimento": to_ts(row.get("nascimento")),
        "cpf": to_str(row.get("cpf")),
        "sexo": to_str(row.get("sexo"))
    }

def transform_cliente_pj(row: dict) -> Optional[dict]:
    t = str(row.get("tipo", "")).lower()
    if t not in ("juridica", "pj"): return None
    cliente_id = uuid5_for("is_clientes", row.get("id"))
    if not cliente_id: return None
    return {
        "cliente_id": cliente_id,
        "razao_social": clean_string(row.get("razao_social")),
        "fantasia": clean_string(row.get("fantasia")),
        "ie": to_str(row.get("ie")),
        "cnpj": to_str(row.get("cnpj"))
    }

# ============================================================================
# ETL PRINCIPAL
# ============================================================================

def load_mappings() -> dict:
    """Carrega o column_mapping.json"""
    mapping_path = os.path.join(os.path.dirname(__file__), "column_mapping.json")
    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f)

def run_etl():
    """Executa o ETL completo."""
    log("=" * 60)
    log("ETL v8 - Migração MySQL → Supabase")
    log("=" * 60)
    
    # Carregar mapeamentos
    MAPPINGS = load_mappings()
    log(f"Mapeamentos carregados: {len(MAPPINGS)} tabelas")
    
    # Conectar
    try:
        mysql = get_mysql()
        cursor = mysql.cursor(dictionary=True)
        supa = get_supabase()
        log("Conexões estabelecidas (MySQL + Supabase)")
    except Exception as e:
        log(f"ERRO de conexão: {e}", "ERROR")
        return
    
    # Determinar ordem de execução
    tables_to_process = [t for t in EXEC_ORDER if t in MAPPINGS]
    tables_to_process += [t for t in MAPPINGS.keys() if t not in tables_to_process]
    
    log(f"Tabelas a processar: {len(tables_to_process)}")
    
    # Estatísticas
    stats: Dict[str, Dict[str, int]] = {}
    seen_emails: Set[str] = set()
    pf_list, pj_list = [], []
    
    # Processar cada tabela
    for table in tables_to_process:
        mapping = MAPPINGS.get(table)
        if not mapping:
            continue
        
        log(f"\n>>> [{table}]")
        
        # Determinar colunas a buscar
        cols = list(set(mapping.values()))
        if "id" not in cols:
            cols.append("id")
        
        # Colunas extras para clientes (PF/PJ)
        if table == "is_clientes":
            for c in ["nome", "sobrenome", "nascimento", "cpf", "sexo", 
                      "razao_social", "fantasia", "ie", "cnpj"]:
                if c not in cols:
                    cols.append(c)
        
        # Query MySQL
        try:
            query = f"SELECT {','.join([f'`{c}`' for c in cols])} FROM `{table}`"
            cursor.execute(query)
        except Exception as e:
            log(f"    SKIP (tabela não existe no MySQL): {e}", "WARN")
            continue
        
        ok, err = 0, 0
        batch = []
        
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            
            for row in rows:
                try:
                    # Transformar
                    if table == "is_clientes":
                        t_row = transform_cliente(row, mapping)
                        if t_row:
                            email = t_row.get("email_log")
                            if email in seen_emails:
                                continue
                            if email:
                                seen_emails.add(email)
                            
                            pf = transform_cliente_pf(row)
                            if pf: pf_list.append(pf)
                            pj = transform_cliente_pj(row)
                            if pj: pj_list.append(pj)
                    else:
                        t_row = transform_row(row, table, mapping)
                    
                    if t_row and t_row.get("id") is not None:
                        batch.append(t_row)
                except Exception as e:
                    err += 1
            
            # Inserir batch
            if len(batch) >= BATCH_SIZE:
                inserted, errors = insert_batch(supa, table, batch)
                ok += inserted
                err += errors
                batch = []
        
        # Inserir batch restante
        if batch:
            inserted, errors = insert_batch(supa, table, batch)
            ok += inserted
            err += errors
        
        log(f"    OK={ok}, ERR={err}")
        stats[table] = {"ok": ok, "err": err}
    
    # Inserir PF/PJ
    if pf_list:
        log(f"\n>>> [is_clientes_pf] ({len(pf_list)} registros)")
        valid = [p for p in pf_list if p.get("cliente_id")]
        inserted, errors = insert_batch(supa, "is_clientes_pf", valid, "cliente_id")
        log(f"    OK={inserted}, ERR={errors}")
    
    if pj_list:
        log(f">>> [is_clientes_pj] ({len(pj_list)} registros)")
        valid = [p for p in pj_list if p.get("cliente_id")]
        inserted, errors = insert_batch(supa, "is_clientes_pj", valid, "cliente_id")
        log(f"    OK={inserted}, ERR={errors}")
    
    # Resumo final
    log("\n" + "=" * 60)
    log("RESUMO FINAL")
    log("=" * 60)
    
    total_ok = sum(s["ok"] for s in stats.values())
    total_err = sum(s["err"] for s in stats.values())
    
    critical_tables = ["is_usuarios", "is_clientes", "is_produtos", 
                       "is_pedidos", "is_pedidos_itens"]
    
    log("\nTabelas Críticas:")
    for t in critical_tables:
        if t in stats:
            s = stats[t]
            status = "✓" if s["err"] == 0 else "!"
            log(f"  {status} {t}: {s['ok']} OK, {s['err']} ERR")
    
    log(f"\nTOTAL: {total_ok:,} registros OK, {total_err:,} erros")
    
    # Fechar conexões
    cursor.close()
    mysql.close()
    
    log("\nETL COMPLETO!")

def insert_batch(supa, table: str, batch: list, conflict_col: str = "id") -> Tuple[int, int]:
    """Insere um batch com fallback para inserção individual."""
    if not batch:
        return 0, 0
    
    try:
        supa.table(table).upsert(batch, on_conflict=conflict_col).execute()
        return len(batch), 0
    except Exception as e:
        # Fallback: inserir individualmente
        ok, err = 0, 0
        for item in batch:
            try:
                supa.table(table).upsert([item], on_conflict=conflict_col).execute()
                ok += 1
            except:
                err += 1
        return ok, err

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    run_etl()
