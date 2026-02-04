"""
Script para extrair amostras de dados do dump MySQL para uso como mocks no Streamlit.
Gera um arquivo JSON com samples de clientes, pedidos e lançamentos financeiros.
"""
import re
import json
from pathlib import Path

DUMP_PATH = Path(r"c:\Users\ozile\OneDrive\Área de Trabalho\supabase-migration_old_dumps\nblgrafica_app-2026-01-22.sql")
OUTPUT_PATH = Path(r"c:\Users\ozile\OneDrive\Área de Trabalho\supabase-migration\data\mock_samples.json")

def extract_inserts(table_name: str, limit: int = 20) -> list:
    """Extrai os primeiros N registros de uma tabela do dump."""
    pattern = rf"INSERT INTO `{table_name}` VALUES"
    records = []
    
    with open(DUMP_PATH, 'r', encoding='utf-8', errors='ignore') as f:
        capture = False
        buffer = ""
        
        for line in f:
            if pattern in line:
                capture = True
                buffer = line
            elif capture:
                buffer += line
                if line.strip().endswith(';'):
                    capture = False
                    # Parse the VALUES
                    values_match = re.search(r'VALUES\s*(.+);', buffer, re.DOTALL)
                    if values_match:
                        values_str = values_match.group(1)
                        # Tentar extrair tuplas individuais
                        tuples = re.findall(r'\(([^)]+)\)', values_str)
                        for t in tuples[:limit]:
                            records.append(t)
                    break  # Pegar apenas o primeiro bloco de INSERT
                    
    return records[:limit]

def parse_client_record(raw: str) -> dict:
    """Converte registro bruto de cliente em dict."""
    # Formato aproximado: id, tipo, razao_social, nome, telefone, email, etc.
    parts = raw.split(',')
    if len(parts) >= 5:
        return {
            "id": parts[0].strip().strip("'"),
            "tipo": parts[1].strip().strip("'"),
            "nome": parts[3].strip().strip("'") or parts[2].strip().strip("'"),
        }
    return None

def main():
    print("Extraindo amostras do dump MySQL...")
    
    # Criar diretório de saída
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    samples = {
        "clientes": [],
        "pedidos": [],
        "financeiro": []
    }
    
    # Extrair clientes
    print("  -> is_clientes...")
    raw_clients = extract_inserts("is_clientes", 30)
    for r in raw_clients:
        parsed = parse_client_record(r)
        if parsed:
            samples["clientes"].append(parsed)
    
    # Extrair pedidos
    print("  -> is_pedidos...")
    # Simplificado: apenas contar quantos existem
    raw_pedidos = extract_inserts("is_pedidos", 50)
    samples["pedidos_count"] = len(raw_pedidos)
    
    # Extrair financeiro
    print("  -> is_financeiro_lancamentos...")
    raw_fin = extract_inserts("is_financeiro_lancamentos", 50)
    samples["financeiro_count"] = len(raw_fin)
    
    # Salvar JSON
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(samples, f, ensure_ascii=False, indent=2)
    
    print(f"Amostras salvas em: {OUTPUT_PATH}")
    print(f"  Clientes: {len(samples['clientes'])}")
    print(f"  Pedidos: {samples.get('pedidos_count', 0)}")
    print(f"  Financeiro: {samples.get('financeiro_count', 0)}")

if __name__ == "__main__":
    main()
