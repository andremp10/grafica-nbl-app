from dotenv import load_dotenv

from data.supabase_repo import fetch_financeiro, fetch_kpis_financeiro, fetch_pedidos

load_dotenv(".env")


def main() -> None:
    print("--- VERIFICANDO INTEGRACAO SUPABASE ---")

    print("\n1. Testando Pedidos...")
    try:
        df_pedidos = fetch_pedidos(page_size=5)
        if df_pedidos.empty:
            df_pedidos = fetch_pedidos(
                start_date="2000-01-01",
                end_date="2030-12-31",
                page_size=5,
            )
        if not df_pedidos.empty:
            print(f"Sucesso: retornou {len(df_pedidos)} pedidos.")
            print("Colunas:", df_pedidos.columns.tolist())
            print("Exemplo de status:", df_pedidos.iloc[0].get("status_pedido"))
        else:
            print("Aviso: retornou vazio (conexao ok, sem dados para o filtro).")
    except Exception as exc:
        print(f"Erro em Pedidos: {exc}")

    print("\n2. Testando Financeiro...")
    try:
        df_financeiro = fetch_financeiro(page_size=5)
        if not df_financeiro.empty:
            print(f"Sucesso: retornou {len(df_financeiro)} lancamentos.")
            print("Colunas:", df_financeiro.columns.tolist())
            print("Exemplo de status:", df_financeiro.iloc[0].get("status_texto"))
        else:
            print("Aviso: retornou vazio (conexao ok, sem dados para o filtro).")
    except Exception as exc:
        print(f"Erro em Financeiro: {exc}")

    print("\n3. Testando KPIs Financeiros (RPC)...")
    try:
        kpis = fetch_kpis_financeiro(start_date="2000-01-01", end_date="2030-12-31")
        print("KPIs:", kpis)
        if int(kpis.get("count", 0)) > 0:
            print("Sucesso: contagem via RPC retornou registros.")
        else:
            print("Aviso: count igual a zero.")
    except Exception as exc:
        print(f"Erro em KPIs: {exc}")


if __name__ == "__main__":
    main()
