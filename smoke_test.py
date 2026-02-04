from dotenv import load_dotenv

from data.repositories import fetch_financeiro, fetch_kpis_financeiro, fetch_pedidos
from data.supabase_client import get_supabase_client

load_dotenv(".env")


def smoke_test() -> None:
    print("Starting Smoke Test...")

    client = get_supabase_client()
    if client:
        print("Supabase client initialized")
    else:
        print("Failed to initialize Supabase client")
        return

    print("\n--- Testing Pedidos Fetch ---")
    try:
        df_pedidos = fetch_pedidos(page_size=5)
        print(f"Fetched {len(df_pedidos)} pedidos.")
        if not df_pedidos.empty:
            print("Columns:", df_pedidos.columns.tolist())
            sample_cols = [c for c in ["pedido_id", "status_pedido", "data_prazo_validada"] if c in df_pedidos.columns]
            print("Sample:\n", df_pedidos[sample_cols].head(2))
    except Exception as exc:
        print(f"Error fetching pedidos: {exc}")

    print("\n--- Testing Financeiro Fetch ---")
    try:
        df_financeiro = fetch_financeiro(page_size=5)
        print(f"Fetched {len(df_financeiro)} lancamentos financeiros.")
        if not df_financeiro.empty:
            sample_cols = [c for c in ["lancamento_id", "descricao", "status_texto", "valor"] if c in df_financeiro.columns]
            print("Sample:\n", df_financeiro[sample_cols].head(2))
    except Exception as exc:
        print(f"Error fetching financeiro: {exc}")

    print("\n--- Testing Financeiro KPIs (RPC) ---")
    try:
        kpis = fetch_kpis_financeiro(start_date="2000-01-01", end_date="2030-12-31")
        print("KPIs:", kpis)
    except Exception as exc:
        print(f"Error fetching KPIs: {exc}")


if __name__ == "__main__":
    smoke_test()
