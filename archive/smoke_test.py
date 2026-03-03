from dotenv import load_dotenv
from data.supabase_client import get_supabase_client
from data.supabase_repo import (
    fetch_view_data,
    fetch_finance_kpis_rpc
)

load_dotenv(".env")

def smoke_test() -> None:
    print("Starting Generic Connector Smoke Test...")

    client = get_supabase_client()
    if client:
        print("Supabase client initialized")
    else:
        print("Failed to initialize Supabase client")
        return

    print("\n--- Testing Generic View Fetch (Pedidos) ---")
    try:
        df_pedidos = fetch_view_data(
            view_name="vw_dashboard_pedidos",
            limit=5,
            order_by="data_criacao",
            ascending=False
        )
        print(f"Fetched {len(df_pedidos)} pedidos.")
        if not df_pedidos.empty:
            print("Columns:", df_pedidos.columns.tolist())
    except Exception as exc:
        print(f"Error fetching pedidos: {exc}")

    print("\n--- Testing Generic View Fetch (Financeiro) ---")
    try:
        df_financeiro = fetch_view_data(
            view_name="vw_dashboard_financeiro",
            limit=5,
            order_by="data_vencimento",
            ascending=False
        )
        print(f"Fetched {len(df_financeiro)} lancamentos financeiros.")
        if not df_financeiro.empty:
            print("Columns:", df_financeiro.columns.tolist())
    except Exception as exc:
        print(f"Error fetching financeiro: {exc}")

    print("\n--- Testing RPC (Financeiro KPIs) ---")
    try:
        kpis = fetch_finance_kpis_rpc("2000-01-01", "2030-12-31")
        print("KPIs:", kpis)
    except Exception as exc:
        print(f"Error fetching KPIs: {exc}")

if __name__ == "__main__":
    smoke_test()
