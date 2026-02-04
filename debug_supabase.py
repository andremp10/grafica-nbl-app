import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env")


def _masked(value: str, visible: int = 10) -> str:
    if not value:
        return ""
    if len(value) <= visible:
        return "*" * len(value)
    return f"{value[:visible]}..."


def debug_connection() -> None:
    print("--- DEBUGGING SUPABASE CONNECTION ---")

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        print("Missing credentials. Configure SUPABASE_URL and SUPABASE_KEY in .env or secrets.")
        return

    print(f"URL: {_masked(url, visible=25)}")
    print(f"Key: {_masked(key)}")

    try:
        client = create_client(url, key)
        print("Client created.")
    except Exception as exc:
        print(f"Client creation failed: {exc}")
        return

    print("\n[TEST 1] Querying 'vw_dashboard_pedidos' - limit 1")
    try:
        response = client.table("vw_dashboard_pedidos").select("*").limit(1).execute()
        print(f"Rows: {len(response.data or [])}")
        if response.data:
            print("Columns:", sorted(response.data[0].keys()))
    except Exception as exc:
        print(f"Failed: {exc}")

    print("\n[TEST 2] Querying 'vw_dashboard_financeiro' - limit 1")
    try:
        response = client.table("vw_dashboard_financeiro").select("*").limit(1).execute()
        print(f"Rows: {len(response.data or [])}")
        if response.data:
            print("Columns:", sorted(response.data[0].keys()))
    except Exception as exc:
        print(f"Failed: {exc}")

    print("\n[TEST 3] Calling RPC get_finance_kpis")
    try:
        response = client.rpc(
            "get_finance_kpis", {"start_date": "2000-01-01", "end_date": "2030-12-31"}
        ).execute()
        print("RPC response:", response.data)
    except Exception as exc:
        print(f"Failed: {exc}")


if __name__ == "__main__":
    debug_connection()
