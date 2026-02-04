import os
import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import streamlit as st
from supabase import create_client

from data.supabase_client import get_supabase_client

# Constants
PAGE_SIZE_DEFAULT = int(os.getenv("DASHBOARD_PAGE_SIZE", "500"))
TTL_DATA = int(os.getenv("DASHBOARD_DATA_TTL_S", str(60 * 5)))  # 5 minutes default
DATE_MIN = "1900-01-01"
DATE_MAX = "2100-12-31"

def _read_runtime_secret(name: str) -> Optional[str]:
    try:
        value = st.secrets.get(name)
    except Exception:
        value = None
    if value:
        return str(value)
    return os.getenv(name)

@st.cache_data(ttl=60) # Short TTL for snapshot meta
def fetch_snapshot_meta() -> Dict[str, Any]:
    """Fetches ETL snapshot metadata to control caching invalidation."""
    client = get_supabase_client()
    if not client:
        return {"is_configured": False, "cache_key": "offline"}

    data: Dict[str, Any] = {}
    try:
        response = client.rpc("get_snapshot_meta", {}).execute()
        payload = response.data
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        if isinstance(payload, dict):
            data = payload
    except Exception as exc:
        print(f"Error fetching snapshot meta: {exc}")

    # Fallback cache key
    cache_key = str(
        data.get("snapshot_id")
        or data.get("snapshot_finished_at")
        or time.strftime("%Y-%m-%d")
    )
    data["cache_key"] = cache_key
    data["is_configured"] = True
    return data

@st.cache_data(ttl=TTL_DATA)
def fetch_view_data(
    view_name: str,
    filters: Optional[Dict[str, Any]] = None,
    date_column: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    order_by: Optional[str] = None,
    ascending: bool = False,
    limit: Optional[int] = None,
    snapshot_key: Optional[str] = None
) -> pd.DataFrame:
    """
    Generic function to fetch data from any Supabase view.
    
    Args:
        view_name: Database view name (e.g. 'vw_dashboard_pedidos')
        filters: Dict of filters {col: val} for equality, or {col__ilike: val}, etc.
        date_column: Column name to filter by date range
        start_date: ISO start date
        end_date: ISO end date
        order_by: Column to sort by
        ascending: Sort direction
        limit: Max rows to return (None = fetch all pages up to reasonable limit)
        snapshot_key: Used for cache invalidation
    """
    _ = snapshot_key # Used only for cache behavior
    client = get_supabase_client()
    if not client:
        return pd.DataFrame()

    try:
        query = client.table(view_name).select("*")

        # scalar filters (eq) and special filters (ilike, neq, etc)
        if filters:
            for key, value in filters.items():
                if value is None:
                    continue
                
                if "__" in key:
                    col, op = key.split("__", 1)
                else:
                    col, op = key, "eq"

                # Handle specific operators
                if op == "eq":
                    query = query.eq(col, value)
                elif op == "ilike":
                    query = query.ilike(col, f"%{value}%")
                elif op == "neq":
                    query = query.neq(col, value)
                elif op == "gt":
                    query = query.gt(col, value)
                elif op == "lt":
                    query = query.lt(col, value)
                elif op == "is":
                    query = query.is_(col, value)

        # Date range filter
        if date_column and start_date and end_date:
            # Ensure ISO format timestamps
            s_ts = f"{start_date}T00:00:00" if "T" not in str(start_date) else start_date
            e_ts = f"{end_date}T23:59:59" if "T" not in str(end_date) else end_date
            query = query.gte(date_column, s_ts).lte(date_column, e_ts)

        # Ordering
        if order_by:
            query = query.order(order_by, desc=not ascending)

        # Pagination / limit
        results = []
        page_size = PAGE_SIZE_DEFAULT
        offset = 0
        max_rows = limit if limit else 10000

        while True:
            current_limit = page_size
            if limit and (offset + page_size > limit):
                current_limit = limit - offset
            
            if current_limit <= 0:
                break

            response = query.range(offset, offset + current_limit - 1).execute()
            data = response.data or []
            results.extend(data)
            
            if len(data) < page_size or (limit and len(results) >= limit):
                break
            
            offset += page_size

        return pd.DataFrame(results)

    except Exception as e:
        st.error(f"Erro ao buscar dados de '{view_name}': {e}")
        return pd.DataFrame()

@st.cache_data(ttl=TTL_DATA)
def fetch_kpis_generic(
    view_name: str,
    probes: List[Dict[str, Any]], # List of filter dicts to count
    date_column: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    snapshot_key: Optional[str] = None
) -> Dict[str, int]:
    """
    Fetches counts for multiple scenarios efficiently.
    Args:
        probes: List of dicts, e.g. [{"label": "total", "filters": {}}, {"label": "delayed", "filters": {"is_atrasado": True}}]
    """
    _ = snapshot_key
    client = get_supabase_client()
    if not client:
        return {p["label"]: 0 for p in probes}
    
    results = {}
    
    for probe in probes:
        try:
            query = client.table(view_name).select("*", count="exact", head=True)
            
            # Apply common date filter
            if date_column and start_date and end_date:
                s_ts = f"{start_date}T00:00:00" if "T" not in str(start_date) else start_date
                e_ts = f"{end_date}T23:59:59" if "T" not in str(end_date) else end_date
                query = query.gte(date_column, s_ts).lte(date_column, e_ts)

            # Apply specific probe filters
            filters = probe.get("filters", {})
            for key, value in filters.items():
                if value is None: continue
                
                if "__" in key:
                    col, op = key.split("__", 1)
                else:
                    col, op = key, "eq"
                    
                if op == "eq": query = query.eq(col, value)
                elif op == "ilike": query = query.ilike(col, f"%{value}%")
                elif op == "is": query = query.is_(col, value)

            res = query.execute()
            results[probe["label"]] = res.count if res.count is not None else 0
            
        except Exception:
            results[probe["label"]] = 0
            
    return results

# Legacy/Helper wrappers for specific complex logic (like RPCs) can stay or receive generic kwargs
@st.cache_data(ttl=TTL_DATA)
def fetch_finance_kpis_rpc(start_date: str, end_date: str, snapshot_key: Optional[str] = None) -> Dict[str, float]:
    _ = snapshot_key
    client = get_supabase_client()
    default_kpis = {"entradas": 0.0, "saidas": 0.0, "saldo": 0.0, "count": 0}
    
    if not client:
        return default_kpis

    try:
        res = client.rpc("get_finance_kpis", {"start_date": start_date, "end_date": end_date}).execute()
        data = res.data
        if isinstance(data, list) and data: data = data[0]
        if isinstance(data, dict):
             return {k: float(v or 0) for k, v in data.items()}
    except Exception as e:
        print(f"RPC Error: {e}")
    
    return default_kpis
