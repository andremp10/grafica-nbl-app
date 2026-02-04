import os
import time
from typing import Any, Callable, Dict, Iterable, Optional

import pandas as pd
import streamlit as st
from supabase import create_client

from data.supabase_client import get_supabase_client

PAGE_SIZE_DEFAULT = int(os.getenv("DASHBOARD_PAGE_SIZE", "200"))
TTL_SNAPSHOT_META = int(os.getenv("SNAPSHOT_META_TTL_S", "60"))
TTL_DATA = int(os.getenv("DASHBOARD_DATA_TTL_S", str(60 * 60 * 6)))
TTL_KPI = int(os.getenv("DASHBOARD_KPI_TTL_S", str(60 * 60 * 6)))
DATE_MIN = "1900-01-01"
DATE_MAX = "2100-12-31"
PEDIDOS_COUNT_PROBES = ("pedido_id", "status_pedido", "cliente_nome")
FINANCE_KPI_DEFAULT = {"entradas": 0.0, "saidas": 0.0, "saldo": 0.0, "count": 0}


def _to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _is_entrada(tipo: Any) -> bool:
    value = str(tipo or "").strip().lower()
    return value in {"entrada", "receita", "1"}


def _is_saida(tipo: Any) -> bool:
    value = str(tipo or "").strip().lower()
    return value in {"saida", "saída", "despesa", "2"}


def _normalize_kpi_payload(payload: Any) -> Dict[str, float]:
    data = payload
    if isinstance(data, list):
        data = data[0] if data else {}
    if not isinstance(data, dict):
        data = {}

    entradas = _to_float(data.get("entradas"))
    saidas = _to_float(data.get("saidas"))
    saldo = _to_float(data.get("saldo"))
    count = _to_int(data.get("count"))
    return {"entradas": entradas, "saidas": saidas, "saldo": saldo, "count": count}


def _apply_pedidos_filters(
    query: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    client_name: Optional[str] = None,
    is_atrasado: Optional[bool] = None,
    is_finalizado: Optional[bool] = None,
) -> Any:
    if start_date and end_date:
        query = query.gte("data_criacao", f"{start_date}T00:00:00").lte(
            "data_criacao", f"{end_date}T23:59:59"
        )
    if status and status != "Todos":
        query = query.ilike("status_pedido", f"%{status}%")
    if client_name:
        query = query.ilike("cliente_nome", f"%{client_name}%")
    if is_atrasado is not None:
        query = query.eq("is_atrasado", is_atrasado)
    if is_finalizado is not None:
        query = query.eq("is_finalizado", is_finalizado)
    return query


def _safe_exact_count(query_factory: Callable[[str], Any], probes: Iterable[str]) -> int:
    for probe in probes:
        try:
            response = query_factory(probe).execute()
            count = getattr(response, "count", None)
            if count is not None:
                return int(count)
        except Exception:
            continue
    return 0


def _compute_finance_kpis_from_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    entradas = 0.0
    saidas = 0.0
    count = 0
    for row in rows:
        count += 1
        valor = _to_float(row.get("valor"))
        tipo = row.get("tipo")
        if _is_entrada(tipo):
            entradas += valor
        elif _is_saida(tipo):
            saidas += valor
    return {
        "entradas": entradas,
        "saidas": saidas,
        "saldo": entradas - saidas,
        "count": count,
    }


def _compute_finance_kpis_fallback(
    client: Any, start_date: Optional[str], end_date: Optional[str], page_size: int
) -> Dict[str, float]:
    start = start_date or DATE_MIN
    end = end_date or DATE_MAX
    offset = 0
    rows: list[Dict[str, Any]] = []

    while True:
        query = client.table("vw_dashboard_financeiro").select("tipo, valor")
        query = query.gte("competencia_mes", start).lte("competencia_mes", end)
        query = query.order("data_vencimento", desc=True)
        response = query.range(offset, offset + page_size - 1).execute()
        chunk = response.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    return _compute_finance_kpis_from_rows(rows)

@st.cache_data(ttl=TTL_SNAPSHOT_META)
def fetch_snapshot_meta() -> Dict[str, Any]:
    """
    Metadados do ultimo snapshot diario.

    Para funcionar completo, aplique a migration:
    - etl/migrations/003_snapshot_meta.sql
    """
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
        # Se o RPC ainda nao existe, cai aqui.
        print(f"Error fetching snapshot meta: {exc}")

    # Fallback diario para nao prender cache para sempre antes do RPC existir.
    cache_key = str(
        data.get("snapshot_id")
        or data.get("snapshot_finished_at")
        or time.strftime("%Y-%m-%d")
    )
    data["cache_key"] = cache_key
    data["is_configured"] = True
    return data


@st.cache_data(ttl=TTL_DATA)
def fetch_pedidos(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    client_name: Optional[str] = None,
    is_atrasado: Optional[bool] = None,
    is_finalizado: Optional[bool] = None,
    page: int = 0,
    page_size: int = PAGE_SIZE_DEFAULT,
    snapshot_key: Optional[str] = None,
) -> pd.DataFrame:
    # snapshot_key exists only to invalidate Streamlit cache when the daily snapshot changes.
    _ = snapshot_key
    client = get_supabase_client()
    if not client:
        return pd.DataFrame()

    query = client.table("vw_dashboard_pedidos").select("*")
    query = _apply_pedidos_filters(
        query=query,
        start_date=start_date,
        end_date=end_date,
        status=status,
        client_name=client_name,
        is_atrasado=is_atrasado,
        is_finalizado=is_finalizado,
    )
    query = query.order("data_criacao", desc=True)

    start = page * page_size
    end = start + page_size - 1
    query = query.range(start, end)

    try:
        response = query.execute()
        return pd.DataFrame(response.data)
    except Exception as exc:
        st.error(f"Erro ao buscar pedidos: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=TTL_KPI)
def fetch_kpis_pedidos(start_date: str, end_date: str, snapshot_key: Optional[str] = None) -> Dict[str, Any]:
    _ = snapshot_key
    client = get_supabase_client()
    if not client:
        return {"total_pedidos": 0, "total_atrasados": 0, "total_finalizados": 0}

    def build_total_query(column: str) -> Any:
        query = client.table("vw_dashboard_pedidos").select(column, count="exact", head=True)
        return _apply_pedidos_filters(query=query, start_date=start_date, end_date=end_date)

    def build_atrasados_query(column: str) -> Any:
        query = client.table("vw_dashboard_pedidos").select(column, count="exact", head=True)
        return _apply_pedidos_filters(
            query=query, start_date=start_date, end_date=end_date, is_atrasado=True
        )

    def build_finalizados_query(column: str) -> Any:
        query = client.table("vw_dashboard_pedidos").select(column, count="exact", head=True)
        return _apply_pedidos_filters(
            query=query, start_date=start_date, end_date=end_date, is_finalizado=True
        )

    total = _safe_exact_count(build_total_query, PEDIDOS_COUNT_PROBES)
    total_atrasados = _safe_exact_count(build_atrasados_query, PEDIDOS_COUNT_PROBES)
    total_finalizados = _safe_exact_count(build_finalizados_query, PEDIDOS_COUNT_PROBES)
    return {
        "total_pedidos": total,
        "total_atrasados": total_atrasados,
        "total_finalizados": total_finalizados,
    }


@st.cache_data(ttl=TTL_DATA)
def fetch_financeiro(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tipo: Optional[str] = None,
    status_codigo: Optional[int] = None,
    categoria: Optional[str] = None,
    page: int = 0,
    page_size: int = PAGE_SIZE_DEFAULT,
    snapshot_key: Optional[str] = None,
) -> pd.DataFrame:
    _ = snapshot_key
    client = get_supabase_client()
    if not client:
        return pd.DataFrame()

    query = client.table("vw_dashboard_financeiro").select("*")

    if start_date and end_date:
        query = query.gte("competencia_mes", start_date).lte("competencia_mes", end_date)
    if tipo and tipo != "Todos":
        query = query.eq("tipo", tipo)
    if status_codigo is not None:
        query = query.eq("status_codigo", status_codigo)
    if categoria and categoria != "Todas":
        query = query.eq("categoria", categoria)

    query = query.order("data_vencimento", desc=True)
    start = page * page_size
    end = start + page_size - 1
    query = query.range(start, end)

    try:
        response = query.execute()
        return pd.DataFrame(response.data)
    except Exception as exc:
        st.error(f"Erro ao buscar financeiro: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=TTL_KPI)
def fetch_kpis_financeiro(start_date: str, end_date: str, snapshot_key: Optional[str] = None) -> Dict[str, float]:
    _ = snapshot_key
    client = get_supabase_client()
    if not client:
        return FINANCE_KPI_DEFAULT.copy()

    start = start_date or DATE_MIN
    end = end_date or DATE_MAX

    try:
        response = client.rpc("get_finance_kpis", {"start_date": start, "end_date": end}).execute()
        return _normalize_kpi_payload(response.data)
    except Exception as exc:
        print(f"Error fetching finance KPIs via RPC: {exc}")
        try:
            return _compute_finance_kpis_fallback(
                client=client,
                start_date=start,
                end_date=end,
                page_size=PAGE_SIZE_DEFAULT,
            )
        except Exception as fallback_exc:
            print(f"Error calculating finance KPIs via fallback: {fallback_exc}")
            return FINANCE_KPI_DEFAULT.copy()


def _read_runtime_secret(name: str) -> Optional[str]:
    try:
        value = st.secrets.get(name)
    except Exception:
        value = None
    if value:
        return str(value)
    return os.getenv(name)


class SupabaseRepo:
    def __init__(
        self,
        url: Optional[str] = None,
        key: Optional[str] = None,
        page_size: int = 1000,
        cache_ttl_s: int = 300,
    ) -> None:
        self.url = url or _read_runtime_secret("SUPABASE_URL")
        self.key = key or _read_runtime_secret("SUPABASE_KEY") or _read_runtime_secret(
            "SUPABASE_ANON_KEY"
        )
        self.page_size = page_size
        self.cache_ttl_s = cache_ttl_s
        self._cache: Dict[tuple[Any, ...], tuple[float, Any]] = {}

    def is_configured(self) -> bool:
        return bool(self.url and self.key)

    def _client(self) -> Any:
        if not self.is_configured():
            raise ValueError("Supabase nao configurado: defina SUPABASE_URL e SUPABASE_KEY.")
        return create_client(self.url, self.key)

    def _cache_get(self, key: tuple[Any, ...]) -> Optional[Any]:
        entry = self._cache.get(key)
        if not entry:
            return None
        timestamp, value = entry
        if time.time() - timestamp > self.cache_ttl_s:
            self._cache.pop(key, None)
            return None
        return value

    def _cache_set(self, key: tuple[Any, ...], value: Any) -> None:
        self._cache[key] = (time.time(), value)

    def _fetch_all(
        self,
        table: str,
        select: str = "*",
        order: Optional[tuple[str, bool]] = None,
        filters: Optional[Iterable[tuple[str, str, Any]]] = None,
    ) -> list[Dict[str, Any]]:
        cache_key = ("table", table, select, order, tuple(filters or []))
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        client = self._client()
        results: list[Dict[str, Any]] = []
        offset = 0

        while True:
            query = client.table(table).select(select)
            if filters:
                for column, op, value in filters:
                    if op == "eq":
                        query = query.eq(column, value)
                    elif op == "neq":
                        query = query.neq(column, value)
                    elif op == "ilike":
                        query = query.ilike(column, value)
                    elif op == "gte":
                        query = query.gte(column, value)
                    elif op == "lte":
                        query = query.lte(column, value)
                    else:
                        raise ValueError(f"Operador de filtro invalido: {op}")
            if order:
                query = query.order(order[0], desc=order[1])

            response = query.range(offset, offset + self.page_size - 1).execute()
            data = response.data or []
            results.extend(data)
            if len(data) < self.page_size:
                break
            offset += self.page_size

        self._cache_set(cache_key, results)
        return results

    def get_pedidos(self) -> list[Dict[str, Any]]:
        return self._fetch_all("vw_dashboard_pedidos", order=("data_criacao", True))

    def get_financeiro(self) -> list[Dict[str, Any]]:
        return self._fetch_all("vw_dashboard_financeiro", order=("data_vencimento", True))

    def get_finance_kpis(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, float]:
        start = start_date or DATE_MIN
        end = end_date or DATE_MAX
        cache_key = ("rpc", "get_finance_kpis", start, end)
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            response = self._client().rpc(
                "get_finance_kpis", {"start_date": start, "end_date": end}
            ).execute()
            data = _normalize_kpi_payload(response.data)
        except Exception:
            rows = self._fetch_all(
                table="vw_dashboard_financeiro",
                select="tipo, valor",
                filters=[("competencia_mes", "gte", start), ("competencia_mes", "lte", end)],
            )
            data = _compute_finance_kpis_from_rows(rows)

        self._cache_set(cache_key, data)
        return data
