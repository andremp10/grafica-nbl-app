import os
import time
from typing import Any, Callable, Dict, Iterable, Optional

import pandas as pd
import streamlit as st
from supabase import create_client

from data.supabase_client import get_supabase_client

PAGE_SIZE_DEFAULT = int(os.getenv("DASHBOARD_PAGE_SIZE", "200"))
TTL_SNAPSHOT_META = int(os.getenv("SNAPSHOT_META_TTL_S", "60"))
TTL_DATA = int(os.getenv("DASHBOARD_DATA_TTL_S", str(60 * 60 * 24)))
TTL_KPI = int(os.getenv("DASHBOARD_KPI_TTL_S", str(60 * 60 * 24)))
DATE_MIN = "1900-01-01"
DATE_MAX = "2100-12-31"
PEDIDOS_COUNT_PROBES = ("pedido_id", "status_pedido", "cliente_nome")
FINANCE_KPI_DEFAULT = {"entradas": 0.0, "saidas": 0.0, "saldo": 0.0, "count": 0}
PEDIDOS_DATE_CUTOFF = pd.Timestamp("1900-01-01", tz="UTC")
POSTGREST_IN_CHUNK = int(os.getenv("POSTGREST_IN_CHUNK", "200"))
POSTGREST_PAGE_ROWS = int(os.getenv("POSTGREST_PAGE_ROWS", "1000"))


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


def _chunked(values: list[Any], chunk_size: int) -> Iterable[list[Any]]:
    if chunk_size <= 0:
        yield values
        return
    for idx in range(0, len(values), chunk_size):
        yield values[idx : idx + chunk_size]


def _fetch_all_in(
    client: Any,
    table: str,
    select: str,
    column: str,
    values: list[Any],
    order: Optional[tuple[str, bool]] = None,
    page_size: int = POSTGREST_PAGE_ROWS,
    in_chunk: int = POSTGREST_IN_CHUNK,
) -> list[Dict[str, Any]]:
    """Fetch rows for many ids without exceeding PostgREST URL limits."""
    rows: list[Dict[str, Any]] = []
    for values_chunk in _chunked(values, in_chunk):
        offset = 0
        while True:
            query = client.table(table).select(select).in_(column, values_chunk)
            if order:
                query = query.order(order[0], desc=order[1])
            response = query.range(offset, offset + page_size - 1).execute()
            chunk = response.data or []
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
    return rows


def _build_cliente_nome_map(client: Any, cliente_ids: list[str]) -> Dict[str, str]:
    if not cliente_ids:
        return {}

    pf_rows = _fetch_all_in(
        client=client,
        table="is_clientes_pf",
        select="cliente_id,nome,sobrenome",
        column="cliente_id",
        values=cliente_ids,
        order=None,
    )
    pj_rows = _fetch_all_in(
        client=client,
        table="is_clientes_pj",
        select="cliente_id,razao_social",
        column="cliente_id",
        values=cliente_ids,
        order=None,
    )

    pf_map: Dict[str, str] = {}
    for row in pf_rows:
        cid = str(row.get("cliente_id") or "")
        nome = str(row.get("nome") or "").strip()
        sobrenome = str(row.get("sobrenome") or "").strip()
        full = f"{nome} {sobrenome}".strip()
        if cid and full:
            pf_map[cid] = full

    pj_map: Dict[str, str] = {}
    for row in pj_rows:
        cid = str(row.get("cliente_id") or "")
        razao = str(row.get("razao_social") or "").strip()
        if cid and razao:
            pj_map[cid] = razao

    # Merge PF first, then PJ, then fallback.
    merged: Dict[str, str] = {}
    for cid in cliente_ids:
        cid_str = str(cid)
        if cid_str in pf_map:
            merged[cid_str] = pf_map[cid_str]
        elif cid_str in pj_map:
            merged[cid_str] = pj_map[cid_str]
        else:
            merged[cid_str] = f"Cliente #{cid_str[:8]}"
    return merged


def _aggregate_pedidos_items(items_df: pd.DataFrame) -> pd.DataFrame:
    if items_df.empty:
        return pd.DataFrame(
            columns=["pedido_id", "qtde_itens", "data_prazo", "data_entrega", "status_pedido"]
        )

    df = items_df.copy()
    for col in ["previsao_producao", "previsao_entrega"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            df.loc[df[col] < PEDIDOS_DATE_CUTOFF, col] = pd.NaT

    def agg_status(series: pd.Series) -> Optional[str]:
        values = sorted(
            {
                str(value).strip()
                for value in series.dropna().tolist()
                if str(value).strip()
            }
        )
        return ", ".join(values) if values else None

    return (
        df.groupby("pedido_id", dropna=False, as_index=False)
        .agg(
            qtde_itens=("pedido_id", "size"),
            data_prazo=("previsao_producao", "min"),
            data_entrega=("previsao_entrega", "max"),
            status_pedido=("status", agg_status),
        )
        .astype({"qtde_itens": "int64"}, errors="ignore")
    )


def _compute_pedidos_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Compute is_finalizado / is_atrasado / dias_em_atraso from status + prazo."""
    if df.empty:
        return df

    out = df.copy()
    out["status_pedido"] = out.get("status_pedido", pd.Series([], dtype="object")).fillna("Sem Status")

    status_lower = out["status_pedido"].astype(str).str.lower()
    out["is_finalizado"] = (
        status_lower.str.contains("entregue", na=False)
        | status_lower.str.contains("finalizado", na=False)
        | status_lower.str.contains("concluid", na=False)
        | status_lower.str.contains("cancelad", na=False)
    )

    prazo = pd.to_datetime(out.get("data_prazo_validada"), errors="coerce", utc=True)
    now = pd.Timestamp.now(tz="UTC")
    out["is_atrasado"] = prazo.notna() & (prazo < now) & (~out["is_finalizado"])

    today = now.normalize()
    prazo_date = prazo.dt.normalize()
    dias = (today - prazo_date).dt.days
    out["dias_em_atraso"] = 0
    out.loc[out["is_atrasado"], "dias_em_atraso"] = dias[out["is_atrasado"]].clip(lower=0).fillna(0).astype(int)
    out["dias_em_atraso"] = out["dias_em_atraso"].fillna(0).astype(int)

    return out


def _fetch_all_pedidos_base(
    client: Any, start_date: Optional[str], end_date: Optional[str]
) -> pd.DataFrame:
    """Fetch all pedidos (id/cliente_id/created_at/total/frete) within a date range."""
    select = "id,cliente_id,total,frete_valor,created_at"
    offset = 0
    rows: list[Dict[str, Any]] = []

    while True:
        query = client.table("is_pedidos").select(select)
        if start_date and end_date:
            query = query.gte("created_at", f"{start_date}T00:00:00").lte(
                "created_at", f"{end_date}T23:59:59"
            )
        query = query.order("created_at", desc=True)
        response = query.range(offset, offset + POSTGREST_PAGE_ROWS - 1).execute()
        chunk = response.data or []
        rows.extend(chunk)
        if len(chunk) < POSTGREST_PAGE_ROWS:
            break
        offset += POSTGREST_PAGE_ROWS

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).rename(
        columns={
            "id": "pedido_id",
            "total": "valor_total",
            "frete_valor": "frete_valor",
            "created_at": "data_criacao",
        }
    )
    df["pedido_id"] = df["pedido_id"].astype(str)
    df["cliente_id"] = df["cliente_id"].astype(str)
    df["data_criacao"] = pd.to_datetime(df["data_criacao"], errors="coerce", utc=True)
    return df


def _compute_pedidos_kpis_from_tables(
    client: Any, start_date: str, end_date: str
) -> Dict[str, Any]:
    pedidos_df = _fetch_all_pedidos_base(client=client, start_date=start_date, end_date=end_date)
    if pedidos_df.empty:
        return {"total_pedidos": 0, "total_atrasados": 0, "total_finalizados": 0}

    pedido_ids = pedidos_df["pedido_id"].dropna().astype(str).unique().tolist()
    items_rows = _fetch_all_in(
        client=client,
        table="is_pedidos_itens",
        select="pedido_id,status,previsao_producao,previsao_entrega",
        column="pedido_id",
        values=pedido_ids,
        order=("pedido_id", False),
    )
    items_df = pd.DataFrame(items_rows) if items_rows else pd.DataFrame()
    if not items_df.empty:
        items_df["pedido_id"] = items_df["pedido_id"].astype(str)

    agg_df = _aggregate_pedidos_items(items_df)
    merged = pedidos_df.merge(agg_df, on="pedido_id", how="left")
    merged["status_pedido"] = merged.get("status_pedido").fillna("Sem Status")
    merged["data_prazo_validada"] = merged.get("data_entrega").combine_first(merged.get("data_prazo"))
    merged = _compute_pedidos_flags(merged)

    total = int(len(merged))
    total_atrasados = int(merged.get("is_atrasado", False).sum())
    total_finalizados = int(merged.get("is_finalizado", False).sum())
    return {
        "total_pedidos": total,
        "total_atrasados": total_atrasados,
        "total_finalizados": total_finalizados,
    }


def _fetch_pedidos_from_tables(
    client: Any,
    start_date: Optional[str],
    end_date: Optional[str],
    page: int,
    page_size: int,
) -> pd.DataFrame:
    pedido_select = "id,cliente_id,total,frete_valor,created_at"
    query = client.table("is_pedidos").select(pedido_select)
    if start_date and end_date:
        query = query.gte("created_at", f"{start_date}T00:00:00").lte("created_at", f"{end_date}T23:59:59")
    query = query.order("created_at", desc=True)

    start = page * page_size
    end = start + page_size - 1
    response = query.range(start, end).execute()
    pedidos_rows = response.data or []
    if not pedidos_rows:
        return pd.DataFrame()

    pedidos_df = pd.DataFrame(pedidos_rows).rename(
        columns={
            "id": "pedido_id",
            "total": "valor_total",
            "frete_valor": "frete_valor",
            "created_at": "data_criacao",
        }
    )
    pedidos_df["pedido_id"] = pedidos_df["pedido_id"].astype(str)
    pedidos_df["cliente_id"] = pedidos_df["cliente_id"].astype(str)
    pedidos_df["data_criacao"] = pd.to_datetime(pedidos_df["data_criacao"], errors="coerce", utc=True)

    pedido_ids = pedidos_df["pedido_id"].dropna().astype(str).unique().tolist()
    if not pedido_ids:
        return pd.DataFrame()

    items_rows = _fetch_all_in(
        client=client,
        table="is_pedidos_itens",
        select="pedido_id,status,qtde,valor,previsao_producao,previsao_entrega",
        column="pedido_id",
        values=pedido_ids,
        order=("pedido_id", False),
    )
    items_df = pd.DataFrame(items_rows) if items_rows else pd.DataFrame()
    if not items_df.empty:
        items_df["pedido_id"] = items_df["pedido_id"].astype(str)

    agg_df = _aggregate_pedidos_items(items_df)
    merged = pedidos_df.merge(agg_df, on="pedido_id", how="left")
    merged["qtde_itens"] = merged.get("qtde_itens").fillna(0).astype(int)
    merged["status_pedido"] = merged.get("status_pedido").fillna("Sem Status")

    merged["data_prazo_validada"] = merged.get("data_entrega").combine_first(merged.get("data_prazo"))
    merged = _compute_pedidos_flags(merged)

    cliente_ids = merged["cliente_id"].dropna().astype(str).unique().tolist()
    nome_map = _build_cliente_nome_map(client, cliente_ids)
    merged["cliente_nome"] = merged["cliente_id"].astype(str).map(nome_map)

    # Keep the public view schema expected by the app.
    columns = [
        "pedido_id",
        "cliente_id",
        "cliente_nome",
        "data_criacao",
        "data_prazo_validada",
        "status_pedido",
        "qtde_itens",
        "valor_total",
        "frete_valor",
        "is_finalizado",
        "is_atrasado",
        "dias_em_atraso",
    ]
    for col in columns:
        if col not in merged.columns:
            merged[col] = None
    return merged[columns]

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

    try:
        df = _fetch_pedidos_from_tables(
            client=client,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
        )
        if df.empty:
            return df

        if status and status != "Todos":
            df = df[df["status_pedido"].astype(str).str.contains(status, case=False, na=False)]
        if client_name:
            df = df[df["cliente_nome"].astype(str).str.contains(client_name, case=False, na=False)]
        if is_atrasado is not None:
            df = df[df["is_atrasado"] == is_atrasado]
        if is_finalizado is not None:
            df = df[df["is_finalizado"] == is_finalizado]
        return df
    except Exception as exc:
        # Fallback: legacy view (may timeout depending on DB definition).
        try:
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
            response = query.range(start, end).execute()
            return pd.DataFrame(response.data)
        except Exception as fallback_exc:
            st.error(f"Erro ao buscar pedidos (tabelas/view): {exc} / {fallback_exc}")
            return pd.DataFrame()


@st.cache_data(ttl=TTL_KPI)
def fetch_kpis_pedidos(start_date: str, end_date: str, snapshot_key: Optional[str] = None) -> Dict[str, Any]:
    _ = snapshot_key
    client = get_supabase_client()
    if not client:
        return {"total_pedidos": 0, "total_atrasados": 0, "total_finalizados": 0}

    # Prefer computing from the source tables. This avoids heavy dashboard views that may
    # hit PostgREST statement_timeout when computing per-row aggregates.
    try:
        return _compute_pedidos_kpis_from_tables(client=client, start_date=start_date, end_date=end_date)
    except Exception as exc:
        print(f"Error computing pedidos KPIs via tables: {exc}")

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
        df = _fetch_pedidos_from_tables(
            client=self._client(),
            start_date=None,
            end_date=None,
            page=0,
            page_size=self.page_size,
        )
        return df.to_dict(orient="records") if not df.empty else []

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
