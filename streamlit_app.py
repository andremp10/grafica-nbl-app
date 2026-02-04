import os
import time
from datetime import datetime, timedelta
from typing import Optional, Sequence

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from data.supabase_client import is_connected
from data.supabase_repo import (
    fetch_snapshot_meta,
    fetch_view_data,
    fetch_kpis_generic,
    fetch_finance_kpis_rpc
)
from services.n8n_service import send_message_to_n8n

# --- 1. CONFIGURACAO ---
load_dotenv()
st.set_page_config(
    page_title="Grafica NBL Admin",
    page_icon="🎨",
    layout="wide",
    initial_sidebar_state="auto",
)

# --- 2. CSS ---
st.markdown(
    """
<style>
    .main .block-container {
        max-width: 1200px;
        padding-top: 2rem;
        padding-bottom: 5rem;
    }

    .hero-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        min-height: 50vh;
        text-align: center;
        padding: 1rem;
    }
    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        color: #fff;
        margin-bottom: 1.5rem;
    }

    @media (max-width: 768px) {
        .main .block-container {
            padding-top: 1rem;
            padding-bottom: 6rem;
        }
        .hero-title { font-size: 1.8rem; }
        .metric-box { padding: 1rem; margin-bottom: 0.5rem; }
        .stButton button { width: 100%; }
    }

    .metric-box {
        background: #151515; border: 1px solid #2a2a2a; border-radius: 12px;
        padding: 1.5rem; text-align: center; transition: transform 0.1s;
    }
    .metric-box:hover {transform: translateY(-2px); border-color: #333;}
    .metric-val {font-size: 2rem; font-weight: bold; color:white; margin: 0.5rem 0;}
    .metric-lbl {font-size: 0.85rem; color: #888; letter-spacing: 0.5px; text-transform: uppercase;}
    .metric-delta {font-size: 0.9rem; font-weight: 500;}
    .up {color: #10b981;} .down {color: #ef4444;}

    .guide-box {background: #1a1a1a; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem; border-left: 4px solid #2563eb;}
    .prompt-card {background: #151515; border: 1px dashed #444; padding: 10px 15px; border-radius: 6px; font-family: monospace; color: #a5b4fc; margin-bottom: 8px; font-size: 0.9rem;}
</style>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def format_currency(value: object) -> str:
    try:
        number = float(value)
        return f"R$ {number:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(value)


def kpi_card(label: str, value: object, delta: Optional[str] = None, color: str = "up") -> str:
    delta_html = f'<div class="metric-delta {color}">{delta}</div>' if delta else ""
    display_value = str(value)
    if isinstance(value, (int, float)):
        # Only format as currency if label strongly implies it
        is_money = any(x in label.lower() for x in ["valor", "total", "saldo", "faturamento", "custo", "entradas", "saidas"])
        is_money = is_money and ("qtde" not in label.lower() and "itens" not in label.lower() and "count" not in label.lower() and "pedidos" not in label.lower() and "registros" not in label.lower())
        
        if is_money:
            display_value = format_currency(value)
        else:
            # Integer formatting for counts
            if isinstance(value, int) or (isinstance(value, float) and value.is_integer()):
                display_value = f"{int(value):,}".replace(",", ".")
            else:
                display_value = f"{value:.2f}"

    return (
        f'<div class="metric-box"><div class="metric-lbl">{label}</div>'
        f'<div class="metric-val">{display_value}</div>{delta_html}</div>'
    )

def _format_iso_dt(value: object) -> str:
    """Safe formatting for timestamps coming from Supabase (timestamptz)."""
    if not value:
        return "-"
    try:
        text = str(value)
        dt_value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt_value.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(value)

def _has_runtime_secret(key: str) -> bool:
    """Checks Streamlit secrets/env without revealing the value."""
    variants = [key, key.upper(), key.lower()]
    for variant in variants:
        try:
            if variant in st.secrets and st.secrets.get(variant):
                return True
        except Exception:
            pass
        if os.getenv(variant):
            return True
    return False


def _to_date_bounds(date_value: object) -> tuple[Optional[str], Optional[str]]:
    if isinstance(date_value, Sequence) and not isinstance(date_value, (str, bytes)):
        if len(date_value) >= 2:
            start, end = date_value[0], date_value[1]
        elif len(date_value) == 1:
            start = end = date_value[0]
        else:
            return None, None
    else:
        start = end = date_value

    if not hasattr(start, "strftime") or not hasattr(end, "strftime"):
        return None, None
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _existing_columns(columns: Sequence[str], preferred: Sequence[str]) -> list[str]:
    existing = set(columns)
    return [column for column in preferred if column in existing]


def _safe_dataframe(df, preferred_order, preferred_config, **kwargs):
    column_order = _existing_columns(df.columns.tolist(), preferred_order)
    column_config = {key: value for key, value in preferred_config.items() if key in df.columns}
    dataframe_kwargs = {
        "use_container_width": True,
        "hide_index": True,
    }
    dataframe_kwargs.update(kwargs)
    if column_order:
        dataframe_kwargs["column_order"] = column_order
    if column_config:
        dataframe_kwargs["column_config"] = column_config
    st.dataframe(df, **dataframe_kwargs)


def _normalize_pedidos_df(df):
    if df.empty:
        return df
    out = df.copy()
    for col in ["data_criacao", "data_prazo_validada"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
    if "valor_total" in out.columns:
        out["valor_total"] = pd.to_numeric(out["valor_total"], errors="coerce").fillna(0.0)
    if "dias_em_atraso" in out.columns:
        out["dias_em_atraso"] = pd.to_numeric(out["dias_em_atraso"], errors="coerce").fillna(0).astype(int)
    if "status_pedido" in out.columns:
        out["status_pedido"] = out["status_pedido"].fillna("Sem Status").astype(str)
    if "cliente_nome" in out.columns:
        out["cliente_nome"] = out["cliente_nome"].fillna("Cliente sem nome").astype(str)
    for col in ["is_atrasado", "is_finalizado"]:
        if col in out.columns:
            out[col] = out[col].fillna(False).astype(bool)
    return out


def _normalize_financeiro_df(df):
    if df.empty:
        return df
    out = df.copy()
    for col in ["data_vencimento", "data_pagamento", "data_emissao", "competencia_mes"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
    if "valor" in out.columns:
        out["valor"] = pd.to_numeric(out["valor"], errors="coerce").fillna(0.0)
    for col in ["tipo", "status_texto", "categoria", "descricao"]:
        if col in out.columns:
            out[col] = out[col].fillna("-").astype(str)
    for col in ["is_atrasado", "is_realizado"]:
        if col in out.columns:
            out[col] = out[col].fillna(False).astype(bool)
    return out


# =============================================================================
# VIEW: INSTRUCOES
# =============================================================================


def render_instructions():
    st.markdown("### 📘 Instruções (Snapshot Diário)")
    st.divider()

    st.markdown(
        """
Este app **não é realtime**: ele funciona em modo **snapshot diário**.

Os dados são atualizados **1x por dia**, na madrugada (truncate + reload).
O Streamlit usa cache e um *token do snapshot* para não ficar consultando o banco o tempo todo
e para evitar telas vazias durante o período de atualização.
"""
    )

    st.markdown("#### ✅ Status do Sistema")
    if is_connected():
        meta = fetch_snapshot_meta()
        finished_at = meta.get("snapshot_finished_at")
        is_running = bool(meta.get("is_running"))

        st.success("Supabase conectado.")
        if finished_at:
            st.info(f"Último snapshot concluído em: `{_format_iso_dt(finished_at)}`")
        else:
            st.warning(
                "Snapshot meta ainda não configurado no banco. "
                "Aplique a migration `etl/migrations/003_snapshot_meta.sql`."
            )
        if is_running:
            st.warning("Atualização em andamento: o app mantém o último snapshot bem-sucedido em cache.")
    else:
        st.error("Supabase offline: configure as secrets para liberar PCP/Financeiro.")

    st.markdown("#### 🔑 Configurar Secrets (Streamlit Cloud)")
    st.markdown(
        "No Streamlit Cloud, vá em **Manage app → Settings → Secrets** e adicione:"
    )
    st.code(
        "\n".join(
            [
                'SUPABASE_URL = \"https://<seu-projeto>.supabase.co\"',
                'SUPABASE_ANON_KEY = \"<sua-anon-key>\"',
                'WEBHOOK_URL = \"https://<seu-n8n>/webhook/...\"',
            ]
        ),
        language="toml",
    )
    st.caption("Recomendado: usar `SUPABASE_ANON_KEY` no app (não use service_role no Streamlit Cloud).")

    st.markdown("#### 🔎 Diagnóstico de Secrets (sem expor valores)")
    st.table(
        [
            {"Chave": "SUPABASE_URL", "Detectada": _has_runtime_secret("SUPABASE_URL")},
            {"Chave": "SUPABASE_ANON_KEY", "Detectada": _has_runtime_secret("SUPABASE_ANON_KEY")},
            {"Chave": "SUPABASE_KEY (fallback)", "Detectada": _has_runtime_secret("SUPABASE_KEY")},
            {"Chave": "WEBHOOK_URL", "Detectada": _has_runtime_secret("WEBHOOK_URL")},
        ]
    )
    st.caption(
        "Se estiver tudo como `False`, o Streamlit Cloud não está lendo seus Secrets. "
        "Confirme o nome das chaves e reinicie o app."
    )

    st.markdown("#### 🧱 Migrações SQL (1x)")
    st.markdown(
        "\n".join(
            [
                "Execute no Supabase SQL Editor (na ordem):",
                "",
                "1. `etl/migrations/001_create_dashboard_views.sql`",
                "2. `etl/migrations/002_dashboard_views_rpc_grants.sql`",
                "3. `etl/migrations/003_snapshot_meta.sql`",
                "4. `etl/migrations/004_optimize_vw_dashboard_pedidos.sql` (recomendado - performance do PCP)",
            ]
        )
    )
    st.caption(
        "Depois de criar RPCs/views, faça reload do schema cache no Supabase (Settings → API) "
        "para o app enxergar as funções."
    )
    st.caption(
        "Se aparecer erro `57014 canceling statement due to statement timeout` ao listar pedidos, "
        "a migration 004 é obrigatória."
    )

    st.markdown("#### 🕒 ETL Diário (ação do backend)")
    st.markdown(
        """
No final do ETL (quando os dados já estão 100% carregados), registre o snapshot na tabela:

- `public.etl_snapshots` (`status='success'`, `finished_at=now()`)

O app consulta o RPC `get_snapshot_meta()` para detectar mudança e atualizar o cache automaticamente.
"""
    )
    st.code(
        "\n".join(
            [
                "-- Início do ETL",
                "insert into public.etl_snapshots(status, note) values ('running', 'carga diaria');",
                "",
                "-- Fim do ETL (marcar sucesso no ultimo run 'running')",
                "update public.etl_snapshots",
                "set status='success', finished_at=now()",
                "where id = (",
                "  select id from public.etl_snapshots",
                "  where status='running'",
                "  order by started_at desc, id desc",
                "  limit 1",
                ");",
            ]
        ),
        language="sql",
    )


# =============================================================================
# VIEW: STATUS (PCP)
# =============================================================================


def render_status_view():
    st.markdown("### 🏭 Chão de Fábrica (PCP)")
    st.caption("Snapshot diário da produção (atualizado 1x por dia) - dados do Supabase")

    if not is_connected():
        st.warning("⚠️ Supabase não configurado. Defina `SUPABASE_URL` + `SUPABASE_ANON_KEY` (ou `SUPABASE_KEY`).")
        return

    st.divider()

    snapshot = fetch_snapshot_meta()
    snapshot_key = snapshot.get("cache_key")
    if snapshot.get("snapshot_finished_at"):
        st.caption(f"Última atualização: `{_format_iso_dt(snapshot.get('snapshot_finished_at'))}`")

    col_f1, col_f2 = st.columns([2, 2])
    with col_f1:
        data_range = st.date_input(
            "Período",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            key="pcp_date_range",
        )
    with col_f2:
        cliente_search = st.text_input("Cliente (busca livre)", key="pcp_cliente_search")

    data_inicio, data_fim = _to_date_bounds(data_range)

    with st.spinner("Carregando pedidos do snapshot..."):
        base_df = fetch_view_data(
            view_name="vw_dashboard_pedidos",
            start_date=data_inicio,
            end_date=data_fim,
            date_column="data_criacao",
            snapshot_key=snapshot_key,
        )
    base_df = _normalize_pedidos_df(base_df)


    with st.spinner("Carregando KPIs..."):
        # Generic KPI fetching
        kpi_results = fetch_kpis_generic(
            view_name="vw_dashboard_pedidos",
            probes=[
                {"label": "total", "filters": {}},
                {"label": "atrasados", "filters": {"is_atrasado": True}},
                {"label": "finalizados", "filters": {"is_finalizado": True}}
            ],
            date_column="data_criacao", # Or data_prazo_validada depending on business logic
            start_date=data_inicio,
            end_date=data_fim,
            snapshot_key=snapshot_key
        )

    total = kpi_results.get("total", 0)
    atrasados = kpi_results.get("atrasados", 0)
    finalizados = kpi_results.get("finalizados", 0)
    em_andamento = max(total - finalizados, 0)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card("Total Pedidos", total), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Atrasados", atrasados, color="down"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card("Finalizados", finalizados, color="up"), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("Em Andamento", em_andamento), unsafe_allow_html=True)

    st.divider()

    
    # Custom sort for essential statuses
    pipeline_order = {
        "Problema no Arquivo": 0,
        "Em Análise": 1,
        "Em Produção": 2,
        "Enviado": 3,
        "Finalizado": 4,
        "Cancelado": 5,
        "Aguardando": 6
    }
    
    unique_statuses = base_df["status_pedido"].dropna().astype(str).unique().tolist()
    status_options = sorted(unique_statuses, key=lambda x: pipeline_order.get(x, 99))

    col_a, col_b, col_c, col_d = st.columns([2, 1, 1, 1])
    with col_a:
        selected_status = st.multiselect(
            "Status",
            options=status_options,
            placeholder="Todos",
            key="pcp_status_multi",
        )
    with col_b:
        situacao = st.selectbox(
            "Situação",
            ["Todos", "Em andamento", "Atrasados", "Finalizados", "No prazo"],
            key="pcp_situacao",
        )
    with col_c:
        sort_field = st.selectbox(
            "Ordenar por",
            ["Criação", "Prazo", "Valor", "Dias em atraso"],
            key="pcp_sort_field",
        )
    with col_d:
        sort_desc = st.checkbox("Descendente", value=True, key="pcp_sort_desc")

    df = base_df.copy()
    if selected_status:
        df = df[df["status_pedido"].isin(selected_status)]
    if cliente_search:
        df = df[df["cliente_nome"].str.contains(cliente_search, case=False, na=False)]
    if situacao == "Atrasados":
        df = df[df["is_atrasado"]]
    elif situacao == "Finalizados":
        df = df[df["is_finalizado"]]
    elif situacao == "Em andamento":
        df = df[~df["is_finalizado"]]
    elif situacao == "No prazo":
        df = df[(~df["is_finalizado"]) & (~df["is_atrasado"])]

    if "valor_total" in df.columns and not df["valor_total"].empty:
        min_valor = float(df["valor_total"].min())
        max_valor = float(df["valor_total"].max())
        if max_valor > min_valor:
            faixa = st.slider(
                "Faixa de valor",
                min_value=min_valor,
                max_value=max_valor,
                value=(min_valor, max_valor),
                key="pcp_valor_range",
            )
            df = df[(df["valor_total"] >= faixa[0]) & (df["valor_total"] <= faixa[1])]

    sort_map = {
        "Criação": "data_criacao",
        "Prazo": "data_prazo_validada",
        "Valor": "valor_total",
        "Dias em atraso": "dias_em_atraso",
    }
    sort_column = sort_map.get(sort_field)
    if sort_column and sort_column in df.columns:
        df = df.sort_values(by=sort_column, ascending=not sort_desc, na_position="last")

    if df.empty:
        st.info("Nenhum pedido encontrado com os filtros dinâmicos.")
        return

    preferred_order = [
        "cliente_nome",
        "status_pedido",
        "valor_total",
        "qtde_itens",
        "data_criacao",
        "data_prazo_validada",
        "dias_em_atraso",
        "is_atrasado",
        "is_finalizado",
    ]
    preferred_config = {
        "cliente_nome": st.column_config.TextColumn("Cliente", width="large"),
        "status_pedido": st.column_config.TextColumn("Status", width="medium"),
        "valor_total": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
        "qtde_itens": st.column_config.NumberColumn("Itens", format="%d"),
        "data_criacao": st.column_config.DatetimeColumn("Criado em", format="DD/MM/YYYY"),
        "data_prazo_validada": st.column_config.DatetimeColumn("Prazo", format="DD/MM/YYYY"),
        "dias_em_atraso": st.column_config.NumberColumn("Dias Atraso", format="%d"),
        "is_atrasado": st.column_config.CheckboxColumn("Atrasado?"),
        "is_finalizado": st.column_config.CheckboxColumn("Finalizado?"),
    }
    _safe_dataframe(df, preferred_order, preferred_config, height=420)
    st.caption(f"Exibindo {len(df)} de {len(base_df)} pedidos no período.")


# =============================================================================
# VIEW: FINANCEIRO
# =============================================================================


import plotly.express as px

def render_finance_view():
    st.markdown("### 💰 Controladoria & Vendas")
    st.caption("Visão Consolidadada: Fluxo de Caixa e Vendas")

    if not is_connected():
        st.warning("⚠️ Supabase não configurado.")
        return

    st.divider()

    # --- FILTROS ---
    col_f1, col_f2 = st.columns([2, 1])
    hoje = datetime.now()
    comp_inicio_default = (hoje - timedelta(days=30)).replace(day=1)

    with col_f1:
        competencia = st.date_input(
            "Período de Análise",
            value=(comp_inicio_default, hoje),
            key="fin_competencia_hybrid",
        )
    
    comp_inicio, comp_fim = _to_date_bounds(competencia)
    snapshot = fetch_snapshot_meta()
    
    # --- FETCH DATA ---
    with st.spinner("Carregando dados..."):
        df_fin = fetch_view_data(
            view_name="vw_dashboard_financeiro",
            start_date=comp_inicio,
            end_date=comp_fim,
            date_column="competencia_mes",
            snapshot_key=snapshot.get("cache_key"),
        )
        
        df_pedidos = fetch_view_data(
            view_name="vw_dashboard_pedidos",
            start_date=comp_inicio,
            end_date=comp_fim,
            date_column="data_criacao",
            snapshot_key=snapshot.get("cache_key"),
        )

    # --- BLOCO 1: FINANCEIRO (KPIs Estáticos) ---
    st.markdown("#### 💵 Resultados Financeiros")
    
    val_receitas = 0.0
    val_despesas = 0.0
    val_saldo = 0.0

    if not df_fin.empty:
        df_fin = _normalize_financeiro_df(df_fin)
        # Clean string/category columns - RELAXED REGEX to allow Accents
        # Removing only strictly unwanted chars: %, @, $, _
        for col in ["categoria", "descricao", "fornecedor"]:
            if col in df_fin.columns:
                df_fin[col] = df_fin[col].astype(str).str.replace(r'[%@_\$]', ' ', regex=True).str.strip()

        val_receitas = df_fin[df_fin["tipo"] == "Entrada"]["valor"].sum()
        val_despesas = df_fin[df_fin["tipo"] == "Saída"]["valor"].sum()
        val_saldo = val_receitas - val_despesas
    
    # Cards Estáticos
    c1, c2, c3 = st.columns(3)
    c1.metric("Faturamento Total", format_currency(val_receitas))
    c2.metric("Despesas Totais", format_currency(val_despesas))
    c3.metric("Resultado Líquido", format_currency(val_saldo), delta=format_currency(val_saldo))

    st.markdown("<br>", unsafe_allow_html=True)

    # --- HELPER: WRAP TEXT FOR CHARTS ---
    def _wrap_text(text, width=15):
        import textwrap
        return "<br>".join(textwrap.wrap(text, width=width))

    # --- CHART: DESPESAS POR CATEGORIA (Dual View) ---
    if val_despesas > 0 and not df_fin.empty:
        st.markdown("##### 📉 Composição de Despesas")
        
        df_saidas = df_fin[df_fin["tipo"] == "Saída"].copy()
        
        # 1. Prepare Data
        df_chart = df_saidas.groupby("categoria")["valor"].sum().reset_index()
        df_chart["percent"] = (df_chart["valor"] / val_despesas) * 100
        df_chart = df_chart.sort_values(by="valor", ascending=False)
        
        # Palette sharing
        colors = px.colors.qualitative.Prism
        
        c_pie, c_bar = st.columns([1, 1])
        
        # --- DONUT CHART (Percentages) ---
        with c_pie:
            st.caption("Distribuição (%)")
            
            # Group for Donut (Top 5 + Outros)
            if len(df_chart) > 5:
                top_n = df_chart.head(5)
                outros_val = df_chart.iloc[5:]["valor"].sum()
                outros_pct = (outros_val / val_despesas) * 100
                outros_row = pd.DataFrame([{"categoria": "OUTROS", "valor": outros_val, "percent": outros_pct}])
                df_donut = pd.concat([top_n, outros_row], ignore_index=True)
            else:
                df_donut = df_chart
            
            fig_pie = px.pie(
                df_donut, 
                values="valor", 
                names="categoria",
                hole=0.5,
                color_discrete_sequence=colors,
            )
            fig_pie.update_traces(
                textposition='outside', 
                textinfo='percent',
                hovertemplate = "<b>%{label}</b><br>R$ %{value:,.2f}<br>(%{percent})"
            )
            fig_pie.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
                showlegend=True,
                legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5), # Legend at bottom
                dragmode=False,
            )
            st.plotly_chart(fig_pie, use_container_width=True, config={'displayModeBar': False})

        # --- VERTICAL BAR CHART (Values) ---
        with c_bar:
            st.caption("Valores Absolutos (R$)")
            
            # Top 8 Categories
            df_bar = df_chart.head(8).copy()
            
            # WRAP LABELS for legibility on vertical chart
            df_bar["wrapped_label"] = df_bar["categoria"].apply(lambda x: _wrap_text(x, width=15))
            
            fig_bar = px.bar(
                df_bar,
                x="wrapped_label", # Use wrapped text
                y="valor",
                text_auto='.2s',
                title="",
                color="categoria", # Sync colors
                color_discrete_sequence=colors,
            )
            
            fig_bar.update_traces(
                textposition='outside',
                hovertemplate = "<b>%{customdata}</b><br>R$ %{y:,.2f}",
                customdata = df_bar[["categoria"]], # Show full name on hover
                showlegend=False
            )
            
            fig_bar.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                height=450, # More vertical space
                margin=dict(l=0, r=0, t=20, b=50), # Bottom margin for labels
                showlegend=False,
                dragmode=False,
                xaxis=dict(
                    showgrid=False, 
                    tickangle=-45, # Slant labels
                    automargin=True, # Prevent cutoff
                ),
                yaxis=dict(showgrid=True, gridcolor='#333', visible=True),
            )
            
            st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})

    st.divider()

    # --- BLOCO 2: VENDAS (Clientes - Vertical Bar) ---
    st.markdown("#### 🏆 Top Clientes")

    if not df_pedidos.empty:
        # Clean client names
        if "cliente_nome" in df_pedidos.columns:
             df_pedidos["cliente_nome"] = df_pedidos["cliente_nome"].astype(str).str.replace(r'[%@_\$]', ' ', regex=True).str.title()
        
        # WRAP LABELS (No excessive truncation)
        df_pedidos["wrapped_name"] = df_pedidos["cliente_nome"].apply(lambda x: _wrap_text(x, width=15))

        top_clientes = df_pedidos.groupby(["cliente_nome", "wrapped_name"])["valor_total"].sum().reset_index()
        top_clientes = top_clientes.sort_values(by="valor_total", ascending=False).head(10)
        
        # --- INSIGHT: CONCENTRAÇÃO DE RECEITA ---
        if val_receitas > 0:
            top_10_total = top_clientes["valor_total"].sum()
            representatividade = (top_10_total / val_receitas) * 100
            
            st.info(f"💡 **Insight:** Os 10 maiores clientes representam **{representatividade:.1f}%** (R$ {top_10_total:,.2f}) do faturamento total do período.")

        fig_cli = px.bar(
            top_clientes,
            x="wrapped_name", # Short name on Axis
            y="valor_total",
            text_auto='.2s',
            title="",
        )
        
        fig_cli.update_traces(
            marker_color='#8e44ad', # Deep Purple
            textposition='outside',
            hovertemplate = "<b>%{customdata}</b><br>R$ %{y:,.2f}",
            customdata = top_clientes[["cliente_nome"]] # Full name on hover
        )
        
        fig_cli.update_layout(
            xaxis_title=None,
            yaxis_title="Total Comprado (R$)",
            height=450, # Increased height
            margin=dict(l=0, r=0, t=10, b=80), # Large bottom margin
            showlegend=False,
            dragmode=False,
            xaxis=dict(
                tickangle=-45,
                automargin=True
            )
        )
        
        st.plotly_chart(fig_cli, use_container_width=True, config={'displayModeBar': False})
    
    else:
        st.info("Sem dados de vendas para o período.")

    if not df_pedidos.empty:
        st.divider()
        st.markdown("##### 📦 Maiores Pedidos")
        
        # Clean client name in orders too if strictly needed, but fetch_view usually has standard cols.
        # df_pedidos already has 'cliente_nome' cleaned above if we used it.
        
        top_pedidos_table = (
            df_pedidos[["cliente_nome", "data_criacao", "valor_total", "qtde_itens", "status_pedido"]]
            .sort_values(by="valor_total", ascending=False)
            .head(10)
        )
        
        st.dataframe(
            top_pedidos_table,
            column_config={
                "cliente_nome": st.column_config.TextColumn("Cliente", width="large"),
                "data_criacao": st.column_config.DatetimeColumn("Data", format="DD/MM/YYYY"),
                "valor_total": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
                "qtde_itens": st.column_config.NumberColumn("Itens"),
                "status_pedido": st.column_config.TextColumn("Status"),
            },
            hide_index=True,
            use_container_width=True
        )





# =============================================================================
# VIEW: CHAT
# =============================================================================


def render_chat_view():
    if "messages" not in st.session_state:
        st.session_state.messages = []

    if not st.session_state.messages:
        st.markdown(
            '<div class="hero-container"><div class="hero-title">Como posso ajudar?</div></div>',
            unsafe_allow_html=True,
        )
        c1, c2, c3 = st.columns(3)
        if c1.button("📦 Meus Pedidos", use_container_width=True):
            st.session_state.pending_prompt = "Status dos meus pedidos"
            st.rerun()
        if c2.button("💰 Faturamento", use_container_width=True):
            st.session_state.pending_prompt = "Resumo financeiro do mes"
            st.rerun()
        if c3.button("📊 Relatorios", use_container_width=True):
            st.session_state.pending_prompt = "Gerar relatorio operacional"
            st.rerun()
    else:
        for message in st.session_state.messages:
            role = "user" if message["role"] == "user" else "assistant"
            with st.chat_message(role):
                st.markdown(message["content"])

    user_input = st.chat_input("Digite sua mensagem...")
    prompt_to_process = None
    if user_input:
        prompt_to_process = user_input
    elif st.session_state.get("pending_prompt"):
        prompt_to_process = st.session_state.pending_prompt
        st.session_state.pending_prompt = None

    if prompt_to_process:
        st.session_state.messages.append({"role": "user", "content": prompt_to_process})
        with st.chat_message("user"):
            st.markdown(prompt_to_process)

        try:
            with st.status("🚀 Consultando Base de Dados...", expanded=True) as status:
                time.sleep(0.5)
                status.write("🔍 Interpretando solicitacao...")
                history = st.session_state.messages[:-1]
                response = send_message_to_n8n(prompt_to_process, history)
                if not response:
                    response = "Sem resposta."
                status.update(label="✅ Resposta Gerada", state="complete", expanded=False)

            st.session_state.messages.append({"role": "assistant", "content": response})
            with st.chat_message("assistant"):
                st.markdown(response)
        except Exception as exc:
            st.error(f"Erro ao conectar com o assistente: {exc}")


# =============================================================================
# MAIN
# =============================================================================


def main():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "current_view" not in st.session_state:
        st.session_state.current_view = "Chat"

    with st.sidebar:
        st.title("🎨 NBL Admin")
        if is_connected():
            st.caption("v6.5 • 🟢 Supabase Conectado")
            meta = fetch_snapshot_meta()
            if meta.get("snapshot_finished_at"):
                st.caption(f"Snapshot: {_format_iso_dt(meta.get('snapshot_finished_at'))}")
            if meta.get("is_running"):
                st.caption("⏳ Atualização em andamento")
        else:
            st.caption("v6.5 • 🔴 Supabase Offline")

        st.divider()
        menu = {
            "💬 Chat": "Chat",
            "🏭 Status (PCP)": "Status",
            "💰 Financeiro": "Financeiro",
            "ℹ️ Instrucoes": "Instrucoes",
        }
        for label, view_name in menu.items():
            if st.button(
                label,
                use_container_width=True,
                type="primary" if st.session_state.current_view == view_name else "secondary",
            ):
                st.session_state.current_view = view_name
                st.rerun()

        st.divider()
        st.caption("Desenvolvido por\n**Golfine Tecnologia**")
        if st.button("Limpar Chat"):
            st.session_state.messages = []
            st.rerun()

    if st.session_state.current_view == "Chat":
        render_chat_view()
    elif st.session_state.current_view == "Status":
        render_status_view()
    elif st.session_state.current_view == "Financeiro":
        render_finance_view()
    elif st.session_state.current_view == "Instrucoes":
        render_instructions()


if __name__ == "__main__":
    main()
