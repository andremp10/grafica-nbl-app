import time
from datetime import datetime, timedelta
from typing import Optional, Sequence

import streamlit as st
from dotenv import load_dotenv

from data.supabase_client import is_connected
from data.supabase_repo import (
    fetch_financeiro,
    fetch_kpis_financeiro,
    fetch_kpis_pedidos,
    fetch_pedidos,
    fetch_snapshot_meta,
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
    value_html = format_currency(value) if isinstance(value, (int, float)) else str(value)
    return (
        f'<div class="metric-box"><div class="metric-lbl">{label}</div>'
        f'<div class="metric-val">{value_html}</div>{delta_html}</div>'
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

    st.markdown("#### 🧱 Migrações SQL (1x)")
    st.markdown(
        "\n".join(
            [
                "Execute no Supabase SQL Editor (na ordem):",
                "",
                "1. `etl/migrations/001_create_dashboard_views.sql`",
                "2. `etl/migrations/002_dashboard_views_rpc_grants.sql`",
                "3. `etl/migrations/003_snapshot_meta.sql`",
            ]
        )
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

    col_f1, col_f2, col_f3, col_f4 = st.columns([2, 2, 1, 1])
    with col_f1:
        data_range = st.date_input(
            "Periodo",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            key="pcp_date_range",
        )
    with col_f2:
        cliente_search = st.text_input("🔍 Buscar Cliente", key="pcp_cliente_search")
    with col_f3:
        status_filter = st.text_input("Status (contem)", key="pcp_status_filter")
    with col_f4:
        show_atrasados = st.checkbox("Apenas Atrasados", key="pcp_atrasados")
        show_finalizados = st.checkbox("Apenas Finalizados", key="pcp_finalizados")

    data_inicio, data_fim = _to_date_bounds(data_range)

    with st.spinner("Carregando KPIs..."):
        kpis = fetch_kpis_pedidos(data_inicio, data_fim, snapshot_key=snapshot_key)

    total = int(kpis.get("total_pedidos", 0))
    atrasados = int(kpis.get("total_atrasados", 0))
    finalizados = int(kpis.get("total_finalizados", 0))
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

    with st.spinner("Carregando pedidos..."):
        df = fetch_pedidos(
            start_date=data_inicio,
            end_date=data_fim,
            status=status_filter or None,
            is_atrasado=True if show_atrasados else None,
            is_finalizado=True if show_finalizados else None,
            client_name=cliente_search or None,
            page_size=500,
            snapshot_key=snapshot_key,
        )

    if df.empty:
        st.info("Nenhum pedido encontrado com os filtros aplicados.")
        return

    preferred_order = [
        "pedido_id",
        "cliente_nome",
        "status_pedido",
        "valor_total",
        "data_prazo_validada",
        "dias_em_atraso",
        "is_atrasado",
        "is_finalizado",
    ]
    preferred_config = {
        "pedido_id": st.column_config.TextColumn("Pedido", width="small"),
        "cliente_nome": st.column_config.TextColumn("Cliente", width="large"),
        "status_pedido": st.column_config.TextColumn("Status", width="medium"),
        "valor_total": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
        "data_prazo_validada": st.column_config.DatetimeColumn("Prazo", format="DD/MM/YYYY"),
        "dias_em_atraso": st.column_config.NumberColumn("Dias Atraso", format="%d"),
        "is_atrasado": st.column_config.CheckboxColumn("Atrasado?"),
        "is_finalizado": st.column_config.CheckboxColumn("Finalizado?"),
    }
    _safe_dataframe(df, preferred_order, preferred_config, height=420)
    st.caption(f"Exibindo {len(df)} pedidos (maximo 500 por consulta).")


# =============================================================================
# VIEW: FINANCEIRO
# =============================================================================


def render_finance_view():
    st.markdown("### 💰 Controladoria Financeira")
    st.caption("Snapshot diário do financeiro (atualizado 1x por dia) - dados do Supabase")

    if not is_connected():
        st.warning("⚠️ Supabase não configurado.")
        return

    st.divider()

    snapshot = fetch_snapshot_meta()
    snapshot_key = snapshot.get("cache_key")
    if snapshot.get("snapshot_finished_at"):
        st.caption(f"Última atualização: `{_format_iso_dt(snapshot.get('snapshot_finished_at'))}`")

    col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
    hoje = datetime.now()
    comp_inicio_default = (hoje - timedelta(days=365)).replace(day=1)

    with col_f1:
        competencia = st.date_input(
            "Competencia",
            value=(comp_inicio_default, hoje),
            key="fin_competencia",
        )
    with col_f2:
        tipo_filter = st.selectbox("Tipo", ["Todos", "Entrada", "Saída"], key="fin_tipo")
        status_filter = st.selectbox("Status", ["Todos", "Aberto", "Pago"], key="fin_status")

    comp_inicio, comp_fim = _to_date_bounds(competencia)

    base_df = fetch_financeiro(
        start_date=comp_inicio,
        end_date=comp_fim,
        page_size=500,
        snapshot_key=snapshot_key,
    )
    categories = ["Todas"]
    if not base_df.empty and "categoria" in base_df.columns:
        values = sorted({str(value) for value in base_df["categoria"].dropna().tolist() if str(value)})
        categories.extend(values)
    with col_f3:
        cat_filter = st.selectbox("Categoria", categories, key="fin_categoria")

    with st.spinner("Carregando KPIs..."):
        kpis = fetch_kpis_financeiro(comp_inicio, comp_fim, snapshot_key=snapshot_key)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card("Entradas", kpis.get("entradas", 0.0), color="up"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card("Saidas", kpis.get("saidas", 0.0), color="down"), unsafe_allow_html=True)
    with c3:
        saldo = float(kpis.get("saldo", 0.0))
        st.markdown(kpi_card("Saldo", saldo, color="up" if saldo >= 0 else "down"), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card("Registros", int(kpis.get("count", 0))), unsafe_allow_html=True)

    st.divider()

    with st.spinner("Carregando lancamentos..."):
        status_code = None
        if status_filter == "Aberto":
            status_code = 1
        elif status_filter == "Pago":
            status_code = 2

        df = fetch_financeiro(
            start_date=comp_inicio,
            end_date=comp_fim,
            tipo=tipo_filter if tipo_filter != "Todos" else None,
            status_codigo=status_code,
            categoria=cat_filter if cat_filter != "Todas" else None,
            page_size=500,
            snapshot_key=snapshot_key,
        )

    if df.empty:
        st.info("Nenhum lancamento encontrado para os filtros aplicados.")
        return

    if {"competencia_mes", "tipo", "valor"}.issubset(df.columns):
        df_chart = df.groupby(["competencia_mes", "tipo"], dropna=False)["valor"].sum().unstack(fill_value=0)
        st.bar_chart(df_chart, height=300)

    st.markdown("#### 🧾 Extrato de Lancamentos")
    preferred_order = [
        "lancamento_id",
        "descricao",
        "tipo",
        "valor",
        "data_vencimento",
        "status_texto",
        "categoria",
    ]
    preferred_config = {
        "lancamento_id": st.column_config.TextColumn("ID", width="small"),
        "descricao": st.column_config.TextColumn("Descricao", width="large"),
        "tipo": st.column_config.TextColumn("Tipo", width="small"),
        "valor": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
        "data_vencimento": st.column_config.DatetimeColumn("Vencimento", format="DD/MM/YYYY"),
        "status_texto": st.column_config.TextColumn("Status"),
        "categoria": st.column_config.TextColumn("Categoria"),
    }
    _safe_dataframe(df, preferred_order, preferred_config, height=360)


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
            st.caption("v6.3 • 🟢 Supabase Conectado")
            meta = fetch_snapshot_meta()
            if meta.get("snapshot_finished_at"):
                st.caption(f"Snapshot: {_format_iso_dt(meta.get('snapshot_finished_at'))}")
            if meta.get("is_running"):
                st.caption("⏳ Atualização em andamento")
        else:
            st.caption("v6.3 • 🔴 Supabase Offline")

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
