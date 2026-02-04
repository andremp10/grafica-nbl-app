import streamlit as st
import html
import os
import time
from datetime import datetime, timedelta
import random
from dotenv import load_dotenv
from services.n8n_service import get_webhook_url, probe_webhook, send_message_to_n8n

# --- 0. CONFIGURAÇÃO INICIAL ---
load_dotenv()

st.set_page_config(
    page_title="Gráfica NBL Admin",
    page_icon="🎨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 1. CSS MINIMALISTA E MODERNO ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Reset e Base */
    .stApp {
        background: #0f0f0f;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Esconder elementos padrão */
    #MainMenu, footer, header {visibility: hidden;}
    .block-container {padding: 1rem 2rem 6rem 2rem; max-width: 1200px;}
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #161616;
        border-right: 1px solid #262626;
    }
    section[data-testid="stSidebar"] .block-container {padding: 1.5rem 1rem;}
    
    /* Tipografia */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        color: #ffffff;
        letter-spacing: -0.02em;
    }
    h1 {font-size: 2rem; margin-bottom: 0.5rem;}
    h2 {font-size: 1.5rem; color: #e0e0e0;}
    h3 {font-size: 1.1rem; color: #a0a0a0;}
    p, div, label, span {color: #b0b0b0; font-family: 'Inter', sans-serif;}
    
    /* Hero Simples */
    .hero-section {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2a2a4a;
        border-radius: 16px;
        padding: 3rem 2rem;
        text-align: center;
        margin-bottom: 2rem;
    }
    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0;
    }
    .hero-subtitle {color: #8888aa; font-size: 1rem; margin-top: 0.5rem;}
    
    /* Cards de Métricas */
    .metric-card {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
        transition: all 0.2s ease;
    }
    .metric-card:hover {
        border-color: #3a3a3a;
        transform: translateY(-2px);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #ffffff;
    }
    .metric-label {
        font-size: 0.75rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 0.25rem;
    }
    .metric-delta {font-size: 0.85rem; margin-top: 0.5rem;}
    .delta-positive {color: #10b981;}
    .delta-negative {color: #ef4444;}
    
    /* Mensagens do Chat */
    .chat-msg {
        padding: 1rem 1.25rem;
        border-radius: 12px;
        margin-bottom: 0.75rem;
        max-width: 80%;
        line-height: 1.5;
        font-size: 0.95rem;
    }
    .user-msg {
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        margin-left: auto;
        border-bottom-right-radius: 4px;
    }
    .ai-msg {
        background: #1e1e1e;
        border: 1px solid #2a2a2a;
        color: #e0e0e0;
        margin-right: auto;
        border-bottom-left-radius: 4px;
    }
    
    /* Quick Actions */
    .stButton > button {
        background: #1a1a1a;
        border: 1px solid #333;
        color: #e0e0e0;
        border-radius: 8px;
        padding: 0.75rem 1rem;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        background: #252525;
        border-color: #667eea;
        color: white;
    }
    
    /* Input do Chat */
    .stChatInput {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        background: rgba(15, 15, 15, 0.95);
        backdrop-filter: blur(10px);
        border-top: 1px solid #262626;
        padding: 1rem;
    }
    .stChatInput > div {max-width: 800px; margin: 0 auto;}
    .stChatInput input {
        background: #1a1a1a !important;
        border: 1px solid #333 !important;
        border-radius: 12px !important;
        color: white !important;
        padding: 1rem !important;
    }
    .stChatInput input:focus {
        border-color: #667eea !important;
        box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.2) !important;
    }
    
    /* Tabela */
    .dataframe {background: #1a1a1a !important;}
    
    /* Status Badge */
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .status-ok {background: rgba(16, 185, 129, 0.15); color: #10b981;}
    .status-warn {background: rgba(245, 158, 11, 0.15); color: #f59e0b;}
    .status-error {background: rgba(239, 68, 68, 0.15); color: #ef4444;}
</style>
""", unsafe_allow_html=True)

# --- 2. STATE MANAGEMENT (Otimizado) ---
def init_state():
    defaults = {
        "messages": [],
        "is_processing": False,
        "current_page": "chat",
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

init_state()

# --- 3. HELPER FUNCTIONS ---
def escape_html(text: str) -> str:
    return html.escape(text or "").replace("\n", "<br>")

def format_currency(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# --- 4. MOCK DATA (Realista para Gráfica) ---
MOCK_ORDERS = [
    {"id": "#2401", "cliente": "Restaurante Sabor & Arte", "produto": "500 Cardápios A4", "status": "✅ Entregue", "valor": 890.00, "data": "03/02/2026"},
    {"id": "#2402", "cliente": "Imobiliária Central", "produto": "1000 Folders Triplos", "status": "🔄 Produção", "valor": 1450.00, "data": "03/02/2026"},
    {"id": "#2403", "cliente": "Clínica Bem Estar", "produto": "200 Cartões de Visita", "status": "⏳ Aguardando Arte", "valor": 180.00, "data": "02/02/2026"},
    {"id": "#2404", "cliente": "Loja Fashion Style", "produto": "50 Banners 60x90", "status": "🔄 Produção", "valor": 2100.00, "data": "02/02/2026"},
    {"id": "#2405", "cliente": "Escritório Contábil ABC", "produto": "2000 Envelopes Timbrados", "status": "✅ Entregue", "valor": 680.00, "data": "01/02/2026"},
    {"id": "#2406", "cliente": "Padaria Pão Quente", "produto": "300 Adesivos Personalizados", "status": "📦 Pronto p/ Retirada", "valor": 420.00, "data": "01/02/2026"},
]

MOCK_METRICS = {
    "pedidos_hoje": 14,
    "pedidos_delta": "+3",
    "receita_dia": 4280.00,
    "receita_delta": "+12%",
    "fila_producao": 8,
    "fila_delta": "-2",
    "ticket_medio": 305.00,
    "ticket_delta": "+5%",
}

# --- 5. SIDEBAR ---
with st.sidebar:
    st.markdown("## 🎨 NBL Admin")
    st.caption("v3.0 • Streamlit Cloud")
    st.markdown("---")
    
    # Menu
    menu_items = {
        "💬 Assistente IA": "chat",
        "📊 Dashboard": "dashboard",
        "📝 Pedidos": "pedidos",
        "⚙️ Configurações": "config",
    }
    
    for label, page in menu_items.items():
        if st.button(label, use_container_width=True, type="secondary" if st.session_state.current_page != page else "primary"):
            st.session_state.current_page = page
    
    st.markdown("---")
    
    # Status
    st.markdown("##### Status")
    webhook_url = get_webhook_url()
    if webhook_url:
        st.markdown('<span class="status-badge status-ok">● Webhook OK</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-badge status-error">● Webhook OFF</span>', unsafe_allow_html=True)
    
    supabase_url = os.getenv("SUPABASE_URL")
    if supabase_url:
        st.markdown('<span class="status-badge status-ok">● Supabase OK</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-badge status-warn">● Supabase N/C</span>', unsafe_allow_html=True)
    
    st.markdown("---")
    if st.button("🗑️ Limpar Chat", use_container_width=True):
        st.session_state.messages = []

# --- 6. PÁGINAS ---

# === CHAT ===
if st.session_state.current_page == "chat":
    # Hero (só quando vazio)
    if not st.session_state.messages:
        st.markdown("""
        <div class="hero-section">
            <h1 class="hero-title">Assistente Gráfica NBL</h1>
            <p class="hero-subtitle">Como posso ajudar você hoje?</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Quick Actions
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📦 Status de Pedidos", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": "Qual o status dos pedidos em aberto?"})
        with col2:
            if st.button("💰 Faturamento do Mês", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": "Como está o faturamento do mês?"})
        with col3:
            if st.button("📊 Relatório Geral", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": "Gere um relatório geral do sistema"})
    
    # Container de mensagens
    chat_container = st.container()
    with chat_container:
        for msg in st.session_state.messages:
            css_class = "user-msg" if msg["role"] == "user" else "ai-msg"
            content_html = escape_html(msg.get("content", ""))
            st.markdown(f'<div class="chat-msg {css_class}">{content_html}</div>', unsafe_allow_html=True)
    
    # Input
    if prompt := st.chat_input("Digite sua mensagem..."):
        # Adiciona mensagem do usuário
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Processa resposta
        with st.spinner("Processando..."):
            history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1]]
            response = send_message_to_n8n(prompt, history)
        
        # Adiciona resposta
        st.session_state.messages.append({
            "role": "assistant", 
            "content": response or "Desculpe, não consegui processar sua mensagem."
        })
        st.rerun()

# === DASHBOARD ===
elif st.session_state.current_page == "dashboard":
    st.markdown("# 📊 Dashboard")
    st.caption("Visão geral do dia • Dados de demonstração")
    
    # Métricas
    col1, col2, col3, col4 = st.columns(4)
    
    def render_metric(label, value, delta, is_currency=False):
        val_str = format_currency(value) if is_currency else str(value)
        delta_class = "delta-positive" if delta.startswith("+") else "delta-negative"
        return f"""
        <div class="metric-card">
            <div class="metric-value">{val_str}</div>
            <div class="metric-label">{label}</div>
            <div class="metric-delta {delta_class}">{delta}</div>
        </div>
        """
    
    with col1:
        st.markdown(render_metric("Pedidos Hoje", MOCK_METRICS["pedidos_hoje"], MOCK_METRICS["pedidos_delta"]), unsafe_allow_html=True)
    with col2:
        st.markdown(render_metric("Receita (Dia)", MOCK_METRICS["receita_dia"], MOCK_METRICS["receita_delta"], True), unsafe_allow_html=True)
    with col3:
        st.markdown(render_metric("Fila Produção", MOCK_METRICS["fila_producao"], MOCK_METRICS["fila_delta"]), unsafe_allow_html=True)
    with col4:
        st.markdown(render_metric("Ticket Médio", MOCK_METRICS["ticket_medio"], MOCK_METRICS["ticket_delta"], True), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Gráfico simples
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📈 Receita Semanal")
        chart_data = {
            "Seg": 3200, "Ter": 4100, "Qua": 3800, 
            "Qui": 4500, "Sex": 5200, "Sáb": 2800
        }
        st.bar_chart(chart_data, color="#667eea")
    
    with col2:
        st.markdown("### 🎯 Status Produção")
        st.markdown("""
        - **Em Produção:** 8 pedidos
        - **Aguardando Arte:** 3 pedidos  
        - **Prontos p/ Retirada:** 5 pedidos
        - **Entregues Hoje:** 6 pedidos
        """)

# === PEDIDOS ===
elif st.session_state.current_page == "pedidos":
    st.markdown("# 📝 Pedidos")
    st.caption("Gestão de pedidos • Dados de demonstração")
    
    # Filtros
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        search = st.text_input("🔍 Buscar cliente ou pedido", placeholder="Digite para buscar...")
    with col2:
        status_filter = st.selectbox("Status", ["Todos", "✅ Entregue", "🔄 Produção", "⏳ Aguardando Arte", "📦 Pronto p/ Retirada"])
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("➕ Novo Pedido", use_container_width=True):
            st.info("Funcionalidade em desenvolvimento")
    
    # Tabela
    import pandas as pd
    df = pd.DataFrame(MOCK_ORDERS)
    
    # Aplicar filtros
    if search:
        df = df[df["cliente"].str.contains(search, case=False) | df["id"].str.contains(search, case=False)]
    if status_filter != "Todos":
        df = df[df["status"] == status_filter]
    
    # Formatar valores
    df["valor"] = df["valor"].apply(lambda x: format_currency(x))
    df.columns = ["ID", "Cliente", "Produto", "Status", "Valor", "Data"]
    
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    # Resumo
    total = sum(o["valor"] for o in MOCK_ORDERS)
    st.markdown(f"**Total em pedidos:** {format_currency(total)} • **{len(MOCK_ORDERS)} pedidos**")

# === CONFIGURAÇÕES ===
elif st.session_state.current_page == "config":
    st.markdown("# ⚙️ Configurações")
    
    with st.expander("🔗 Integrações", expanded=True):
        col1, col2 = st.columns([3, 1])
        with col1:
            webhook = get_webhook_url() or ""
            masked = webhook[:20] + "..." + webhook[-10:] if len(webhook) > 35 else webhook
            st.text_input("Webhook N8N", value=masked if webhook else "Não configurado", disabled=True)
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Testar", use_container_width=True):
                ok, msg = probe_webhook()
                if ok:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"❌ {msg}")
    
    with st.expander("📊 Supabase"):
        supabase_url = os.getenv("SUPABASE_URL") or ""
        st.text_input("URL", value=supabase_url[:30] + "..." if len(supabase_url) > 30 else supabase_url or "Não configurado", disabled=True)
        st.caption("Configure via Secrets no Streamlit Cloud")
    
    with st.expander("ℹ️ Sobre"):
        st.markdown("""
        **Gráfica NBL Admin** v3.0
        
        Desenvolvido para gestão de pedidos e atendimento ao cliente.
        
        - Assistente IA via N8N
        - Dashboard de métricas
        - Gestão de pedidos
        """)
