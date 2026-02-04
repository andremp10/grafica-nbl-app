import streamlit as st
import html
import os
import time
from datetime import datetime
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

# --- 1. CSS PREMIUM ESTILO CHATGPT ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Base */
    .stApp {
        background: #0f0f0f;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    #MainMenu, footer, header {visibility: hidden;}
    .block-container {padding: 1rem 2rem 8rem 2rem; max-width: 900px;}
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #171717;
        border-right: 1px solid #262626;
    }
    section[data-testid="stSidebar"] .block-container {padding: 1.5rem 1rem;}
    
    /* Tipografia */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        color: #ffffff;
        letter-spacing: -0.02em;
    }
    h1 {font-size: 1.75rem;}
    h2 {font-size: 1.25rem; color: #e0e0e0;}
    p, div, label, span {color: #a0a0a0; font-family: 'Inter', sans-serif;}
    
    /* Hero Clean */
    .hero-clean {
        text-align: center;
        padding: 4rem 2rem 2rem;
    }
    .hero-title {
        font-size: 2rem;
        font-weight: 600;
        color: #ffffff;
        margin-bottom: 0.5rem;
    }
    .hero-subtitle {
        color: #666;
        font-size: 1rem;
    }
    
    /* Quick Actions Grid */
    .quick-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 0.75rem;
        max-width: 600px;
        margin: 2rem auto;
    }
    .quick-btn {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        cursor: pointer;
        transition: all 0.2s ease;
        color: #d0d0d0;
        font-size: 0.9rem;
    }
    .quick-btn:hover {
        background: #252525;
        border-color: #404040;
    }
    
    /* Chat Messages */
    .msg-container {
        max-width: 800px;
        margin: 0 auto;
        padding-bottom: 2rem;
    }
    .chat-msg {
        padding: 1rem 1.25rem;
        border-radius: 16px;
        margin-bottom: 1rem;
        line-height: 1.6;
        font-size: 0.95rem;
        max-width: 85%;
    }
    .user-msg {
        background: #2563eb;
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
    .ai-msg .ai-icon {
        display: inline-block;
        width: 24px;
        height: 24px;
        background: linear-gradient(135deg, #667eea, #764ba2);
        border-radius: 6px;
        margin-right: 8px;
        vertical-align: middle;
        text-align: center;
        line-height: 24px;
        font-size: 12px;
    }
    
    /* Loading Animation */
    .loading-container {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 1rem;
        background: #1e1e1e;
        border: 1px solid #2a2a2a;
        border-radius: 16px;
        max-width: 400px;
        margin-bottom: 1rem;
    }
    .loading-dots {
        display: flex;
        gap: 4px;
    }
    .loading-dots span {
        width: 8px;
        height: 8px;
        background: #667eea;
        border-radius: 50%;
        animation: pulse 1.4s infinite ease-in-out;
    }
    .loading-dots span:nth-child(1) {animation-delay: 0s;}
    .loading-dots span:nth-child(2) {animation-delay: 0.2s;}
    .loading-dots span:nth-child(3) {animation-delay: 0.4s;}
    @keyframes pulse {
        0%, 80%, 100% {transform: scale(0.6); opacity: 0.5;}
        40% {transform: scale(1); opacity: 1;}
    }
    .loading-text {
        color: #888;
        font-size: 0.9rem;
    }
    
    /* Input Premium - Estilo ChatGPT */
    .stChatInput {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        background: linear-gradient(to top, #0f0f0f 80%, transparent);
        padding: 1.5rem;
        padding-top: 3rem;
    }
    .stChatInput > div {
        max-width: 800px;
        margin: 0 auto;
        background: #1a1a1a;
        border: 1px solid #333;
        border-radius: 24px;
        overflow: hidden;
    }
    .stChatInput input, .stChatInput textarea {
        background: transparent !important;
        border: none !important;
        color: white !important;
        padding: 1rem 1.5rem !important;
        font-size: 1rem !important;
        box-shadow: none !important;
    }
    .stChatInput input:focus, .stChatInput textarea:focus {
        box-shadow: none !important;
        border: none !important;
    }
    .stChatInput button {
        background: #2563eb !important;
        border: none !important;
        border-radius: 50% !important;
        width: 36px !important;
        height: 36px !important;
        margin: 8px !important;
    }
    
    /* Metric Cards */
    .metric-card {
        background: #1a1a1a;
        border: 1px solid #262626;
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
    }
    .metric-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: #ffffff;
    }
    .metric-label {
        font-size: 0.7rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 0.25rem;
    }
    .metric-delta {font-size: 0.8rem; margin-top: 0.25rem;}
    .delta-positive {color: #10b981;}
    .delta-negative {color: #ef4444;}
    
    /* Status Badge */
    .status-badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 12px;
        font-size: 0.7rem;
        font-weight: 600;
    }
    .status-ok {background: rgba(16, 185, 129, 0.15); color: #10b981;}
    .status-warn {background: rgba(245, 158, 11, 0.15); color: #f59e0b;}
    
    /* Buttons */
    .stButton > button {
        background: #1a1a1a;
        border: 1px solid #333;
        color: #e0e0e0;
        border-radius: 10px;
        padding: 0.6rem 1rem;
        font-weight: 500;
        transition: all 0.15s ease;
    }
    .stButton > button:hover {
        background: #252525;
        border-color: #444;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. STATE MANAGEMENT ---
def init_state():
    defaults = {
        "messages": [],
        "current_page": "chat",
        "pending_prompt": None,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

init_state()

# --- 3. HELPERS ---
def escape_html(text: str) -> str:
    return html.escape(text or "").replace("\n", "<br>")

def format_currency(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# --- 4. FRASES DE LOADING ANIMADAS ---
LOADING_PHRASES = [
    "🔍 Analisando sua solicitação...",
    "🧠 Processando com IA...",
    "📊 Consultando dados do sistema...",
    "⚙️ Preparando resposta...",
    "✨ Quase pronto...",
    "🔄 Finalizando análise...",
]

# --- 5. MOCK DATA ---
MOCK_ORDERS = [
    {"id": "#2401", "cliente": "Restaurante Sabor & Arte", "produto": "500 Cardápios A4", "status": "✅ Entregue", "valor": 890.00, "data": "03/02"},
    {"id": "#2402", "cliente": "Imobiliária Central", "produto": "1000 Folders Triplos", "status": "🔄 Produção", "valor": 1450.00, "data": "03/02"},
    {"id": "#2403", "cliente": "Clínica Bem Estar", "produto": "200 Cartões de Visita", "status": "⏳ Arte", "valor": 180.00, "data": "02/02"},
    {"id": "#2404", "cliente": "Loja Fashion Style", "produto": "50 Banners 60x90", "status": "🔄 Produção", "valor": 2100.00, "data": "02/02"},
    {"id": "#2405", "cliente": "Escritório ABC", "produto": "2000 Envelopes", "status": "✅ Entregue", "valor": 680.00, "data": "01/02"},
    {"id": "#2406", "cliente": "Padaria Pão Quente", "produto": "300 Adesivos", "status": "📦 Retirada", "valor": 420.00, "data": "01/02"},
]

MOCK_METRICS = {
    "pedidos_hoje": 14, "pedidos_delta": "+3",
    "receita_dia": 4280.00, "receita_delta": "+12%",
    "fila_producao": 8, "fila_delta": "-2",
    "ticket_medio": 305.00, "ticket_delta": "+5%",
}

# --- 6. SIDEBAR ---
with st.sidebar:
    st.markdown("## 🎨 NBL Admin")
    st.caption("v3.1 • Streamlit Cloud")
    st.markdown("---")
    
    menu = {"💬 Assistente": "chat", "📊 Dashboard": "dashboard", "📝 Pedidos": "pedidos", "⚙️ Config": "config"}
    for label, page in menu.items():
        btn_type = "primary" if st.session_state.current_page == page else "secondary"
        if st.button(label, use_container_width=True, type=btn_type):
            st.session_state.current_page = page
            st.rerun()
    
    st.markdown("---")
    st.markdown("##### Status")
    if get_webhook_url():
        st.markdown('<span class="status-badge status-ok">● Webhook OK</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-badge status-warn">● Webhook N/C</span>', unsafe_allow_html=True)
    
    st.markdown("---")
    if st.button("🗑️ Limpar Chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# --- 7. PROCESSAR PROMPT PENDENTE ---
def process_pending():
    if st.session_state.pending_prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        
        # Adiciona mensagem do usuário
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Placeholder para loading animado
        loading_placeholder = st.empty()
        
        # Simular loading com frases rotativas
        start_time = time.time()
        phrase_idx = 0
        
        # Usar thread para não bloquear? Não, vamos fazer polling simulado
        # Na verdade, Streamlit não permite isso bem. Vamos mostrar uma frase e processar
        loading_placeholder.markdown(f"""
        <div class="loading-container">
            <div class="loading-dots"><span></span><span></span><span></span></div>
            <span class="loading-text">{random.choice(LOADING_PHRASES)}</span>
        </div>
        """, unsafe_allow_html=True)
        
        # Processar
        history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1]]
        response = send_message_to_n8n(prompt, history)
        
        loading_placeholder.empty()
        
        st.session_state.messages.append({
            "role": "assistant",
            "content": response or "Desculpe, não consegui processar sua solicitação."
        })
        st.rerun()

# --- 8. PÁGINAS ---

# === CHAT ===
if st.session_state.current_page == "chat":
    # Processar prompt pendente primeiro
    process_pending()
    
    # Hero (só quando vazio)
    if not st.session_state.messages:
        st.markdown("""
        <div class="hero-clean">
            <div class="hero-title">Como posso ajudar?</div>
            <div class="hero-subtitle">Assistente inteligente da Gráfica NBL</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Quick Actions como botões reais
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📦 Status de Pedidos", use_container_width=True, key="q1"):
                st.session_state.pending_prompt = "Qual o status dos pedidos em aberto?"
                st.rerun()
        with col2:
            if st.button("💰 Faturamento", use_container_width=True, key="q2"):
                st.session_state.pending_prompt = "Como está o faturamento do mês?"
                st.rerun()
        with col3:
            if st.button("📊 Relatório Geral", use_container_width=True, key="q3"):
                st.session_state.pending_prompt = "Gere um relatório geral do sistema"
                st.rerun()
    
    # Mensagens
    st.markdown('<div class="msg-container">', unsafe_allow_html=True)
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(f'<div class="chat-msg user-msg">{escape_html(msg["content"])}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'''
            <div class="chat-msg ai-msg">
                <span class="ai-icon">🤖</span>
                {escape_html(msg["content"])}
            </div>
            ''', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Input
    if prompt := st.chat_input("Mensagem para o assistente..."):
        st.session_state.pending_prompt = prompt
        st.rerun()

# === DASHBOARD ===
elif st.session_state.current_page == "dashboard":
    st.markdown("# 📊 Dashboard")
    st.caption("Visão geral • Dados demonstrativos")
    
    col1, col2, col3, col4 = st.columns(4)
    
    def metric(label, value, delta, is_curr=False):
        v = format_currency(value) if is_curr else str(value)
        d_class = "delta-positive" if delta.startswith("+") else "delta-negative"
        return f'<div class="metric-card"><div class="metric-value">{v}</div><div class="metric-label">{label}</div><div class="metric-delta {d_class}">{delta}</div></div>'
    
    with col1:
        st.markdown(metric("Pedidos Hoje", MOCK_METRICS["pedidos_hoje"], MOCK_METRICS["pedidos_delta"]), unsafe_allow_html=True)
    with col2:
        st.markdown(metric("Receita", MOCK_METRICS["receita_dia"], MOCK_METRICS["receita_delta"], True), unsafe_allow_html=True)
    with col3:
        st.markdown(metric("Em Produção", MOCK_METRICS["fila_producao"], MOCK_METRICS["fila_delta"]), unsafe_allow_html=True)
    with col4:
        st.markdown(metric("Ticket Médio", MOCK_METRICS["ticket_medio"], MOCK_METRICS["ticket_delta"], True), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("### 📈 Receita Semanal")
        st.bar_chart({"Seg": 3200, "Ter": 4100, "Qua": 3800, "Qui": 4500, "Sex": 5200, "Sáb": 2800}, color="#2563eb")
    with col2:
        st.markdown("### 🎯 Produção")
        st.markdown("- **Em Produção:** 8\n- **Aguardando:** 3\n- **Prontos:** 5\n- **Entregues:** 6")

# === PEDIDOS ===
elif st.session_state.current_page == "pedidos":
    st.markdown("# 📝 Pedidos")
    st.caption("Consulta de pedidos • Dados demonstrativos")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        search = st.text_input("🔍 Buscar", placeholder="Cliente ou pedido...", label_visibility="collapsed")
    with col2:
        status_filter = st.selectbox("Status", ["Todos", "✅ Entregue", "🔄 Produção", "⏳ Arte", "📦 Retirada"], label_visibility="collapsed")
    
    import pandas as pd
    df = pd.DataFrame(MOCK_ORDERS)
    if search:
        df = df[df["cliente"].str.contains(search, case=False) | df["id"].str.contains(search, case=False)]
    if status_filter != "Todos":
        df = df[df["status"] == status_filter]
    
    df["valor"] = df["valor"].apply(format_currency)
    df.columns = ["ID", "Cliente", "Produto", "Status", "Valor", "Data"]
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    total = sum(o["valor"] for o in MOCK_ORDERS)
    st.caption(f"Total: {format_currency(total)} • {len(MOCK_ORDERS)} pedidos")

# === CONFIG ===
elif st.session_state.current_page == "config":
    st.markdown("# ⚙️ Configurações")
    
    with st.expander("🔗 Webhook N8N", expanded=True):
        webhook = get_webhook_url() or ""
        masked = webhook[:25] + "..." if len(webhook) > 28 else webhook or "Não configurado"
        col1, col2 = st.columns([4, 1])
        with col1:
            st.text_input("URL", value=masked, disabled=True, label_visibility="collapsed")
        with col2:
            if st.button("Testar"):
                ok, msg = probe_webhook()
                st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")
    
    with st.expander("ℹ️ Sobre"):
        st.markdown("**Gráfica NBL Admin** v3.1\n\nAssistente IA + Dashboard + Consulta de Pedidos")
