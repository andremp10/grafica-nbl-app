import streamlit as st
import html
import os
import time
import random
from dotenv import load_dotenv
from services.n8n_service import get_webhook_url, probe_webhook, send_message_to_n8n

# --- CONFIGURAÇÃO ---
load_dotenv()
st.set_page_config(page_title="Gráfica NBL Admin", page_icon="🎨", layout="wide", initial_sidebar_state="expanded")

# --- CSS CENTRALIZADO E LIMPO ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Base */
    .stApp {background: #0a0a0a; font-family: 'Inter', sans-serif;}
    #MainMenu, footer, header {visibility: hidden;}
    
    /* Container principal centralizado */
    .block-container {
        max-width: 800px !important;
        margin: 0 auto !important;
        padding: 2rem 1rem 8rem 1rem !important;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {background: #111; border-right: 1px solid #222;}
    section[data-testid="stSidebar"] .block-container {padding: 1.5rem 1rem; max-width: 100% !important;}
    
    /* Tipografia */
    h1 {font-size: 1.5rem; font-weight: 600; color: #fff; text-align: center; margin-bottom: 0.25rem;}
    h2 {font-size: 1.25rem; font-weight: 600; color: #fff; margin-bottom: 1rem;}
    h3 {font-size: 1rem; font-weight: 600; color: #ccc;}
    p, span, div, label {color: #999; font-family: 'Inter', sans-serif;}
    
    /* Hero centralizado */
    .hero {text-align: center; padding: 3rem 1rem 2rem; margin-bottom: 1rem;}
    .hero h1 {font-size: 1.75rem; margin-bottom: 0.5rem;}
    .hero p {color: #666; font-size: 0.9rem;}
    
    /* Botões de ação centralizados */
    .action-grid {
        display: flex;
        justify-content: center;
        gap: 0.75rem;
        flex-wrap: wrap;
        margin: 1.5rem auto;
        max-width: 600px;
    }
    
    /* Mensagens centralizadas */
    .messages-container {
        max-width: 700px;
        margin: 0 auto;
        padding: 1rem 0;
    }
    .chat-msg {
        padding: 1rem 1.25rem;
        border-radius: 16px;
        margin-bottom: 0.75rem;
        line-height: 1.6;
        font-size: 0.95rem;
    }
    .user-msg {
        background: linear-gradient(135deg, #2563eb, #1d4ed8);
        color: white;
        margin-left: 20%;
        border-bottom-right-radius: 4px;
    }
    .ai-msg {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        color: #ddd;
        margin-right: 20%;
        border-bottom-left-radius: 4px;
    }
    
    /* Loading */
    .loading-box {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 1rem;
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        max-width: 350px;
        margin: 0 auto 1rem;
    }
    .dots {display: flex; gap: 4px;}
    .dots span {
        width: 8px; height: 8px;
        background: #2563eb;
        border-radius: 50%;
        animation: pulse 1.4s infinite ease-in-out;
    }
    .dots span:nth-child(2) {animation-delay: 0.2s;}
    .dots span:nth-child(3) {animation-delay: 0.4s;}
    @keyframes pulse {
        0%, 80%, 100% {transform: scale(0.6); opacity: 0.4;}
        40% {transform: scale(1); opacity: 1;}
    }
    
    /* Input fixo e bonito */
    .stChatInput {
        position: fixed;
        bottom: 0; left: 0; right: 0;
        background: linear-gradient(to top, #0a0a0a 85%, transparent);
        padding: 1.5rem;
    }
    .stChatInput > div {
        max-width: 700px;
        margin: 0 auto;
        background: #1a1a1a;
        border: 1px solid #333;
        border-radius: 24px;
    }
    .stChatInput input {
        background: transparent !important;
        border: none !important;
        color: white !important;
        padding: 1rem 1.25rem !important;
    }
    .stChatInput button {
        background: #2563eb !important;
        border-radius: 50% !important;
        margin: 6px !important;
    }
    
    /* Cards de métricas */
    .metric-box {
        background: #141414;
        border: 1px solid #222;
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
    }
    .metric-box .value {font-size: 2rem; font-weight: 700; color: #fff;}
    .metric-box .label {font-size: 0.7rem; color: #666; text-transform: uppercase; letter-spacing: 1px; margin-top: 4px;}
    .metric-box .delta {font-size: 0.85rem; margin-top: 4px;}
    .up {color: #22c55e;}
    .down {color: #ef4444;}
    
    /* Status badges */
    .badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 0.7rem;
        font-weight: 600;
    }
    .badge-ok {background: rgba(34, 197, 94, 0.15); color: #22c55e;}
    .badge-warn {background: rgba(234, 179, 8, 0.15); color: #eab308;}
    
    /* Botões */
    .stButton > button {
        background: #1a1a1a;
        border: 1px solid #333;
        color: #ddd;
        border-radius: 10px;
        transition: all 0.15s;
    }
    .stButton > button:hover {background: #222; border-color: #444;}
    
    /* Documentação */
    .doc-section {
        background: #111;
        border: 1px solid #222;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    .doc-section h3 {margin-bottom: 0.75rem; color: #fff;}
    .doc-section p, .doc-section li {color: #888; font-size: 0.9rem; line-height: 1.6;}
    .doc-section code {background: #1a1a1a; padding: 2px 6px; border-radius: 4px; color: #ddd;}
</style>
""", unsafe_allow_html=True)

# --- STATE ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "current_page" not in st.session_state:
    st.session_state.current_page = "chat"
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None

# --- HELPERS ---
def escape_html(text):
    return html.escape(text or "").replace("\n", "<br>")

def fmt_currency(v):
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

LOADING_MSGS = ["🔍 Analisando...", "🧠 Processando com IA...", "📊 Consultando dados...", "✨ Quase pronto..."]

MOCK_ORDERS = [
    {"id": "#2401", "cliente": "Restaurante Sabor & Arte", "produto": "500 Cardápios", "status": "✅ Entregue", "valor": 890.00},
    {"id": "#2402", "cliente": "Imobiliária Central", "produto": "1000 Folders", "status": "🔄 Produção", "valor": 1450.00},
    {"id": "#2403", "cliente": "Clínica Bem Estar", "produto": "200 Cartões", "status": "⏳ Arte", "valor": 180.00},
    {"id": "#2404", "cliente": "Loja Fashion Style", "produto": "50 Banners", "status": "🔄 Produção", "valor": 2100.00},
    {"id": "#2405", "cliente": "Escritório ABC", "produto": "2000 Envelopes", "status": "✅ Entregue", "valor": 680.00},
]

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("## 🎨 NBL Admin")
    st.caption("v3.2")
    st.markdown("---")
    
    for label, page in [("💬 Assistente", "chat"), ("📊 Dashboard", "dashboard"), ("📝 Pedidos", "pedidos"), ("ℹ️ Ajuda", "ajuda")]:
        if st.button(label, use_container_width=True, type="primary" if st.session_state.current_page == page else "secondary"):
            st.session_state.current_page = page
            st.rerun()
    
    st.markdown("---")
    st.markdown("##### Sistema")
    if get_webhook_url():
        st.markdown('<span class="badge badge-ok">● Online</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="badge badge-warn">● Offline</span>', unsafe_allow_html=True)
    
    st.markdown("---")
    if st.button("🗑️ Limpar Chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# --- PROCESS PENDING ---
def process_pending():
    if st.session_state.pending_prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        placeholder = st.empty()
        placeholder.markdown(f'''
        <div class="loading-box">
            <div class="dots"><span></span><span></span><span></span></div>
            <span style="color:#888">{random.choice(LOADING_MSGS)}</span>
        </div>
        ''', unsafe_allow_html=True)
        
        history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1]]
        response = send_message_to_n8n(prompt, history)
        placeholder.empty()
        
        st.session_state.messages.append({"role": "assistant", "content": response or "Não foi possível processar."})
        st.rerun()

# === PÁGINA: CHAT ===
if st.session_state.current_page == "chat":
    process_pending()
    
    if not st.session_state.messages:
        st.markdown('''
        <div class="hero">
            <h1>Como posso ajudar?</h1>
            <p>Assistente inteligente da Gráfica NBL</p>
        </div>
        ''', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📦 Status de Pedidos", use_container_width=True):
                st.session_state.pending_prompt = "Qual o status dos pedidos em aberto?"
                st.rerun()
        with col2:
            if st.button("💰 Faturamento", use_container_width=True):
                st.session_state.pending_prompt = "Como está o faturamento do mês?"
                st.rerun()
        with col3:
            if st.button("📊 Relatório", use_container_width=True):
                st.session_state.pending_prompt = "Gere um relatório geral"
                st.rerun()
    
    st.markdown('<div class="messages-container">', unsafe_allow_html=True)
    for msg in st.session_state.messages:
        cls = "user-msg" if msg["role"] == "user" else "ai-msg"
        st.markdown(f'<div class="chat-msg {cls}">{escape_html(msg["content"])}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    if prompt := st.chat_input("Mensagem..."):
        st.session_state.pending_prompt = prompt
        st.rerun()

# === PÁGINA: DASHBOARD ===
elif st.session_state.current_page == "dashboard":
    st.markdown("<h1 style='text-align:center'>📊 Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;color:#666'>Visão geral • Dados demonstrativos</p>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    metrics = [("Pedidos Hoje", 14, "+3"), ("Receita", "R$ 4.280", "+12%"), ("Em Produção", 8, "-2"), ("Ticket Médio", "R$ 305", "+5%")]
    
    for col, (label, value, delta) in zip([col1, col2, col3, col4], metrics):
        d_cls = "up" if "+" in delta else "down"
        with col:
            st.markdown(f'''
            <div class="metric-box">
                <div class="value">{value}</div>
                <div class="label">{label}</div>
                <div class="delta {d_cls}">{delta}</div>
            </div>
            ''', unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align:center'>📈 Receita Semanal</h3>", unsafe_allow_html=True)
    st.bar_chart({"Seg": 3200, "Ter": 4100, "Qua": 3800, "Qui": 4500, "Sex": 5200}, color="#2563eb")

# === PÁGINA: PEDIDOS ===
elif st.session_state.current_page == "pedidos":
    st.markdown("<h1 style='text-align:center'>📝 Pedidos</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;color:#666'>Consulta • Dados demonstrativos</p>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    
    search = st.text_input("🔍 Buscar cliente...", label_visibility="collapsed", placeholder="Buscar cliente ou pedido...")
    
    import pandas as pd
    df = pd.DataFrame(MOCK_ORDERS)
    if search:
        df = df[df["cliente"].str.contains(search, case=False) | df["id"].str.contains(search, case=False)]
    df["valor"] = df["valor"].apply(fmt_currency)
    df.columns = ["ID", "Cliente", "Produto", "Status", "Valor"]
    st.dataframe(df, use_container_width=True, hide_index=True)

# === PÁGINA: AJUDA ===
elif st.session_state.current_page == "ajuda":
    st.markdown("<h1 style='text-align:center'>ℹ️ Ajuda & Status</h1>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Status do Sistema
    st.markdown('''
    <div class="doc-section">
        <h3>🔌 Status do Sistema</h3>
    </div>
    ''', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        if get_webhook_url():
            st.success("✅ Webhook N8N: Conectado")
        else:
            st.error("❌ Webhook N8N: Não configurado")
    with col2:
        if st.button("🔄 Testar Conexão"):
            ok, msg = probe_webhook()
            st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Documentação
    st.markdown('''
    <div class="doc-section">
        <h3>📖 Como Usar</h3>
        <p><strong>Assistente IA</strong></p>
        <ul>
            <li>Digite sua pergunta no campo de mensagem</li>
            <li>Use os botões rápidos para consultas comuns</li>
            <li>O assistente pode demorar alguns segundos para processar</li>
        </ul>
        <p><strong>Dashboard</strong></p>
        <ul>
            <li>Visualize métricas em tempo real</li>
            <li>Acompanhe receita e pedidos do dia</li>
        </ul>
        <p><strong>Pedidos</strong></p>
        <ul>
            <li>Consulte pedidos por cliente ou ID</li>
            <li>Acompanhe status de produção</li>
        </ul>
    </div>
    ''', unsafe_allow_html=True)
    
    st.markdown('''
    <div class="doc-section">
        <h3>❓ Suporte</h3>
        <p>Em caso de problemas, verifique:</p>
        <ul>
            <li>Status do webhook (acima)</li>
            <li>Conexão com a internet</li>
            <li>Se o N8N está online</li>
        </ul>
    </div>
    ''', unsafe_allow_html=True)
