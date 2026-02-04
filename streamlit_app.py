import streamlit as st
import html
import os
import time
from dotenv import load_dotenv
from services.n8n_service import get_webhook_url, probe_webhook, send_message_to_n8n

# --- 0. CONFIGURAÇÃO INICIAL (UX: Performance & Cache) ---
load_dotenv()

st.set_page_config(
    page_title="Gráfica NBL Admin",
    page_icon="🎨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 1. ESTILO PREMIUM INSPIRADO NO CANVAS EFFECT ---
def load_premium_css():
    st.markdown("""
    <style>
        /* ================= IMPORTS ================= */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
        
        /* ================= GLOBAL ================= */
        .stApp {
            background: linear-gradient(135deg, #0a0a0f 0%, #12121a 50%, #0a0a0f 100%);
            font-family: 'Inter', sans-serif;
        }

        /* evita que o input fixo cubra o conteúdo no final da página */
        .block-container {
            padding-bottom: 7rem;
        }
        
        h1, h2, h3 {
            font-family: 'Inter', sans-serif;
            font-weight: 800;
            background: linear-gradient(135deg, #ff4b4b 0%, #ff6b6b 50%, #ff8585 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        p, div, label, span {
            color: #e0e0e0;
            font-family: 'Inter', sans-serif;
        }

        /* ================= SIDEBAR ================= */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0d0d14 0%, #151520 100%);
            border-right: 1px solid rgba(255, 75, 75, 0.1);
        }
        section[data-testid="stSidebar"] .block-container {
            padding-top: 2rem;
        }

        /* ================= ANIMATED BACKGROUND ================= */
        .chat-hero {
            position: relative;
            padding: 40px 20px;
            text-align: center;
            overflow: hidden;
            border-radius: 20px;
            background: linear-gradient(135deg, rgba(20,20,30,0.95), rgba(15,15,22,0.98));
            border: 1px solid rgba(255, 75, 75, 0.15);
            margin-bottom: 20px;
        }
        
        .chat-hero::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-image: 
                radial-gradient(circle at 20% 80%, rgba(255, 75, 75, 0.15) 0%, transparent 50%),
                radial-gradient(circle at 80% 20%, rgba(255, 107, 107, 0.1) 0%, transparent 50%),
                radial-gradient(circle at 40% 40%, rgba(255, 133, 133, 0.05) 0%, transparent 30%);
            animation: pulse-bg 4s ease-in-out infinite alternate;
            pointer-events: none;
        }
        
        @keyframes pulse-bg {
            0% { opacity: 0.5; transform: scale(1); }
            100% { opacity: 1; transform: scale(1.05); }
        }
        
        /* Dot Matrix Effect (CSS version) */
        .dot-matrix {
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-image: radial-gradient(rgba(255, 75, 75, 0.3) 1px, transparent 1px);
            background-size: 20px 20px;
            opacity: 0.3;
            animation: dot-fade 3s ease-in-out infinite alternate;
            pointer-events: none;
        }
        
        @keyframes dot-fade {
            0% { opacity: 0.1; }
            100% { opacity: 0.4; }
        }
        
        /* ================= ANIMATED TITLE ================= */
        .gradient-title {
            font-size: 2.5rem;
            font-weight: 900;
            letter-spacing: -0.02em;
            margin: 0;
            padding: 0;
            position: relative;
            z-index: 10;
        }
        
        .gradient-word {
            display: inline-block;
            background-size: 200% 200%;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            animation: gradient-shift 3s ease infinite;
        }
        
        .word-1 {
            background-image: linear-gradient(135deg, #007cf0, #00dfd8);
            animation-delay: 0s;
        }
        .word-2 {
            background-image: linear-gradient(135deg, #7928ca, #ff0080);
            animation-delay: 0.5s;
        }
        .word-3 {
            background-image: linear-gradient(135deg, #ff4b4b, #fbbf24);
            animation-delay: 1s;
        }
        
        @keyframes gradient-shift {
            0%, 100% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
        }
        
        .subtitle {
            color: rgba(255,255,255,0.5);
            font-size: 0.9rem;
            margin-top: 10px;
            position: relative;
            z-index: 10;
        }

        /* ================= CHAT CONTAINER ================= */
        .chat-container {
            max-width: 800px;
            margin: 0 auto;
            padding-bottom: 20px;
        }
        
        /* ================= MESSAGE BUBBLES ================= */
        .chat-bubble {
            padding: 16px 20px;
            border-radius: 16px;
            margin-bottom: 16px;
            line-height: 1.6;
            font-size: 15px;
            position: relative;
            max-width: 85%;
            animation: slideIn 0.3s ease-out;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateY(15px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* User Message */
        .user-bubble {
            background: linear-gradient(135deg, #ff4b4b 0%, #ff6b6b 100%);
            color: #FFFFFF;
            margin-left: auto;
            border-bottom-right-radius: 4px;
            font-weight: 500;
        }

        /* AI Message */
        .ai-bubble {
            background: linear-gradient(135deg, rgba(30,33,41,0.95) 0%, rgba(25,28,36,0.98) 100%);
            color: #E0E0E0;
            margin-right: auto;
            border-bottom-left-radius: 4px;
            border: 1px solid rgba(255, 75, 75, 0.2);
            backdrop-filter: blur(10px);
        }
        
        .ai-icon {
            display: inline-block;
            width: 24px;
            height: 24px;
            background: linear-gradient(135deg, #ff4b4b, #ff8585);
            border-radius: 50%;
            margin-right: 8px;
            vertical-align: middle;
            text-align: center;
            line-height: 24px;
            font-size: 12px;
        }

        /* ================= INPUT STYLING ================= */
        .stChatInput {
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: rgba(10,10,15,0.95);
            backdrop-filter: blur(20px);
            border-top: 1px solid rgba(255,75,75,0.1);
            padding: 16px;
        }
        
        .stChatInput > div {
            max-width: 800px;
            margin: 0 auto;
        }
        
        .stChatInput input, .stChatInput textarea {
            background: rgba(30,33,41,0.9) !important;
            color: white !important;
            border: 1px solid rgba(255, 75, 75, 0.2) !important;
            border-radius: 16px !important;
            padding: 14px 20px !important;
            font-family: 'Inter', sans-serif !important;
            font-size: 15px !important;
        }
        
        .stChatInput input:focus, .stChatInput textarea:focus {
            border-color: #ff4b4b !important;
            box-shadow: 0 0 0 2px rgba(255, 75, 75, 0.2) !important;
        }

        /* ================= METRICS & CARDS ================= */
        .metric-card {
            background: linear-gradient(135deg, rgba(30,33,41,0.9), rgba(25,28,36,0.95));
            padding: 24px;
            border-radius: 16px;
            border: 1px solid rgba(255, 75, 75, 0.15);
            text-align: center;
            backdrop-filter: blur(10px);
            transition: all 0.3s ease;
        }
        .metric-card:hover {
            transform: translateY(-4px);
            border-color: rgba(255, 75, 75, 0.4);
            box-shadow: 0 10px 40px rgba(255, 75, 75, 0.1);
        }
        .metric-value {
            font-size: 32px;
            font-weight: 800;
            background: linear-gradient(135deg, #ff4b4b, #ff8585);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .metric-label {
            font-size: 12px;
            color: rgba(255,255,255,0.5);
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-top: 8px;
        }

        /* ================= HIDE STREAMLIT DEFAULTS ================= */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Status badges */
        .status-badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .status-online {
            background: rgba(0, 200, 150, 0.15);
            color: #00c896;
            border: 1px solid rgba(0, 200, 150, 0.3);
        }
    </style>
    """, unsafe_allow_html=True)

load_premium_css()

def escape_text_to_html(text: str) -> str:
    return html.escape(text).replace("\n", "<br>")

def is_error_like_message(text: str) -> bool:
    return (text or "").lstrip().startswith(("❌", "⚠️", "🔌", "⏳", "⚙️"))

# --- 2. GERENCIAMENTO DE ESTADO (UX: Fluidez) ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_update" not in st.session_state:
    st.session_state.last_update = time.time()
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None
if "last_n8n_ok" not in st.session_state:
    st.session_state.last_n8n_ok = None
if "last_n8n_ts" not in st.session_state:
    st.session_state.last_n8n_ts = None
if "is_sending" not in st.session_state:
    st.session_state.is_sending = False

def enqueue_prompt(prompt: str) -> None:
    st.session_state.pending_prompt = prompt
    st.rerun()

def process_pending_prompt() -> None:
    prompt = (st.session_state.pending_prompt or "").strip()
    if not prompt or st.session_state.is_sending:
        st.session_state.pending_prompt = None
        return

    st.session_state.is_sending = True
    st.session_state.pending_prompt = None

    st.session_state.messages.append({"role": "user", "content": prompt})
    history_payload = [
        {"role": m["role"], "content": m["content"]}
        for m in st.session_state.messages[:-1]
    ]

    with st.spinner("🧠 Processando..."):
        response_text = send_message_to_n8n(prompt, history_payload)

    st.session_state.last_n8n_ok = not is_error_like_message(response_text)
    st.session_state.last_n8n_ts = time.time()
    st.session_state.is_sending = False

    st.session_state.messages.append({
        "role": "model",
        "content": response_text or "⚠️ Resposta vazia do servidor.",
    })

    st.rerun()

# --- 3. UI: SIDEBAR (Navegação) ---
with st.sidebar:
    st.title("🎨 NBL Admin")
    st.caption("v2.0 Premium | Streamlit")
    st.markdown("---")
    
    # Navegação Clara
    menu_selection = st.radio(
        "Navegação",
        ["🤖 Assistente IA", "📊 Dashboard", "📝 Pedidos (BETA)", "⚙️ Configurações"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    
    # Bloco de Status (Feedback constante)
    with st.container():
        st.markdown("### Status do Sistema")

        webhook_url = get_webhook_url()
        if webhook_url:
            st.success("🟢 Webhook: configurado")
        else:
            st.error("🔴 Webhook: não configurado")

        if st.session_state.last_n8n_ok is True:
            st.caption("Última chamada: ✅ OK")
        elif st.session_state.last_n8n_ok is False:
            st.caption("Última chamada: ❌ erro")

        supabase_url = os.getenv("SUPABASE_URL") or None
        supabase_key = os.getenv("SUPABASE_KEY") or None
        if supabase_url and supabase_key:
            st.success("☁️ Supabase: configurado")
        else:
            st.info("☁️ Supabase: não configurado")

    st.markdown("---")
    if st.button("🗑️ Limpar Chat", type="secondary", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# --- 4. PÁGINAS (Conteúdo) ---

# --- PÁGINA: ASSISTENTE IA ---
if menu_selection == "🤖 Assistente IA":
    # Processa ações pendentes (ex.: botões rápidos) antes de renderizar
    if st.session_state.pending_prompt:
        process_pending_prompt()
    
    # Hero Section com efeito animado (quando não há mensagens)
    if not st.session_state.messages:
        st.markdown("""
        <div class="chat-hero">
            <div class="dot-matrix"></div>
            <h1 class="gradient-title">
                <span class="gradient-word word-1">AI.</span>
                <span class="gradient-word word-2">Chat.</span>
                <span class="gradient-word word-3">Experience.</span>
            </h1>
            <p class="subtitle">Como posso ajudar você hoje?</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Quick Actions
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📦 Status de Pedidos", use_container_width=True):
                enqueue_prompt("Qual o status dos pedidos em aberto?")
        with col2:
            if st.button("💰 Faturamento", use_container_width=True):
                enqueue_prompt("Como está o faturamento do mês?")
        with col3:
            if st.button("📊 Relatório Geral", use_container_width=True):
                enqueue_prompt("Gere um relatório geral do sistema")
    
    # Container de Mensagens
    with st.container():
        st.markdown('<div class="chat-container">', unsafe_allow_html=True)
        
        # Renderizar Histórico
        for msg in st.session_state.messages:
            if msg["role"] == "user":
                content_html = escape_text_to_html(msg.get("content", ""))
                st.markdown(f'<div class="chat-bubble user-bubble">{content_html}</div>', unsafe_allow_html=True)
            else:
                content_html = escape_text_to_html(msg.get("content", ""))
                st.markdown(f'''
                <div class="chat-bubble ai-bubble">
                    <span class="ai-icon">🤖</span>
                    {content_html}
                </div>
                ''', unsafe_allow_html=True)
                
        st.markdown('</div>', unsafe_allow_html=True)

    # Input Area (Fixo visualmente pelo st.chat_input)
    if prompt := st.chat_input("💬 Digite sua mensagem..."):
        enqueue_prompt(prompt)

# --- PÁGINA: DASHBOARD ---
elif menu_selection == "📊 Dashboard":
    st.markdown("## 📊 Visão Geral")
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Cards Estilizados (HTML/CSS Custom)
    def metric_card(label, value, delta):
        return f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div style="color: {'#00C896' if '+' in delta else '#FF4B4B'}; font-size: 14px; margin-top: 5px;">
                {delta}
            </div>
        </div>
        """

    with col1:
        st.markdown(metric_card("Pedidos Hoje", "14", "+3 un"), unsafe_allow_html=True)
    with col2:
        st.markdown(metric_card("Receita (Dia)", "R$ 4.2k", "+12%"), unsafe_allow_html=True)
    with col3:
        st.markdown(metric_card("Fila de Impressão", "8", "-2 un"), unsafe_allow_html=True)
    with col4:
        st.markdown(metric_card("Ticket Médio", "R$ 305", "+5%"), unsafe_allow_html=True)

    st.markdown("### 📈 Fluxo de Caixa (Simulado)")
    st.bar_chart({"Receita": [4200, 3800, 5100, 4900, 4200]}, color="#FF4B4B")

# --- PÁGINA: PEDIDOS ---
elif menu_selection == "📝 Pedidos (BETA)":
    st.markdown("## 📝 Gestão de Pedidos")
    st.warning("🚧 Módulo em construção. Dados mockados para visualização.")
    
    # Exemplo de Tabela Estilizada
    data = [
        {"ID": "#1234", "Cliente": "Impacto Criativo", "Status": "Produção", "Valor": "R$ 450,00"},
        {"ID": "#1235", "Cliente": "Dona Maria Bolos", "Status": "Entregue", "Valor": "R$ 120,00"},
        {"ID": "#1236", "Cliente": "Tech Solutions", "Status": "Aguardando Arte", "Valor": "R$ 1.200,00"},
    ]
    st.dataframe(
        data, 
        use_container_width=True,
        hide_index=True
    )

# --- CONFIGURAÇÕES ---
elif menu_selection == "⚙️ Configurações":
    st.header("Configurações")
    with st.expander("🔗 Conexões"):
        url = get_webhook_url() or ""
        masked = ""
        if url:
            masked = url[:8] + "…" + url[-8:] if len(url) > 20 else "••••••••"
        st.text_input(
            "Webhook N8N URL",
            value=masked,
            type="password",
            disabled=True,
            help="Configure via Secrets no Streamlit Cloud (WEBHOOK_URL).",
        )

        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("Testar webhook", use_container_width=True):
                ok, detail = probe_webhook()
                if ok:
                    st.success(f"✅ {detail}")
                else:
                    st.error(f"❌ {detail}")
        with col2:
            st.caption("Dica: use `WEBHOOK_URL` (recomendado). Também aceitamos `VITE_WEBHOOK_URL` por compatibilidade.")

