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

# --- CSS REFINADO (Input Full Width) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Base */
    .stApp {background: #0a0a0a; font-family: 'Inter', sans-serif;}
    #MainMenu, footer, header {visibility: hidden;}
    
    /* Container Central Principal - 800px e centralizado */
    .block-container {
        max-width: 800px !important;
        margin: auto !important;
        padding-top: 1rem !important;
        padding-bottom: 8rem !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {background: #111; border-right: 1px solid #222;}
    
    /* Input Fixo (Largura correta) */
    .stChatInput {
        position: fixed;
        bottom: 0; left: 0; right: 0;
        background: linear-gradient(to top, #0a0a0a 90%, transparent);
        padding: 2rem 1rem;
        z-index: 999;
    }
    .stChatInput > div {
        max-width: 800px !important;
        margin: 0 auto !important;
        width: 100% !important;
        background: #1a1a1a;
        border: 1px solid #333;
        border-radius: 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .stChatInput input {
        background: transparent !important;
        border: none !important;
        color: white !important;
        padding: 1rem !important;
    }
    .stChatInput button {
        background: #2563eb !important;
        border-radius: 8px !important;
        width: 32px !important;
        height: 32px !important;
        margin-right: 8px !important;
    }
    
    /* Mensagens */
    .msg-container {
        display: flex;
        flex-direction: column;
        gap: 1rem;
        padding-bottom: 1rem;
        width: 100%;
    }
    .chat-msg {
        padding: 1rem 1.25rem;
        border-radius: 12px;
        line-height: 1.6;
        font-size: 0.95rem;
        position: relative;
        max-width: 85%;
        word-wrap: break-word;
    }
    .user-msg {
        align-self: flex-end;
        background: #2563eb;
        color: white;
        border-bottom-right-radius: 2px;
    }
    .ai-msg {
        align-self: flex-start;
        background: #1a1a1a;
        border: 1px solid #333;
        color: #e0e0e0;
        border-bottom-left-radius: 2px;
    }
    
    /* Hero */
    .hero {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        height: 60vh;
        width: 100%;
    }
    .hero h1 {font-size: 2.2rem; margin-bottom: 0.5rem; color: #fff; font-weight: 700;}
    .hero p {color: #666; font-size: 1.1rem; margin-bottom: 2rem;}
    
    /* Botões de Sugestão (Maiores) */
    .stButton button {
        width: 100%;
        background: #1a1a1a;
        border: 1px solid #333;
        color: #ccc;
        padding: 1.25rem;
        border-radius: 12px;
        font-size: 1rem;
        transition: all 0.2s;
    }
    .stButton button:hover {
        background: #2563eb;
        border-color: #2563eb;
        color: white;
        transform: translateY(-2px);
    }
    
    /* Loading */
    .loading-box {
        background: #1a1a1a;
        border: 1px solid #333;
        border-radius: 12px;
        padding: 1rem;
        display: flex;
        align-items: center;
        gap: 12px;
        width: fit-content;
        margin-bottom: 1rem;
    }
    .dots span {
        width: 6px; height: 6px; background: #2563eb; border-radius: 50%;
        display: inline-block; animation: wave 1.2s infinite ease-in-out;
    }
    @keyframes wave {0%, 100% {transform: scale(0.6); opacity:0.5} 50% {transform: scale(1); opacity:1}}
</style>
""", unsafe_allow_html=True)

# --- CONFIGURAR STATE ---
if "messages" not in st.session_state: st.session_state.messages = []
if "current_page" not in st.session_state: st.session_state.current_page = "chat"
if "pending_prompt" not in st.session_state: st.session_state.pending_prompt = None

# --- SIDEBAR FIXA ---
with st.sidebar:
    st.markdown("## 🎨 NBL Admin")
    st.caption("v3.3")
    st.markdown("---")
    
    pages = {"💬 Assistente": "chat", "📊 Dashboard": "dashboard", "📝 Pedidos": "pedidos", "ℹ️ Ajuda": "ajuda"}
    for label, key in pages.items():
        if st.button(label, use_container_width=True, type="primary" if st.session_state.current_page == key else "secondary"):
            st.session_state.current_page = key
            st.rerun()
            
    st.markdown("---")
    st.markdown("**Status**: " + ("🟢 Online" if get_webhook_url() else "🔴 Offline"))
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🗑️ Limpar", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# --- LOGICA LOADING ---
def process_pending():
    if st.session_state.pending_prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # UI Placeholder
        container = st.empty()
        container.markdown(f"""
        <div class="loading-box">
            <div class="dots"><span></span> <span></span> <span></span></div>
            <span style="color:#888; font-size:0.9rem">{random.choice(["Analisando...", "Processando solicitação...", "Consultando dados..."])}</span>
        </div>
        """, unsafe_allow_html=True)
        
        # Call N8N
        history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1]]
        response = send_message_to_n8n(prompt, history)
        container.empty()
        
        st.session_state.messages.append({"role": "assistant", "content": response or "Sem resposta."})
        st.rerun()

# --- PÁGINA: CHAT ---
if st.session_state.current_page == "chat":
    process_pending()
    
    # Hero (ocupando centro da tela se vazio)
    if not st.session_state.messages:
        st.markdown("""
        <div class="hero">
            <h1>Como posso ajudar?</h1>
            <p>Selecione uma opção ou digite abaixo</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📦 Status Pedidos"):
                st.session_state.pending_prompt = "Status dos pedidos?"
                st.rerun()
        with col2:
            if st.button("💰 Faturamento"):
                st.session_state.pending_prompt = "Faturamento mês?"
                st.rerun()
        with col3:
            if st.button("📊 Relatório"):
                st.session_state.pending_prompt = "Relatório geral"
                st.rerun()
    
    # Mensagens
    else:
        st.markdown('<div class="msg-container">', unsafe_allow_html=True)
        for msg in st.session_state.messages:
            cls = "user-msg" if msg["role"] == "user" else "ai-msg"
            content = html.escape(msg["content"]).replace("\n", "<br>")
            st.markdown(f'<div class="chat-msg {cls}">{content}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
            
    # Input
    if prompt := st.chat_input("Digite sua mensagem..."):
        st.session_state.pending_prompt = prompt
        st.rerun()

# === OUTRAS PÁGINAS (Mantendo estilo limpo) ===
elif st.session_state.current_page == "dashboard":
    st.markdown("### 📊 Visão Geral")
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pedidos", "14", "+3")
    c2.metric("Receita", "R$ 4.2k", "+12%")
    c3.metric("Produção", "8", "-2")
    c4.metric("Ticket", "R$ 305", "+5%")
    st.bar_chart({"A":10, "B":20, "C":15})
    
elif st.session_state.current_page == "pedidos":
    st.markdown("### 📝 Pedidos Recentes")
    st.markdown("---")
    st.dataframe(MOCK_ORDERS, use_container_width=True)
    
elif st.session_state.current_page == "ajuda":
    st.markdown("### ℹ️ Ajuda")
    st.info("Sistema conectado e operante.")
    st.markdown("Use o chat para interagir com o assistente IA.")
