import streamlit as st
import time
import random
from dotenv import load_dotenv
from services.n8n_service import get_webhook_url, probe_webhook, send_message_to_n8n

# --- 1. CONFIGURAÇÃO (Page Config) ---
load_dotenv()
st.set_page_config(
    page_title="Gráfica NBL Admin",
    page_icon="🎨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CSS MÍNIMO (Acabamento) ---
st.markdown("""
<style>
    /* Ajuste de largura e padding central */
    .main .block-container {
        max-width: 900px;
        padding-top: 1rem;
        padding-bottom: 5rem;
    }
    
    /* Cards de Ação Rápida */
    .quick-card {
        background-color: #1c1c1c;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
        cursor: pointer;
        transition: transform 0.1s, border-color 0.1s;
    }
    .quick-card:hover {
        transform: translateY(-2px);
        border-color: #2563eb;
    }
    .quick-card h4 {
        margin: 0;
        font-size: 1rem;
        color: #fff;
    }
    .quick-card p {
        margin: 0;
        font-size: 0.8rem;
        color: #888;
    }

    /* Ajuste Status Badge */
    .status-dot {
        height: 8px;
        width: 8px;
        background-color: #22c55e;
        border-radius: 50%;
        display: inline-block;
        margin-right: 6px;
    }
    .status-dot.offline {
        background-color: #ef4444;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. GESTÃO DE ESTADO (Init State) ---
def init_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "current_view" not in st.session_state:
        st.session_state.current_view = "Chat"  # Chat, Status, Faturamento, Relatórios
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None

init_state()

# --- 4. COMPONENTES DE UI ---

def render_sidebar():
    with st.sidebar:
        st.caption("Navegação")
        
        # Menu principal
        selected = st.radio(
            "Módulos",
            options=["Chat", "Status de Pedidos", "Faturamento", "Relatórios"],
            label_visibility="collapsed"
        )
        
        # Atualiza view se mudar
        if selected != st.session_state.current_view:
            st.session_state.current_view = selected
            st.rerun()

        st.divider()
        
        # Informações de suporte
        st.caption("Suporte")
        st.info("Para dúvidas, use o chat ou contate o suporte técnico.")

def render_topbar():
    # Topbar contextual usando colunas
    c1, c2, c3 = st.columns([3, 1, 1])
    
    with c1:
        st.markdown("### 🎨 Gráfica NBL Admin")
        st.caption("Sistema Integrado de Gestão & Assistente IA")
    
    with c2:
        webhook_ok = bool(get_webhook_url())
        status_color = "green" if webhook_ok else "red"
        status_text = "Online" if webhook_ok else "Offline"
        st.markdown(f"<div style='margin-top: 10px; text-align: right;'><span class='status-dot {'offline' if not webhook_ok else ''}'></span>{status_text}</div>", unsafe_allow_html=True)
        
    with c3:
        if st.button("Limpar Chat", type="secondary", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

def render_quick_actions():
    st.markdown("##### Ações Rápidas")
    
    c1, c2, c3 = st.columns(3)
    
    # Card 1: Status
    with c1:
        if st.button("📦 Status Pedidos", use_container_width=True):
            st.session_state.pending_prompt = "Qual o status dos pedidos em aberto?"
            st.rerun()
            
    # Card 2: Faturamento
    with c2:
        if st.button("💰 Faturamento Mês", use_container_width=True):
            st.session_state.pending_prompt = "Como está o faturamento deste mês?"
            st.rerun()
            
    # Card 3: Relatório
    with c3:
        if st.button("📊 Relatório Geral", use_container_width=True):
            st.session_state.pending_prompt = "Gere um relatório geral da operação."
            st.rerun()
            
    st.divider()

def render_chat_area():
    # Container para mensagens
    chat_container = st.container()
    
    # Exibir mensagens
    with chat_container:
        if not st.session_state.messages:
            st.info("👋 Olá! Sou o assistente da Gráfica NBL. Selecione uma ação acima ou digite sua dúvida.")
        
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    # Lógica de envio (Pending Prompt ou Input)
    prompt = st.chat_input("Digite sua mensagem...")
    
    if st.session_state.pending_prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
    
    if prompt:
        # Exibe mensagem do usuário
        st.session_state.messages.append({"role": "user", "content": prompt})
        with chat_container:
            with st.chat_message("user"):
                st.markdown(prompt)
        
        # Processamento com Spinner
        with st.spinner("Processando solicitação..."):
            history = st.session_state.messages[:-1]
            response = send_message_to_n8n(prompt, history)
            
            if not response:
                response = "Desculpe, não consegui obter uma resposta do servidor. Tente novamente."
        
        # Exibe resposta da IA
        st.session_state.messages.append({"role": "assistant", "content": response})
        with chat_container:
            with st.chat_message("assistant"):
                st.markdown(response)
        
        # Rerun para atualizar estado visualmente limpo
        st.rerun()

# --- 5. LOGICA DE MÓDULOS (Dashboard, etc.) ---

def render_dashboard_view():
    st.title("📊 Status de Pedidos")
    st.info("Módulo visual em desenvolvimento. Use o Chat para consultar status reais.")
    
    # Mock data simples para não ficar vazio
    c1, c2, c3 = st.columns(3)
    c1.metric("Pedidos Hoje", "12", "+2")
    c2.metric("Pendentes", "5", "-1")
    c3.metric("Entregues", "85%", "+5%")
    
    st.markdown("### Pedidos Recentes")
    st.dataframe([
        {"ID": 101, "Cliente": "Padaria Estrela", "Status": "Produção", "Valor": "R$ 450,00"},
        {"ID": 102, "Cliente": "Advocacia Silva", "Status": "Arte", "Valor": "R$ 120,00"},
        {"ID": 103, "Cliente": "Mercado Central", "Status": "Entregue", "Valor": "R$ 890,00"},
    ])

# --- 6. APP PRINCIPAL (Main Loop) ---

def main():
    render_sidebar()
    render_topbar()
    
    # Roteamento de Views
    view = st.session_state.current_view
    
    if view == "Chat":
        render_quick_actions()
        render_chat_area()
        
    elif view in ["Status de Pedidos", "Faturamento", "Relatórios"]:
        # Por enquanto, esses módulos podem usar a mesma view de Dashboard ou customizadas
        render_dashboard_view()
        
        # Botão para voltar ao chat rápido
        if st.button("💬 Voltar ao Chat", type="primary"):
            st.session_state.current_view = "Chat"
            st.rerun()

if __name__ == "__main__":
    main()
