import streamlit as st
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from services.n8n_service import get_webhook_url, send_message_to_n8n

# --- 1. CONFIGURAÇÃO ---
load_dotenv()
st.set_page_config(
    page_title="Gráfica NBL Admin",
    page_icon="🎨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CSS (Acabamento) ---
st.markdown("""
<style>
    .main .block-container {max-width: 900px; padding-top: 2rem; padding-bottom: 5rem;}
    
    /* Hero - Tela inicial centralizada */
    .hero-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        height: 60vh;
        text-align: center;
    }
    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        color: #fff;
        margin-bottom: 2rem;
    }
    
    /* Sugestões agrupadas */
    .suggestions-grid {
        display: flex;
        gap: 1rem;
        justify-content: center;
        width: 100%;
        max-width: 800px;
    }
    .stButton button {
        width: 100%;
        padding: 1rem;
        border-radius: 12px;
        background: #1a1a1a;
        border: 1px solid #333;
        color: #ddd;
        transition: all 0.2s;
    }
    .stButton button:hover {
        border-color: #2563eb;
        color: #fff;
        transform: translateY(-2px);
    }
    
    /* Mensagens */
    .chat-container {
        margin-top: 1rem;
        display: flex;
        flex-direction: column;
        gap: 1rem;
    }
    
    /* Dashboards */
    .metric-box {
        background: #1a1a1a; border: 1px solid #333; padding: 1.5rem; border-radius: 10px; text-align: center;
    }
    .metric-val {font-size: 1.8rem; font-weight: bold; color:white}
    .metric-lbl {font-size: 0.8rem; color: #888; text-transform: uppercase;}
    
    /* Footer */
    .footer {position: fixed; bottom: 10px; left: 20px; font-size: 11px; color: #444;}
</style>
""", unsafe_allow_html=True)

# --- 3. DADOS MOCKADOS ---
def get_mock_orders():
    return pd.DataFrame([
        {"ID": "#2401", "Cliente": "Restaurante Sabor", "Status": "Entregue", "Valor": 890},
        {"ID": "#2402", "Cliente": "Imob. Central", "Status": "Produção", "Valor": 1450},
        {"ID": "#2403", "Cliente": "Clínica Bem Estar", "Status": "Arte", "Valor": 180},
    ])

# --- 4. VIEWS ---

def render_chat_view():
    # Se não tem mensagens, mostrar Hero Centralizado
    if not st.session_state.messages:
        st.markdown("""
        <div class="hero-container">
            <div class="hero-title">Como posso ajudar?</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Botões de sugestão centralizados
        c1, c2, c3 = st.columns(3)
        with c1: 
            if st.button("📦 Meus Pedidos", use_container_width=True): 
                st.session_state.pending_prompt = "Status dos meus pedidos"
                st.rerun()
        with c2: 
            if st.button("💰 Faturamento", use_container_width=True): 
                st.session_state.pending_prompt = "Resumo financeiro do mês"
                st.rerun()
        with c3: 
            if st.button("📊 Relatórios", use_container_width=True): 
                st.session_state.pending_prompt = "Gerar relatório operacional"
                st.rerun()
                
    else:
        # Se tem mensagens, mostra histórico normal
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    # Input sempre visível
    if prompt := st.chat_input("Digite sua mensagem..."):
        st.session_state.pending_prompt = prompt
        st.rerun()

    # Processamento
    if st.session_state.get("pending_prompt"):
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)
        
        # St.Status com etapas (Loading)
        with st.status("🚀 Processando...", expanded=True) as status:
            time.sleep(1)
            status.write("🔍 Analisando contexto...")
            time.sleep(1)
            status.write("📊 Consultando dados...")
            
            history = st.session_state.messages[:-1]
            response = send_message_to_n8n(prompt, history)
            status.update(label="✅ Concluído", state="complete", expanded=False)
            
        final_resp = response or "Erro ao processar."
        st.session_state.messages.append({"role": "assistant", "content": final_resp})
        with st.chat_message("assistant"): st.markdown(final_resp)
        st.rerun()

def render_status_view():
    st.markdown("### 🏭 Status de Produção")
    st.divider()
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Na Fila", "8")
    c2.metric("Produção", "12")
    c3.metric("Atrasados", "3", "-1", delta_color="inverse")
    c4.metric("No Prazo", "98%")
    st.markdown("#### Lista de Pedidos")
    st.dataframe(get_mock_orders(), use_container_width=True)

def render_finance_view():
    st.markdown("### 💰 Financeiro")
    st.divider()
    c1,c2 = st.columns(2)
    c1.metric("Faturamento", "R$ 14.5k", "+12%")
    c2.metric("Ticket Médio", "R$ 480")
    st.line_chart([10, 20, 15, 25, 30])

def render_instructions():
    st.markdown("### ℹ️ Instruções")
    st.info("Utilize a sidebar para navegar. O chat IA responde sobre preços e status.")

# --- 5. MAIN ---
def main():
    if "messages" not in st.session_state: st.session_state.messages = []
    if "current_view" not in st.session_state: st.session_state.current_view = "Chat"

    with st.sidebar:
        st.title("🎨 NBL Admin")
        st.caption("v4.2")
        st.divider()
        menu = {"💬 Chat": "Chat", "🏭 Status": "Status", "💰 Financeiro": "Financeiro", "ℹ️ Instruções": "Instruções"}
        for k,v in menu.items():
            if st.button(k, use_container_width=True, type="primary" if st.session_state.current_view==v else "secondary"):
                st.session_state.current_view = v
                st.rerun()
        st.divider()
        st.caption("Desenvolvido por\n**Golfine Tecnologia**")
        if st.button("Limpar"):
             st.session_state.messages = []
             st.rerun()

    if st.session_state.current_view == "Chat": render_chat_view()
    elif st.session_state.current_view == "Status": render_status_view()
    elif st.session_state.current_view == "Financeiro": render_finance_view()
    elif st.session_state.current_view == "Instruções": render_instructions()

if __name__ == "__main__":
    main()
