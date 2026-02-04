"""
Gráfica NBL Admin - Sistema de Gestão
Versão Streamlit
"""

import streamlit as st

# Configuração da página
st.set_page_config(
    page_title="Gráfica NBL Admin",
    page_icon="🖨️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado para estilo premium
st.markdown("""
<style>
    /* Cores principais */
    :root {
        --primary: #f97316;
        --primary-light: #fff7ed;
        --dark: #1e293b;
    }
    
    /* Header */
    .main-header {
        background: linear-gradient(135deg, #f97316 0%, #ea580c 100%);
        padding: 1.5rem 2rem;
        border-radius: 1rem;
        margin-bottom: 2rem;
        color: white;
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 1.8rem;
        font-weight: 800;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
        font-size: 0.9rem;
    }
    
    /* Cards de métricas */
    .metric-card {
        background: white;
        border: 1px solid #fed7aa;
        border-radius: 1rem;
        padding: 1.5rem;
        box-shadow: 0 4px 6px -1px rgba(249, 115, 22, 0.1);
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 800;
        color: #f97316;
    }
    
    .metric-label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #64748b;
        font-weight: 700;
    }
    
    /* Status badge */
    .status-online {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        background: #ecfdf5;
        color: #059669;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 700;
    }
    
    .status-online::before {
        content: "";
        width: 8px;
        height: 8px;
        background: #10b981;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: #1e293b;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
        color: white;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Header principal
st.markdown("""
<div class="main-header">
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
            <h1>🖨️ Sistema NBL</h1>
            <p>Centro de Inteligência Gráfica</p>
        </div>
        <div class="status-online">Servidor Online</div>
    </div>
</div>
""", unsafe_allow_html=True)

# Conteúdo principal - redireciona para páginas
st.info("👈 Selecione uma página no menu lateral para começar")

# Quick stats na home
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="📋 Fila Total", value="24", delta="Pedidos")

with col2:
    st.metric(label="✅ Eficiência", value="94%", delta="No Prazo")

with col3:
    st.metric(label="📦 Estoque", value="62%", delta="-5%")

with col4:
    st.metric(label="💰 Ticket Médio", value="R$ 680", delta="+12%")

st.divider()

# Links rápidos
st.subheader("⚡ Acesso Rápido")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📊 Ver Dashboard", use_container_width=True, type="primary"):
        st.switch_page("pages/1_dashboard.py")

with col2:
    if st.button("📋 Gerenciar Pedidos", use_container_width=True):
        st.switch_page("pages/2_pedidos.py")

with col3:
    if st.button("💬 Chat com IA", use_container_width=True):
        st.switch_page("pages/3_chat_ia.py")
