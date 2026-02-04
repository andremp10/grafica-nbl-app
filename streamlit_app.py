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
    .main .block-container {max-width: 1000px; padding-top: 2rem; padding-bottom: 5rem;}
    
    /* Cards Dashboard */
    .metric-container {
        background-color: #1a1a1a;
        border: 1px solid #333;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .metric-value {font-size: 28px; font-weight: bold; color: #fff;}
    .metric-label {font-size: 14px; color: #888; text-transform: uppercase;}
    .metric-delta {font-size: 14px; margin-top: 5px;}
    .up {color: #22c55e;}
    .down {color: #ef4444;}
    
    /* Footer */
    .footer {
        position: fixed; bottom: 10px; left: 20px;
        font-size: 12px; color: #555; pointer-events: none;
    }
    
    /* Quick Actions */
    .stButton button {width: 100%; border-radius: 8px;}
</style>
""", unsafe_allow_html=True)

# --- 3. DADOS MOCKADOS (GRÁFICA) ---
def get_mock_data():
    clients = ["Restaurante Sabor & Arte", "Imobiliária Central", "Clínica Bem Estar", "Advocacia Silva", "Academia Fit"]
    products = ["Cardápios A4", "Folders Triplos", "Cartões de Visita", "Banners 60x90", "Adesivos 5x5"]
    status_list = ["🎨 Arte", "🖨️ Impressão", "✂️ Acabamento", "✅ Entregue", "📦 Retirada"]
    
    data = []
    base_date = datetime.now()
    for i in range(25):
        data.append({
            "Pedido": f"#{2400+i}",
            "Cliente": random.choice(clients),
            "Produto": random.choice(products),
            "Valor": random.randint(150, 2500),
            "Status": random.choice(status_list),
            "Data": (base_date - timedelta(days=random.randint(0, 10))).strftime("%d/%m")
        })
    return pd.DataFrame(data)

df_orders = get_mock_data()

# --- 4. VIEWS DE DASHBOARD ---

def render_status_view():
    st.title("🏭 Status de Produção")
    st.divider()
    
    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown('<div class="metric-container"><div class="metric-value">08</div><div class="metric-label">Na Fila</div></div>', unsafe_allow_html=True)
    with c2: st.markdown('<div class="metric-container"><div class="metric-value">12</div><div class="metric-label">Em Produção</div></div>', unsafe_allow_html=True)
    with c3: st.markdown('<div class="metric-container"><div class="metric-value">03</div><div class="metric-label">Atrasados</div><div class="metric-delta down">⚠️ Atenção</div></div>', unsafe_allow_html=True)
    with c4: st.markdown('<div class="metric-container"><div class="metric-value">98%</div><div class="metric-label">No Prazo</div><div class="metric-delta up">▲ Excelente</div></div>', unsafe_allow_html=True)
    
    st.markdown("### 📋 Fila de Produção")
    
    # Filtros
    col1, col2 = st.columns([3, 1])
    with col1: query = st.text_input("Buscar cliente ou pedido", placeholder="Digite para filtrar...")
    with col2: st_filter = st.selectbox("Status", ["Todos"] + list(df_orders["Status"].unique()))
    
    filtered = df_orders
    if query: filtered = filtered[filtered["Cliente"].str.contains(query, case=False) | filtered["Pedido"].str.contains(query)]
    if st_filter != "Todos": filtered = filtered[filtered["Status"] == st_filter]
    
    st.dataframe(
        filtered,
        column_config={
            "Valor": st.column_config.NumberColumn(format="R$ %.2f"),
            "Status": st.column_config.TextColumn("Status", help="Fase atual")
        },
        use_container_width=True,
        hide_index=True
    )

def render_billing_view():
    st.title("💰 Financeiro")
    st.divider()
    
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown('<div class="metric-container"><div class="metric-value">R$ 14.5k</div><div class="metric-label">Faturamento Mês</div><div class="metric-delta up">▲ 12% vs mês anterior</div></div>', unsafe_allow_html=True)
    with c2: st.markdown('<div class="metric-container"><div class="metric-value">R$ 480</div><div class="metric-label">Ticket Médio</div></div>', unsafe_allow_html=True)
    with c3: st.markdown('<div class="metric-container"><div class="metric-value">R$ 2.8k</div><div class="metric-label">A Receber</div></div>', unsafe_allow_html=True)
    
    st.markdown("### 📈 Evolução de Vendas (30 dias)")
    chart_data = pd.DataFrame({
        "Data": [(datetime.now() - timedelta(days=i)).strftime("%d/%m") for i in range(15)][::-1],
        "Vendas": [random.randint(2000, 6000) for _ in range(15)]
    }).set_index("Data")
    st.line_chart(chart_data, color="#2563eb", height=300)

def render_instructions_view():
    st.title("ℹ️ Instruções e Ajuda")
    st.markdown("""
    ### Bem-vindo ao NBL Admin
    
    Este sistema foi desenvolvido para facilitar a gestão da Gráfica NBL.
    
    #### 🤖 Como usar o Assistente IA
    - O **chat** está conectado à base de conhecimento da empresa.
    - Você pode perguntar sobre **preços**, **prazos**, **status de pedidos** e **procedimentos**.
    - Use os botões de ação rápida para consultas frequentes.
    
    #### 📊 Dashboards
    - **Status de Pedidos**: Acompanhe o fluxo de produção em tempo real.
    - **Faturamento**: Visão financeira gerencial.
    
    #### 📞 Suporte
    - Desenvolvido por **Golfine Tecnologia**
    - Suporte técnico: (11) 99999-9999
    - Email: suporte@golfine.tech
    """)

# --- 5. CHAT & LOADING DINÂMICO ---

def render_chat_view():
    # Quick Actions
    c1, c2, c3 = st.columns(3)
    if c1.button("📦 Meus Pedidos"): st.session_state.pending_prompt = "Quais pedidos estão em produção hoje?"
    if c2.button("💰 Fechamento"): st.session_state.pending_prompt = "Quanto faturamos nesta semana?"
    if c3.button("📊 Relatório"): st.session_state.pending_prompt = "Gere um resumo da operação de ontem."
    
    # Mensagens
    for msg in st.session_state.messages:
        align = "user" if msg["role"] == "user" else "assistant"
        with st.chat_message(align):
            st.markdown(msg["content"])
            
    # Input
    if prompt := st.chat_input("Como posso ajudar?"):
        st.session_state.pending_prompt = prompt
        st.rerun()

    # Processamento Pending
    if st.session_state.get("pending_prompt"):
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        
        # User MSG
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)
        
        # STATUS LOADING ANIMADO
        with st.status("🚀 Iniciando assistente...", expanded=True) as status:
            time.sleep(1) # UX Timing
            status.write("🔍 Analisando sua solicitação...")
            time.sleep(1.5)
            status.write("📊 Consultando banco de dados...")
            time.sleep(1.5)
            status.write("🧠 Gerando resposta inteligente...")
            
            history = st.session_state.messages[:-1]
            response = send_message_to_n8n(prompt, history)
            
            status.update(label="✅ Resposta gerada!", state="complete", expanded=False)
            
        # Assistant MSG
        reply = response or "Ocorreu um erro ao processar."
        st.session_state.messages.append({"role": "assistant", "content": reply})
        with st.chat_message("assistant"): st.markdown(reply)

# --- 6. MAIN APP ---

def main():
    # Sidebar
    with st.sidebar:
        st.title("🎨 NBL Admin")
        st.caption("v4.1 • Golfine Tecnologia")
        st.divider()
        
        menu = {
            "💬 Assistente": "Chat",
            "🏭 Status": "Status",
            "💰 Financeiro": "Financeiro",
            "ℹ️ Instruções": "Instruções"
        }
        
        for label, view in menu.items():
            if st.button(label, use_container_width=True, type="primary" if st.session_state.get("current_view") == view else "secondary"):
                st.session_state.current_view = view
                st.rerun()
                
        st.markdown("<div style='flex:1'></div>", unsafe_allow_html=True)
        st.divider()
        st.caption("Desenvolvido por\n**Golfine Tecnologia**")
        if st.button("Limpar Chat"):
            st.session_state.messages = []
            st.rerun()

    # Init State
    if "current_view" not in st.session_state: st.session_state.current_view = "Chat"
    if "messages" not in st.session_state: st.session_state.messages = []
    
    # Routing
    view = st.session_state.current_view
    if view == "Chat": render_chat_view()
    elif view == "Status": render_status_view()
    elif view == "Financeiro": render_billing_view()
    elif view == "Instruções": render_instructions_view()

if __name__ == "__main__":
    main()
