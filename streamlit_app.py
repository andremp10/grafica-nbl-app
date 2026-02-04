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

# --- 2. CSS ---
st.markdown("""
<style>
    .main .block-container {max-width: 1000px; padding-top: 2rem; padding-bottom: 5rem;}
    
    /* Hero */
    .hero-container {display: flex; flex-direction: column; align-items: center; justify-content: center; height: 60vh; text-align: center;}
    .hero-title {font-size: 2.5rem; font-weight: 700; color: #fff; margin-bottom: 2rem;}
    
    /* Metrics */
    .metric-box {
        background: #151515; border: 1px solid #2a2a2a; border-radius: 12px; padding: 1.5rem; text-align: center;
        transition: transform 0.1s;
    }
    .metric-box:hover {transform: translateY(-2px); border-color: #333;}
    .metric-val {font-size: 2rem; font-weight: bold; color:white; margin: 0.5rem 0;}
    .metric-lbl {font-size: 0.85rem; color: #888; letter-spacing: 0.5px; text-transform: uppercase;}
    .metric-delta {font-size: 0.9rem; font-weight: 500;}
    .up {color: #10b981;} .down {color: #ef4444;}
    
    /* Instruções */
    .guide-box {background: #1a1a1a; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem; border-left: 4px solid #2563eb;}
    .prompt-card {background: #151515; border: 1px dashed #444; padding: 10px 15px; border-radius: 6px; font-family: monospace; color: #a5b4fc; margin-bottom: 8px;}
</style>
""", unsafe_allow_html=True)

# --- 3. DADOS MOCKADOS AVANÇADOS ---
def get_finance_data():
    dates = [(datetime.now() - timedelta(days=i)).strftime("%d/%m") for i in range(30)][::-1]
    revenue = [random.randint(2000, 5000) for _ in range(30)]
    return pd.DataFrame({"Data": dates, "Faturamento": revenue}).set_index("Data")

def get_product_mix():
    return pd.DataFrame({
        "Categoria": ["Editorial", "Comercial", "Promocional", "Brindes", "Grandes Formatos"],
        "Valor": [15000, 28000, 12000, 5400, 8900]
    })

def get_product_mix():
    return pd.DataFrame({
        "Categoria": ["Editorial", "Comercial", "Promocional", "Brindes", "Grandes Formatos"],
        "Valor": [15000, 28000, 12000, 5400, 8900]
    })

# --- 4. VIEWS ---

def render_instructions():
    st.markdown("### 📚 Guia de Uso do Sistema")
    st.markdown("Aprenda a extrair o máximo do seu assistente IA e dos dashboards.")
    st.divider()
    
    col1, col2 = st.columns([1.5, 1])
    
    with col1:
        st.markdown("#### 🤖 Capacidades do Agente")
        with st.expander("🔍 Consultas de Status", expanded=True):
            st.write("O agente conecta-se ao banco de dados em tempo real.")
            st.markdown('<div class="prompt-card">Qual o status do pedido da Padaria Pão Quente?</div>', unsafe_allow_html=True)
            st.markdown('<div class="prompt-card">O pedido #2405 já foi entregue?</div>', unsafe_allow_html=True)
            
        with st.expander("💰 Orçamentos e Preços"):
            st.write("O agente conhece a tabela de preços atualizada.")
            st.markdown('<div class="prompt-card">Quanto custa 1000 cartões couche 300g?</div>', unsafe_allow_html=True)
            st.markdown('<div class="prompt-card">Me dê o preço de 50 banners 60x90.</div>', unsafe_allow_html=True)
            
        with st.expander("📊 Análise Gerencial"):
            st.write("Peça resumos e insights.")
            st.markdown('<div class="prompt-card">Qual foi o faturamento da semana passada?</div>', unsafe_allow_html=True)
            st.markdown('<div class="prompt-card">Quais são os 3 maiores clientes deste mês?</div>', unsafe_allow_html=True)

    with col2:
        st.markdown("#### 🏭 Fluxo da Gráfica")
        st.markdown("""
        Entenda as etapas que aparecem nos dashboards:
        
        1. **⏳ Arte / Aprovação**: Arquivo recebido, aguardando validação do cliente ou pré-impressão.
        2. **🖥️ CTP / Pré-press**: Gravação de chapas ou preparação de arquivo digital.
        3. **🖨️ Impressão**: Produção rodando em máquina (Offset/Digital).
        4. **✂️ Acabamento**: Corte, refile, verniz, dobra, encadernação.
        5. **📦 Expedição**: Pronto para retirada ou rota de entrega.
        """)
        st.info("💡 **Dica:** Se um pedido estiver 'na fila' por muito tempo, pergunte ao agente o motivo!")

def render_finance_view():
    st.markdown("### 💰 Análise Financeira")
    st.caption("Visão gerencial de receitas, custos e margens.")
    st.divider()
    
    # 1. KPIs Estratégicos
    c1, c2, c3, c4 = st.columns(4)
    def kpi(label, val, delta, d_color):
        return f'<div class="metric-box"><div class="metric-lbl">{label}</div><div class="metric-val">{val}</div><div class="metric-delta {d_color}">{delta}</div></div>'
    
    with c1: st.markdown(kpi("Faturamento (Mês)", "R$ 69.3k", "▲ 15%", "up"), unsafe_allow_html=True)
    with c2: st.markdown(kpi("Custos Variáveis", "R$ 27.7k", "▼ 40%", "down"), unsafe_allow_html=True)
    with c3: st.markdown(kpi("Margem Contrib.", "R$ 41.6k", "60%", "up"), unsafe_allow_html=True)
    with c4: st.markdown(kpi("Ticket Médio", "R$ 480", "▲ R$ 20", "up"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # 2. Gráficos
    col_chart1, col_chart2 = st.columns([2, 1])
    
    with col_chart1:
        st.markdown("#### 📈 Evolução Diária")
        st.area_chart(get_finance_data(), color="#2563eb", height=300)
    
    with col_chart2:
        st.markdown("#### 🥯 Mix de Produtos")
        df_mix = get_product_mix()
        st.dataframe(
            df_mix.style.format({"Valor": "R$ {:,.2f}"}).background_gradient(cmap="Blues"),
            use_container_width=True,
            hide_index=True
        )

    st.markdown("---")
    st.markdown("#### ⭐ Top Clientes (Pareto 80/20)")
    st.dataframe(
        pd.DataFrame([
            {"Cliente": "Rede Supermercados Bom Preço", "Pedidos": 12, "Total": "R$ 15.400"},
            {"Cliente": "Construtora Horizonte", "Pedidos": 5, "Total": "R$ 8.900"},
            {"Cliente": "Colégio Saber", "Pedidos": 3, "Total": "R$ 5.200"},
            {"Cliente": "Agência Criativa Marketing", "Pedidos": 20, "Total": "R$ 4.800"}
        ]),
        use_container_width=True,
        hide_index=True
    )

def render_chat_view():
    if not st.session_state.messages:
        st.markdown('<div class="hero-container"><div class="hero-title">Como posso ajudar?</div></div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        if c1.button("📦 Meus Pedidos", use_container_width=True): st.session_state.pending_prompt = "Status dos meus pedidos"
        if c2.button("💰 Faturamento", use_container_width=True): st.session_state.pending_prompt = "Resumo financeiro do mês"
        if c3.button("📊 Relatórios", use_container_width=True): st.session_state.pending_prompt = "Gerar relatório operacional"
        if st.session_state.get("pending_prompt"): st.rerun()
                
    else:
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]): st.markdown(msg["content"])

    if prompt := st.chat_input("Digite sua mensagem..."):
        st.session_state.pending_prompt = prompt
        st.rerun()

    if st.session_state.get("pending_prompt"):
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)
        
        with st.status("🚀 Processando...", expanded=True) as status:
            time.sleep(1); status.write("🔍 Analisando contexto...")
            time.sleep(1); status.write("📊 Consultando dados...")
            history = st.session_state.messages[:-1]
            response = send_message_to_n8n(prompt, history)
            status.update(label="✅ Concluído", state="complete", expanded=False)
            
        final_resp = response or "Erro ao processar."
        st.session_state.messages.append({"role": "assistant", "content": final_resp})
        with st.chat_message("assistant"): st.markdown(final_resp)
        st.rerun()

def render_status_view():
    st.markdown("### 🏭 Status de Produção")
    st.caption("Acompanhamento de chão de fábrica em tempo real.")
    st.divider()
    c1,c2,c3,c4 = st.columns(4)
    # Reutilizando mock data simples para brevidade, mas com UI melhor
    st.dataframe(pd.DataFrame([
        {"Pedido": "#2401", "Cliente": "Restaurante Sabor", "Fase": "Expedição", "Status": "✅ Pronto"},
        {"Pedido": "#2402", "Cliente": "Imob. Central", "Fase": "Impressão", "Status": "🔄 Rodando"},
        {"Pedido": "#2403", "Cliente": "Clínica Bem Estar", "Fase": "Pré-press", "Status": "⏳ Aguardando Chapa"},
    ]), use_container_width=True)

# --- 5. MAIN ---
def main():
    if "messages" not in st.session_state: st.session_state.messages = []
    if "current_view" not in st.session_state: st.session_state.current_view = "Chat"

    with st.sidebar:
        st.title("🎨 NBL Admin")
        st.caption("v4.3")
        st.divider()
        menu = {"💬 Chat": "Chat", "🏭 Status": "Status", "💰 Financeiro": "Financeiro", "ℹ️ Instruções": "Instruções"}
        for k,v in menu.items():
            if st.button(k, use_container_width=True, type="primary" if st.session_state.current_view==v else "secondary"):
                st.session_state.current_view = v
                st.rerun()
        st.divider()
        st.caption("Desenvolvido por\n**Golfine Tecnologia**")
        if st.button("Limpar"): st.session_state.messages = []; st.rerun()

    if st.session_state.current_view == "Chat": render_chat_view()
    elif st.session_state.current_view == "Status": render_status_view()
    elif st.session_state.current_view == "Financeiro": render_finance_view()
    elif st.session_state.current_view == "Instruções": render_instructions()

if __name__ == "__main__":
    main()
