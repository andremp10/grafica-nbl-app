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
    
    /* Status Badges */
    .status-badge {padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold;}
    .status-ok {background: rgba(16, 185, 129, 0.2); color: #10b981;}
    .status-warn {background: rgba(245, 158, 11, 0.2); color: #f59e0b;}
    .status-err {background: rgba(239, 68, 68, 0.2); color: #ef4444;}
</style>
""", unsafe_allow_html=True)

# --- 3. DADOS MOCKADOS (Baseado no Schema SQL) ---
def get_db_mock_orders():
    # Simula is_pedidos + is_clientes
    clients_pj = ["Padaria Estrela do Sul", "Construtora Mendes", "Academia PowerFit", "Escola O Pequeno Príncipe", "Restaurante Sabor Caseiro"]
    clients_pf = ["Ana Silva", "Carlos Oliveira", "Fernanda Santos", "Ricardo Souza"]
    
    products = ["Cartão de Visita 300g", "Panfleto A5 Couchê 115g", "Banner Lona 440g", "Adesivo Vinil Recorte", "Bloco de Notas Personalizado"]
    
    # Status baseados em is_extras_status ou fluxo real
    statuses = ["Aguardando Arte", "Em Produção (CTP)", "Em Produção (Impressão)", "Acabamento/Corte", "Pronto para Retirada", "Entregue"]
    
    data = []
    base_id = 2450
    
    for i in range(15):
        is_pj = random.random() > 0.3
        client = random.choice(clients_pj) if is_pj else random.choice(clients_pf)
        tipo_cliente = "PJ" if is_pj else "PF"
        
        status = random.choice(statuses)
        val = random.randint(100, 3500)
        
        data.append({
            "Pedido ID": f"#{base_id + i}",
            "Cliente": client,
            "Tipo": tipo_cliente,
            "Produto Principal": random.choice(products),
            "Valor": val,
            "Status": status,
            "Prazo": (datetime.now() + timedelta(days=random.randint(-2, 5))).strftime("%d/%m")
        })
    
    df = pd.DataFrame(data)
    return df

def get_finance_kpis():
    # Simula dados agregados de is_financeiro_lancamentos e is_financeiro_caixas
    return {
        "faturamento_mes": "R$ 68.450,00",
        "custos_fixos": "R$ 12.500,00",
        "custos_var": "R$ 24.300,00",
        "lucro_bruto": "R$ 31.650,00",
        "ticket_medio": "R$ 485,00" # Média de is_pedidos.total
    }

def get_daily_revenue():
    # Simula select sum(valor) from is_financeiro_lancamentos group by data
    dates = [(datetime.now() - timedelta(days=i)).strftime("%d/%m") for i in range(15)][::-1]
    values = [random.randint(2000, 8000) for _ in range(15)]
    return pd.DataFrame({"Data": dates, "Receita (R$)": values}).set_index("Data")

# --- 4. VIEWS ---

def render_instructions():
    st.markdown("### 📘 Manual do Usuário NBL Admin")
    st.markdown("""
    Bem-vindo ao **NBL Admin**, seu sistema integrado de gestão para gráficas. 
    Este manual descreve as funcionalidades da plataforma e como utilizá-las para maximizar sua produtividade.
    """)
    st.divider()

    # Seção 1: Visão Geral
    st.markdown("#### 1. Visão Geral")
    st.info("""
    O **NBL Admin** não é apenas um dashboard, é um **Sistema Especialista**. 
    Ele unifica o controle de produção (PCP), a gestão financeira e o atendimento ao cliente em uma interface simples, 
    potencializada por uma **Inteligência Artificial** que entende o contexto da sua gráfica.
    """)

    # Seção 2: Módulos do Sistema
    st.markdown("#### 2. Módulos do Sistema")
    
    with st.expander("💬 Assistente IA (Chat)", expanded=True):
        st.markdown("""
        O coração do sistema. Diferente de um chat comum, este assistente está **conectado ao seu banco de dados**.
        
        **O que ele faz:**
        - **Consulta Dados:** "Qual o status do pedido #2450?"
        - **Analisa Financeiro:** "Quanto faturei ontem?"
        - **Tira Dúvidas:** "Qual o prazo de entrega para banners?"
        
        **Limitações:**
        - Ele não pode *criar* novos pedidos (ainda). Apenas consulta e análise.
        """)
        
    with st.expander("🏭 PCP (Planejamento e Controle de Produção)"):
        st.markdown("""
        O módulo de status permite rastrear cada ordem de serviço no chão de fábrica.
        
        **Status Disponíveis:**
        - `Aguardando Arte`: Pedido entrou mas arquivo está pendente.
        - `Pré-Impressão`: Arquivo sendo analisado ou chapa sendo gravada.
        - `Em Produção`: Pedido em máquina (Offset/Digital/Plotter).
        - `Acabamento`: Fase final (corte, refile, dobra).
        - `Expedição`: Pronto para logística.
        """)

    with st.expander("💰 Controladoria Financeira"):
        st.markdown("""
        Visão gerencial para tomadores de decisão.
        
        - **KPIs:** Faturamento Bruto, Custos Variáveis, Margem de Contribuição.
        - **Fluxo de Caixa:** Gráfico diário de entradas para identificar tendências.
        - **Mix de Produtos:** Entenda quais categorias (Ex: Grandes Formatos vs Promocional) trazem mais receita.
        """)

    st.divider()

    # Seção 3: Guia de Uso (Dicas Práticas)
    st.markdown("#### 3. Como Usar Corretamente (Melhores Práticas)")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**✅ Seja Específico no Chat**")
        st.caption("A IA responde melhor a perguntas diretas.")
        st.code("Errado: Como estão as coisas?\nCerto: Quais pedidos estão atrasados hoje?")
        
        st.markdown("**✅ Use Filtros nos Dashboards**")
        st.caption("As tabelas possuem filtros de texto e categoria.")
        st.write("Para achar um cliente rápido, digite apenas uma parte do nome (ex: 'Padaria') na busca.")

    with c2:
        st.markdown("**✅ Verifique os Prazos**")
        st.caption("O sistema destaca prazos críticos.")
        st.write("Na tabela de produção, datas passadas ficam em destaque. Use isso para priorizar a fila de impressão.")

    st.markdown("#### 4. Suporte Técnico")
    st.markdown("Em caso de inconsistência de dados ou falha no sistema, entre em contato:")
    st.markdown("- **Email:** suporte@golfine.tech")
    st.markdown("- **Horário:** Seg-Sex, 08h às 18h")

def render_finance_view():
    st.markdown("### 💰 Controladoria Financeira")
    st.caption("Visão consolidada do fluxo de caixa e resultados.")
    st.divider()
    
    kpis = get_finance_kpis()
    
    # 1. KPIs
    c1, c2, c3, c4 = st.columns(4)
    def kpi_card(label, val, delta=None, color="up"):
        d = f'<div class="metric-delta {color}">{delta}</div>' if delta else ""
        return f'<div class="metric-box"><div class="metric-lbl">{label}</div><div class="metric-val">{val}</div>{d}</div>'
    
    with c1: st.markdown(kpi_card("Faturamento (Mês)", kpis["faturamento_mes"], "▲ 12%", "up"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card("Custos Variáveis", kpis["custos_var"], "▼ 5% (Economia)", "up"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card("Ticket Médio", kpis["ticket_medio"], None), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card("Lucro Bruto Est.", kpis["lucro_bruto"], "46% Margem"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # 2. Gráficos
    col_chart1, col_chart2 = st.columns([2, 1])
    with col_chart1:
        st.markdown("#### 📈 Entrada de Caixa")
        st.area_chart(get_daily_revenue(), color="#10b981", height=300)
    
    with col_chart2:
        st.markdown("#### 🍰 Receita por Categoria")
        st.dataframe(pd.DataFrame({
            "Categoria": ["Grandes Formatos", "Offset Promocional", "Digital Pequeno Porte", "Brindes"],
            "%" : ["40%", "35%", "15%", "10%"]
        }), use_container_width=True, hide_index=True)

def render_status_view():
    st.markdown("### 🏭 Chão de Fábrica (PCP)")
    st.caption("Acompanhamento da produção em tempo real.")
    st.divider()
    
    df = get_db_mock_orders()
    
    # Filtros
    c1, c2 = st.columns([3, 1])
    with c1: search = st.text_input("Buscar Pedido / Cliente", placeholder="Digite nome, empresa ou número do pedido...")
    with c2: filter_status = st.selectbox("Filtrar por Fase", ["Todos"] + list(df["Status"].unique()))
    
    if search:
        df = df[df["Cliente"].str.contains(search, case=False) | df["Pedido ID"].str.contains(search)]
    if filter_status != "Todos":
        df = df[df["Status"] == filter_status]
        
    st.dataframe(
        df,
        column_config={
            "Valor": st.column_config.NumberColumn(format="R$ %.2f"),
            "Status": st.column_config.TextColumn("Fase Atual"),
            "Prazo": st.column_config.TextColumn("Entrega Prevista")
        },
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
            align = "user" if msg["role"] == "user" else "assistant"
            with st.chat_message(align): st.markdown(msg["content"])

    if prompt := st.chat_input("Digite sua mensagem..."):
        st.session_state.pending_prompt = prompt
        st.rerun()

    if st.session_state.get("pending_prompt"):
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)
        
        with st.status("🚀 Consultando Base de Dados...", expanded=True) as status:
            time.sleep(1); status.write("🔍 Interpretando solicitação...")
            time.sleep(1); status.write("📡 Buscando informações atualizadas...")
            history = st.session_state.messages[:-1]
            response = send_message_to_n8n(prompt, history)
            status.update(label="✅ Resposta Gerada", state="complete", expanded=False)
            
        final_resp = response or "Erro ao processar."
        st.session_state.messages.append({"role": "assistant", "content": final_resp})
        with st.chat_message("assistant"): st.markdown(final_resp)
        st.rerun()

# --- 5. MAIN ---
def main():
    if "messages" not in st.session_state: st.session_state.messages = []
    if "current_view" not in st.session_state: st.session_state.current_view = "Chat"

    with st.sidebar:
        st.title("🎨 NBL Admin")
        st.caption("v4.4 • Conectado")
        st.divider()
        menu = {"💬 Chat": "Chat", "🏭 Status (PCP)": "Status", "💰 Financeiro": "Financeiro", "ℹ️ Instruções": "Instruções"}
        for k,v in menu.items():
            if st.button(k, use_container_width=True, type="primary" if st.session_state.current_view==v else "secondary"):
                st.session_state.current_view = v
                st.rerun()
        st.divider()
        st.caption("Desenvolvido por\n**Golfine Tecnologia**")
        if st.button("Limpar Chat"): st.session_state.messages = []; st.rerun()

    if st.session_state.current_view == "Chat": render_chat_view()
    elif st.session_state.current_view == "Status": render_status_view()
    elif st.session_state.current_view == "Financeiro": render_finance_view()
    elif st.session_state.current_view == "Instruções": render_instructions()

if __name__ == "__main__":
    main()
