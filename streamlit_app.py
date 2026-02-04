import streamlit as st
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from services.n8n_service import get_webhook_url, send_message_to_n8n

# --- 1. CONFIGURAÇÃO ---
load_dotenv()
st.set_page_config(
    page_title="Gráfica NBL Admin",
    page_icon="🎨",
    layout="wide",
    initial_sidebar_state="auto" # Auto colapsa em mobile
)

# --- 2. CSS ---
st.markdown("""
<style>
    .main .block-container {
        max-width: 1000px;
        padding-top: 2rem;
        padding-bottom: 5rem;
    }
    
    /* Hero Section Responsiva */
    .hero-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        min-height: 50vh; /* Altura mínima flexível */
        text-align: center;
        padding: 1rem;
    }
    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        color: #fff;
        margin-bottom: 1.5rem;
    }
    
    /* Mobile CSS Adjustments */
    @media (max-width: 768px) {
        .main .block-container {
            padding-top: 1rem;
            padding-bottom: 6rem; /* Mais espaço para input mobile */
        }
        .hero-title {
            font-size: 1.8rem; /* Título menor mobile */
        }
        .metric-box {
            padding: 1rem;
            margin-bottom: 0.5rem;
        }
        .stButton button {
            width: 100%; /* Botões full width */
        }
    }
    
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
    .prompt-card {background: #151515; border: 1px dashed #444; padding: 10px 15px; border-radius: 6px; font-family: monospace; color: #a5b4fc; margin-bottom: 8px; font-size: 0.9rem;}
    
</style>
""", unsafe_allow_html=True)

# --- 3. DADOS MOCKADOS AVANÇADOS (FINANCEIRO REALISTA) ---
def generate_financial_ledger():
    """Gera um livro razão (ledger) completo simulando is_financeiro_lancamentos"""
    
    # Categorias baseadas no negócio de gráfica
    categories = {
        "receitas": [
            ("Venda Balcão", 200, 1500), 
            ("Venda Online", 500, 3000), 
            ("Serviços de Arte", 50, 400),
            ("Grandes Formatos", 1000, 5000)
        ],
        "despesas_fixas": [
            ("Aluguel Galpão", 4500, 5),     # Dia 5
            ("Folha de Pagamento", 18000, 5), # Dia 5
            ("Internet/Telefone", 350, 10),  # Dia 10
            ("Sistema de Gestão", 299, 15),  # Dia 15
            ("Contabilidade", 1200, 20)      # Dia 20
        ],
        "despesas_var": [
            ("Fornecedor Papel", 1000, 5000),
            ("Manutenção Máquina", 500, 2000),
            ("Insumos Tinta/Toner", 800, 3000),
            ("Logística/Motoboy", 50, 300)
        ]
    }
    
    transactions = []
    base_date = datetime.now().replace(day=1) # Começo do mês atual
    days_in_month = 30
    
    for day in range(1, days_in_month + 1):
        current_date = base_date.replace(day=day)
        
        # 1. Gerar Receitas Diárias (Aleatórias, menos fim de semana)
        if current_date.weekday() < 6: # Seg-Sab
            num_sales = random.randint(3, 8)
            for _ in range(num_sales):
                cat, min_v, max_v = random.choice(categories["receitas"])
                val = random.randint(min_v, max_v)
                transactions.append({
                    "data": current_date,
                    "descricao": f"Pedido #{random.randint(1000, 9999)} - {cat}",
                    "categoria": cat,
                    "tipo": "Receita",
                    "valor": val
                })
                
        # 2. Gerar Despesas Fixas (Dias Específicos)
        for name, val, due_day in categories["despesas_fixas"]:
            if day == due_day:
                transactions.append({
                    "data": current_date,
                    "descricao": name,
                    "categoria": "Despesa Fixa",
                    "tipo": "Despesa",
                    "valor": val
                })
                
        # 3. Gerar Despesas Variáveis (Aleatórias)
        if random.random() > 0.7:
            item, min_v, max_v = random.choice(categories["despesas_var"])
            val = random.randint(min_v, max_v)
            transactions.append({
                "data": current_date,
                "descricao": f"Pagto {item}",
                "categoria": "Despesa Variável",
                "tipo": "Despesa",
                "valor": val
            })

    df = pd.DataFrame(transactions)
    df["data"] = pd.to_datetime(df["data"]).dt.strftime("%d/%m")
    return df

def get_db_mock_orders():
    # Simula is_pedidos
    clients = ["Restaurante Sabor", "Imob. Central", "Clínica Bem Estar", "Advocacia Silva", "Academia Fit"]
    statuses = ["Aguardando Arte", "Em Produção", "Acabamento", "Expedição", "Entregue"]
    data = []
    for i in range(15):
        data.append({
            "Pedido ID": f"#{2450+i}",
            "Cliente": random.choice(clients),
            "Valor": random.randint(150, 2500),
            "Status": random.choice(statuses),
            "Prazo": (datetime.now() + timedelta(days=random.randint(-2, 5))).strftime("%d/%m")
        })
    return pd.DataFrame(data)

# --- 4. VIEWS ---

def render_instructions():
    st.markdown("### 📘 Manual do Usuário NBL Admin")
    st.markdown("Bem-vindo ao **NBL Admin**, seu sistema integrado de gestão para gráficas. Este manual descreve as funcionalidades da plataforma.")
    st.divider()

    st.markdown("#### 1. Visão Geral")
    st.info("O **NBL Admin** não é apenas um dashboard, é um **Sistema Especialista**. Ele unifica o controle de produção (PCP), a gestão financeira e o atendimento ao cliente.")

    st.markdown("#### 2. Módulos do Sistema")
    with st.expander("💬 Assistente IA (Chat)", expanded=True):
        st.markdown("""
        **O que ele faz:**
        - **Consulta Dados:** "Qual o status do pedido #2450?"
        - **Analisa Financeiro:** "Quanto faturei ontem?"
        """)
    with st.expander("🏭 PCP (Produção)"):
        st.markdown("O módulo de status permite rastrear cada ordem de serviço: `Aguardando Arte`, `Pré-Impressão`, `Em Produção`, `Acabamento`, `Expedição`.")
    with st.expander("💰 Controladoria Financeira"):
        st.markdown("Visão gerencial de Fluxo de Caixa, KPIs e Resultados.")

    st.divider()
    st.markdown("#### 3. Melhores Práticas")
    col1, col2 = st.columns(2)
    with col1: st.write("**✅ Seja Específico:** Pergunte datas e nomes diretos."); st.write("**✅ Use Filtros:** As tabelas possuem busca.")
    with col2: st.write("**✅ Verifique Prazos:** Datas passadas ficam em destaque."); st.write("**✅ Suporte:** suporte@golfine.tech")

def render_finance_view():
    st.markdown("### 💰 Controladoria Financeira")
    st.caption("Análise detalhada de Fluxo de Caixa (Baseado em `is_financeiro_lancamentos`)")
    st.divider()
    
    # Gerar dados
    df = generate_financial_ledger()
    
    # Cálculos KPIs
    total_receita = df[df["tipo"]=="Receita"]["valor"].sum()
    total_despesa = df[df["tipo"]=="Despesa"]["valor"].sum()
    saldo = total_receita - total_despesa
    ticket_medio = df[df["tipo"]=="Receita"]["valor"].mean()
    
    # 1. KPIs Cards
    c1, c2, c3, c4 = st.columns(4)
    def kpi_card(label, val, delta=None, color="up"):
        d = f'<div class="metric-delta {color}">{delta}</div>' if delta else ""
        val_fmt = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f'<div class="metric-box"><div class="metric-lbl">{label}</div><div class="metric-val">{val_fmt}</div>{d}</div>'
    
    with c1: st.markdown(kpi_card("Faturamento Líquido", total_receita, "Entradas Totais", "up"), unsafe_allow_html=True)
    with c2: st.markdown(kpi_card("Despesas Totais", total_despesa, "Fixas + Variáveis", "down"), unsafe_allow_html=True)
    with c3: st.markdown(kpi_card("Resultado Operacional", saldo, "Lucro/Prejuízo", "up" if saldo > 0 else "down"), unsafe_allow_html=True)
    with c4: st.markdown(kpi_card("Ticket Médio", ticket_medio, "Por Venda", "up"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # 2. Charts Avançados
    col_chart1, col_chart2 = st.columns([2, 1])
    
    with col_chart1:
        st.markdown("#### 📊 Fluxo de Caixa Diário (Receita x Despesa)")
        
        # Pivot para gráfico de barras
        daily_data = df.groupby(["data", "tipo"])["valor"].sum().unstack().fillna(0)
        daily_data["Saldo"] = daily_data.get("Receita", 0) - daily_data.get("Despesa", 0)
        
        st.bar_chart(daily_data[["Receita", "Despesa"]], color=["#10b981", "#ef4444"], height=350, stack=False)
        
    with col_chart2:
        st.markdown("#### 📂 Composição de Receita")
        pie_data = df[df["tipo"]=="Receita"].groupby("categoria")["valor"].sum()
        st.dataframe(pie_data.to_frame(name="Valor Atual").style.format("R$ {:,.2f}"), use_container_width=True)

    st.markdown("#### 🧾 Extrato de Lançamentos (Últimos 10)")
    st.dataframe(
        df.tail(10).sort_index(ascending=False), 
        column_config={
            "valor": st.column_config.NumberColumn(format="R$ %.2f"),
            "data": "Data",
            "descricao": "Histórico",
            "tipo": st.column_config.Column("Tipo", width="small"),
        },
        use_container_width=True,
        hide_index=True
    )

def render_status_view():
    st.markdown("### 🏭 Chão de Fábrica (PCP)")
    st.caption("Visualização em tempo real da produção.")
    st.divider()
    df = get_db_mock_orders()
    search = st.text_input("Buscar Pedido", placeholder="Digite o nome ou ID...")
    if search: df = df[df["Cliente"].str.contains(search, case=False) | df["Pedido ID"].str.contains(search)]
    st.dataframe(df, use_container_width=True, hide_index=True)

def render_chat_view():
    # Inicializa estados se não existirem
    if "messages" not in st.session_state: st.session_state.messages = []
    
    # Renderiza histórico ANTES (sempre visível)
    if not st.session_state.messages:
        # Hero apenas se vazio
        st.markdown('<div class="hero-container"><div class="hero-title">Como posso ajudar?</div></div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        # Botões definem o prompt e forçam rerun apenas aqui
        if c1.button("📦 Meus Pedidos", use_container_width=True): 
             st.session_state.pending_prompt = "Status dos meus pedidos"
             st.rerun()
        if c2.button("💰 Faturamento", use_container_width=True): 
             st.session_state.pending_prompt = "Resumo financeiro do mês"
             st.rerun()
        if c3.button("📊 Relatórios", use_container_width=True): 
             st.session_state.pending_prompt = "Gerar relatório operacional"
             st.rerun()
    else:
        # Mostra mensagens anteriores
        for msg in st.session_state.messages:
            align = "user" if msg["role"] == "user" else "assistant"
            with st.chat_message(align): st.markdown(msg["content"])

    # Captura Input (Texto ou Botão Prévio)
    # A prioridade é: Texto digitado AGORA > Texto vindo de Botão (pending_prompt)
    
    user_input = st.chat_input("Digite sua mensagem...")
    prompt_to_process = None

    if user_input:
        prompt_to_process = user_input
    elif st.session_state.get("pending_prompt"):
        prompt_to_process = st.session_state.pending_prompt
        st.session_state.pending_prompt = None # Limpa imediatamente

    # Processamento Unificado (Síncrono/Linear)
    if prompt_to_process:
        # 1. Exibe msg do usuário imediatamente
        st.session_state.messages.append({"role": "user", "content": prompt_to_process})
        with st.chat_message("user"): 
            st.markdown(prompt_to_process)
        
        # 2. Processa resposta (Status Container)
        try:
            with st.status("🚀 Consultando Base de Dados...", expanded=True) as status:
                time.sleep(0.5); status.write("🔍 Interpretando solicitação...")
                history = st.session_state.messages[:-1]
                response = send_message_to_n8n(prompt_to_process, history)
                
                if not response:
                    raise Exception("Resposta vazia do n8n")
                    
                status.update(label="✅ Resposta Gerada", state="complete", expanded=False)
            
            # 3. Exibe e salva resposta
            st.session_state.messages.append({"role": "assistant", "content": response})
            with st.chat_message("assistant"): 
                st.markdown(response)
                
        except Exception as e:
            st.error(f"Erro ao conectar com o assistente: {str(e)}")

# --- 5. MAIN ---
def main():
    if "messages" not in st.session_state: st.session_state.messages = []
    if "current_view" not in st.session_state: st.session_state.current_view = "Chat"

    with st.sidebar:
        st.title("🎨 NBL Admin")
        st.caption("v5.1 • Conectado")
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
