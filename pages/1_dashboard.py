"""
Dashboard - Métricas e Produção
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(
    page_title="Dashboard | NBL",
    page_icon="📊",
    layout="wide"
)

# Dados mockados (depois conectar ao Supabase)
MOCK_ORDERS = [
    {"id": "101", "client": "Padaria Silva", "product": "Panfletos 5000un", "quantity": 5000, "status": "production", "price": 450.00, "priority": "Normal"},
    {"id": "102", "client": "Tech Solutions", "product": "Cartões de Visita", "quantity": 1000, "status": "production", "price": 180.00, "priority": "Alta"},
    {"id": "103", "client": "Dra. Ana Paula", "product": "Receituários 10 blocos", "quantity": 500, "status": "tomorrow", "price": 120.00, "priority": "Normal"},
    {"id": "104", "client": "Restaurante Gourmet", "product": "Cardápios PVC", "quantity": 20, "status": "tomorrow", "price": 850.00, "priority": "Alta"},
    {"id": "105", "client": "Evento Rock In Rio", "product": "Banners Lona 2x1m", "quantity": 5, "status": "next_7_days", "price": 1200.00, "priority": "Urgente"},
]

PRODUCTION_FLOW = [
    {"stage": "Pré-Impressão", "count": 12, "percent": 85},
    {"stage": "Produção (Fila)", "count": 9, "percent": 60},
    {"stage": "Acabamento", "count": 5, "percent": 40},
    {"stage": "Expedição", "count": 3, "percent": 20},
]

SECTOR_LOAD = [
    {"name": "Setor Offset", "load": 88, "status": "Crítico"},
    {"name": "Impressão Digital", "load": 45, "status": "Estável"},
    {"name": "Comunicação Visual", "load": 72, "status": "Alerta"},
    {"name": "Corte e Vinco", "load": 15, "status": "Ocioso"},
]

INVENTORY = [
    {"item": "Papel Couché 150g", "quantity": "5000 fls", "status": "OK"},
    {"item": "Papel Supremo 300g", "quantity": "200 fls", "status": "Baixo"},
    {"item": "Tinta Ciano (Offset)", "quantity": "2 Latas", "status": "Crítico"},
    {"item": "Lona Vinílica", "quantity": "3 Rolos", "status": "OK"},
]

# Header
st.markdown("""
<h1 style="color: #1e293b; font-weight: 800;">📊 Status de Produção</h1>
<p style="color: #64748b; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.1em;">
    Monitoramento NBL em Tempo Real
</p>
""", unsafe_allow_html=True)

st.divider()

# KPI Row
col1, col2, col3, col4 = st.columns(4)

total_orders = len(MOCK_ORDERS) + 15
avg_ticket = sum(o["price"] for o in MOCK_ORDERS) / len(MOCK_ORDERS)

with col1:
    st.metric("📋 Fila Total", f"{total_orders}", "Pedidos")

with col2:
    st.metric("✅ Eficiência", "94%", "No Prazo")

with col3:
    st.metric("📦 Materiais", "62%", "Estoque")

with col4:
    st.metric("💰 Ticket Médio", f"R$ {avg_ticket:.0f}", "+12%")

st.divider()

# Layout principal
col_left, col_right = st.columns(2)

with col_left:
    # Funil Produtivo
    st.subheader("🔄 Funil Produtivo")
    
    df_flow = pd.DataFrame(PRODUCTION_FLOW)
    
    fig = go.Figure(go.Funnel(
        y=df_flow["stage"],
        x=df_flow["count"],
        textinfo="value+percent initial",
        marker={"color": ["#94a3b8", "#f97316", "#fb923c", "#10b981"]}
    ))
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Alertas de Estoque
    st.subheader("⚠️ Insumos Críticos")
    
    critical_items = [i for i in INVENTORY if i["status"] != "OK"]
    
    if critical_items:
        for item in critical_items:
            status_color = "🔴" if item["status"] == "Crítico" else "🟡"
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between; padding: 0.75rem; 
                        background: #fef2f2; border-radius: 0.5rem; margin-bottom: 0.5rem;">
                <span>{item['item']}</span>
                <span>{status_color} {item['quantity']} ({item['status']})</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.success("Nenhum alerta de estoque!")

with col_right:
    # Uso de Máquinas
    st.subheader("⚙️ Uso de Máquinas")
    
    for sector in SECTOR_LOAD:
        status_emoji = "🔴" if sector["status"] == "Crítico" else "🟢" if sector["status"] == "Estável" else "🟡"
        
        col_name, col_bar, col_status = st.columns([2, 3, 1])
        
        with col_name:
            st.markdown(f"**{sector['name']}**")
        
        with col_bar:
            bar_color = "#ef4444" if sector["load"] > 80 else "#f97316"
            st.progress(sector["load"] / 100)
        
        with col_status:
            st.markdown(f"{status_emoji} {sector['load']}%")
    
    st.divider()
    
    # Visão Financeira
    st.subheader("💰 Visão Financeira")
    
    col_f1, col_f2 = st.columns(2)
    
    with col_f1:
        st.metric("Receita Mês", "R$ 89.2k", "+12%")
        
    with col_f2:
        st.metric("Receita Dia", "R$ 3.4k")
    
    col_f3, col_f4 = st.columns(2)
    
    with col_f3:
        st.metric("Pendente", "R$ 4.5k", "-8%")
        
    with col_f4:
        st.metric("Margem", "32%", "+2%")
