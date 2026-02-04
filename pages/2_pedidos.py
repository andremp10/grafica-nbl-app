"""
Pedidos - Gestão de Pedidos da Gráfica
"""

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Pedidos | NBL",
    page_icon="📋",
    layout="wide"
)

# Dados mockados (depois conectar ao Supabase)
MOCK_ORDERS = [
    {"id": "101", "client": "Padaria Silva", "product": "Panfletos 5000un", "quantity": 5000, "status": "production", "dueDate": "2023-10-27", "price": 450.00, "priority": "Normal"},
    {"id": "102", "client": "Tech Solutions", "product": "Cartões de Visita Verniz Localizado", "quantity": 1000, "status": "production", "dueDate": "2023-10-27", "price": 180.00, "priority": "Alta"},
    {"id": "103", "client": "Dra. Ana Paula", "product": "Receituários 10 blocos", "quantity": 500, "status": "tomorrow", "dueDate": "2023-10-28", "price": 120.00, "priority": "Normal"},
    {"id": "104", "client": "Restaurante Gourmet", "product": "Cardápios PVC", "quantity": 20, "status": "tomorrow", "dueDate": "2023-10-28", "price": 850.00, "priority": "Alta"},
    {"id": "105", "client": "Evento Rock In Rio", "product": "Banners Lona 2x1m", "quantity": 5, "status": "next_7_days", "dueDate": "2023-11-02", "price": 1200.00, "priority": "Urgente"},
    {"id": "106", "client": "Loja de Roupas Chic", "product": "Sacolas Personalizadas", "quantity": 200, "status": "next_7_days", "dueDate": "2023-11-04", "price": 600.00, "priority": "Normal"},
    {"id": "107", "client": "Escola ABC", "product": "Apostilas encadernadas", "quantity": 50, "status": "production", "dueDate": "2023-10-27", "price": 320.00, "priority": "Baixa"},
    {"id": "108", "client": "Construtora Forte", "product": "Placas de Sinalização", "quantity": 30, "status": "production", "dueDate": "2023-10-29", "price": 1500.00, "priority": "Normal"},
    {"id": "109", "client": "Buffet Alegria", "product": "Convites de Casamento Luxo", "quantity": 150, "status": "next_7_days", "dueDate": "2023-11-05", "price": 890.00, "priority": "Alta"},
]

STATUS_MAP = {
    "production": "🔄 Em Produção",
    "tomorrow": "📅 Para Amanhã",
    "next_7_days": "📆 Próx. 7 Dias",
    "done": "✅ Concluído"
}

PRIORITY_COLORS = {
    "Urgente": "🔴",
    "Alta": "🟠",
    "Normal": "🟢",
    "Baixa": "⚪"
}

# Header
st.markdown("""
<h1 style="color: #1e293b; font-weight: 800;">📋 Gestão de Pedidos</h1>
<p style="color: #64748b; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.1em;">
    Fila de Produção NBL
</p>
""", unsafe_allow_html=True)

st.divider()

# Filtros
col_filter1, col_filter2, col_filter3 = st.columns(3)

with col_filter1:
    status_filter = st.selectbox(
        "🔍 Filtrar por Status",
        ["Todos", "Em Produção", "Para Amanhã", "Próx. 7 Dias"]
    )

with col_filter2:
    priority_filter = st.selectbox(
        "⚡ Filtrar por Prioridade",
        ["Todas", "Urgente", "Alta", "Normal", "Baixa"]
    )

with col_filter3:
    search = st.text_input("🔎 Buscar cliente/produto", placeholder="Digite para buscar...")

st.divider()

# Filtrar dados
filtered_orders = MOCK_ORDERS.copy()

if status_filter != "Todos":
    status_key = {
        "Em Produção": "production",
        "Para Amanhã": "tomorrow",
        "Próx. 7 Dias": "next_7_days"
    }.get(status_filter)
    if status_key:
        filtered_orders = [o for o in filtered_orders if o["status"] == status_key]

if priority_filter != "Todas":
    filtered_orders = [o for o in filtered_orders if o["priority"] == priority_filter]

if search:
    search_lower = search.lower()
    filtered_orders = [o for o in filtered_orders if search_lower in o["client"].lower() or search_lower in o["product"].lower()]

# Estatísticas
col_s1, col_s2, col_s3, col_s4 = st.columns(4)

with col_s1:
    st.metric("Total Filtrado", len(filtered_orders))

with col_s2:
    urgentes = len([o for o in filtered_orders if o["priority"] == "Urgente"])
    st.metric("🔴 Urgentes", urgentes)

with col_s3:
    total_value = sum(o["price"] for o in filtered_orders)
    st.metric("💰 Valor Total", f"R$ {total_value:,.2f}")

with col_s4:
    em_producao = len([o for o in filtered_orders if o["status"] == "production"])
    st.metric("🔄 Em Produção", em_producao)

st.divider()

# Lista de Pedidos
if not filtered_orders:
    st.warning("Nenhum pedido encontrado com os filtros selecionados.")
else:
    for order in filtered_orders:
        priority_emoji = PRIORITY_COLORS.get(order["priority"], "⚪")
        status_label = STATUS_MAP.get(order["status"], order["status"])
        
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([1, 2, 2, 1, 1])
            
            with col1:
                st.markdown(f"### #{order['id']}")
                st.caption(f"{priority_emoji} {order['priority']}")
            
            with col2:
                st.markdown(f"**{order['client']}**")
                st.caption(order["product"])
            
            with col3:
                st.markdown(f"📅 **{order['dueDate']}**")
                st.caption(f"Qtd: {order['quantity']:,} un")
            
            with col4:
                st.markdown(f"💰 **R$ {order['price']:.2f}**")
            
            with col5:
                st.markdown(status_label)
                if st.button("👁️ Ver", key=f"view_{order['id']}", use_container_width=True):
                    st.session_state.selected_order = order
            
            st.divider()

# Modal de detalhes
if "selected_order" in st.session_state and st.session_state.selected_order:
    order = st.session_state.selected_order
    
    with st.sidebar:
        st.subheader(f"📄 Pedido #{order['id']}")
        st.divider()
        
        st.markdown(f"**Cliente:** {order['client']}")
        st.markdown(f"**Produto:** {order['product']}")
        st.markdown(f"**Quantidade:** {order['quantity']:,}")
        st.markdown(f"**Entrega:** {order['dueDate']}")
        st.markdown(f"**Valor:** R$ {order['price']:.2f}")
        st.markdown(f"**Prioridade:** {order['priority']}")
        st.markdown(f"**Status:** {STATUS_MAP.get(order['status'], order['status'])}")
        
        st.divider()
        
        if st.button("❌ Fechar", use_container_width=True):
            st.session_state.selected_order = None
            st.rerun()
