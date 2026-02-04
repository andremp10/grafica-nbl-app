"""
Chat IA - Assistente Inteligente NBL
Com animação de "pensando" enquanto aguarda resposta do webhook
"""

import streamlit as st
import requests
import time

st.set_page_config(
    page_title="Chat IA | NBL",
    page_icon="💬",
    layout="wide"
)

# URL do webhook - será configurado pelo usuário
WEBHOOK_URL = st.secrets.get("WEBHOOK_URL", "")

# CSS customizado para o chat
st.markdown("""
<style>
    .chat-container {
        max-width: 800px;
        margin: 0 auto;
    }
    
    .message {
        padding: 1rem 1.5rem;
        border-radius: 1rem;
        margin-bottom: 1rem;
        max-width: 80%;
    }
    
    .user-message {
        background: linear-gradient(135deg, #f97316 0%, #ea580c 100%);
        color: white;
        margin-left: auto;
        text-align: right;
        border-bottom-right-radius: 0.25rem;
    }
    
    .bot-message {
        background: #f1f5f9;
        color: #1e293b;
        margin-right: auto;
        border-bottom-left-radius: 0.25rem;
    }
    
    .thinking-animation {
        display: flex;
        gap: 0.25rem;
        padding: 1rem;
    }
    
    .thinking-dot {
        width: 8px;
        height: 8px;
        background: #f97316;
        border-radius: 50%;
        animation: bounce 1.4s ease-in-out infinite;
    }
    
    .thinking-dot:nth-child(1) { animation-delay: 0s; }
    .thinking-dot:nth-child(2) { animation-delay: 0.2s; }
    .thinking-dot:nth-child(3) { animation-delay: 0.4s; }
    
    @keyframes bounce {
        0%, 60%, 100% { transform: translateY(0); }
        30% { transform: translateY(-10px); }
    }
    
    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    
    .status-connected {
        background: #ecfdf5;
        color: #059669;
    }
    
    .status-disconnected {
        background: #fef2f2;
        color: #dc2626;
    }
</style>
""", unsafe_allow_html=True)

# Inicializar histórico de mensagens
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "👋 Bem-vindo ao centro de inteligência gráfica NBL. Estou monitorando o fluxo lateral para te auxiliar. O que deseja consultar?"
        }
    ]

if "is_thinking" not in st.session_state:
    st.session_state.is_thinking = False

# Header
col_header, col_status = st.columns([3, 1])

with col_header:
    st.markdown("""
    <h1 style="color: #1e293b; font-weight: 800;">💬 Assistente NBL</h1>
    <p style="color: #64748b; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.1em;">
        Chat com Inteligência Artificial
    </p>
    """, unsafe_allow_html=True)

with col_status:
    if WEBHOOK_URL:
        st.markdown('<span class="status-badge status-connected">🟢 Conectado</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-badge status-disconnected">🔴 Webhook não configurado</span>', unsafe_allow_html=True)

st.divider()

# Container de mensagens
chat_container = st.container()

with chat_container:
    # Exibir histórico de mensagens
    for message in st.session_state.messages:
        if message["role"] == "user":
            with st.chat_message("user", avatar="👤"):
                st.markdown(message["content"])
        else:
            with st.chat_message("assistant", avatar="🤖"):
                st.markdown(message["content"])
    
    # Mostrar animação de pensando
    if st.session_state.is_thinking:
        with st.chat_message("assistant", avatar="🤖"):
            st.markdown("""
            <div class="thinking-animation">
                <div class="thinking-dot"></div>
                <div class="thinking-dot"></div>
                <div class="thinking-dot"></div>
            </div>
            <p style="color: #64748b; font-size: 0.875rem; margin-top: 0.5rem;">
                🧠 Processando sua solicitação...
            </p>
            """, unsafe_allow_html=True)


def send_to_webhook(message: str, history: list) -> str:
    """
    Envia mensagem para o webhook externo e retorna a resposta.
    """
    if not WEBHOOK_URL:
        return "⚠️ Webhook não configurado. Por favor, configure a URL do webhook nas secrets do Streamlit."
    
    try:
        payload = {
            "message": message,
            "history": history,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        response = requests.post(
            WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60  # Timeout de 60 segundos
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get("response", data.get("message", str(data)))
        else:
            return f"❌ Erro na comunicação: Status {response.status_code}"
            
    except requests.exceptions.Timeout:
        return "⏱️ Timeout: O servidor demorou muito para responder."
    except requests.exceptions.RequestException as e:
        return f"❌ Erro de conexão: {str(e)}"
    except Exception as e:
        return f"❌ Erro inesperado: {str(e)}"


# Input do chat
if prompt := st.chat_input("Digite sua mensagem...", disabled=st.session_state.is_thinking):
    # Adicionar mensagem do usuário
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Mostrar mensagem do usuário
    with chat_container:
        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)
    
    # Ativar animação de pensando
    st.session_state.is_thinking = True
    st.rerun()

# Processar resposta se estiver pensando
if st.session_state.is_thinking:
    # Preparar histórico para o webhook
    history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[:-1]]
    last_message = st.session_state.messages[-1]["content"]
    
    # Enviar para webhook
    with st.spinner(""):
        response = send_to_webhook(last_message, history)
    
    # Adicionar resposta ao histórico
    st.session_state.messages.append({"role": "assistant", "content": response})
    
    # Desativar animação
    st.session_state.is_thinking = False
    st.rerun()

# Sidebar com configurações
with st.sidebar:
    st.subheader("⚙️ Configurações")
    st.divider()
    
    st.markdown("**Status do Webhook:**")
    if WEBHOOK_URL:
        st.success(f"✅ Configurado")
        st.caption(f"URL: {WEBHOOK_URL[:30]}...")
    else:
        st.error("❌ Não configurado")
        st.caption("Configure `WEBHOOK_URL` nas secrets do Streamlit Cloud")
    
    st.divider()
    
    if st.button("🗑️ Limpar Histórico", use_container_width=True):
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": "👋 Histórico limpo. Como posso ajudar?"
            }
        ]
        st.rerun()
    
    st.divider()
    
    st.markdown("**📊 Estatísticas:**")
    st.markdown(f"- Mensagens: {len(st.session_state.messages)}")
    user_msgs = len([m for m in st.session_state.messages if m["role"] == "user"])
    st.markdown(f"- Suas perguntas: {user_msgs}")
