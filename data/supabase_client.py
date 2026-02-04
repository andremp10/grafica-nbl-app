"""
Supabase Client - NBL Admin
Estabelece a conexão com o Supabase.
"""
import os
from typing import Optional
import streamlit as st

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None

def _get_secret(key: str) -> Optional[str]:
    """Obtém secret do Streamlit Cloud ou variável de ambiente."""
    try:
        value = st.secrets.get(key)
    except Exception:
        value = None
    if value is None:
        return os.getenv(key)
    return str(value) if value else None


@st.cache_resource
def get_supabase_client() -> Optional[Client]:
    """Retorna cliente Supabase singleton (cached)."""
    if not SUPABASE_AVAILABLE:
        st.error("Biblioteca 'supabase' não instalada.")
        return None

    url = _get_secret("SUPABASE_URL")
    key = _get_secret("SUPABASE_KEY") or _get_secret("SUPABASE_ANON_KEY")

    if not url or not key:
        # Tenta pegar das variaveis de ambiente setadas manualmente (local dev)
        return None

    try:
        return create_client(url, key)
    except Exception as e:
        st.error(f"Erro ao conectar no Supabase: {e}")
        return None


def is_connected() -> bool:
    """Verifica se há conexão ativa com Supabase."""
    return get_supabase_client() is not None
