"""
Supabase Client - NBL Admin
Estabelece a conexão com o Supabase.
"""
import os
from typing import Iterable, Optional

import streamlit as st

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None

def _iter_key_variants(key: str) -> Iterable[str]:
    """Streamlit Cloud secrets are case-sensitive; accept common variants."""
    yield key
    upper = key.upper()
    lower = key.lower()
    if upper != key:
        yield upper
    if lower != key and lower != upper:
        yield lower


def _get_secret(key: str) -> Optional[str]:
    """Obtém secret do Streamlit Cloud ou variável de ambiente (com fallbacks)."""
    for variant in _iter_key_variants(key):
        # Secrets (Streamlit Cloud)
        try:
            value = st.secrets.get(variant)
        except Exception:
            value = None
        if value:
            return str(value)

        # Environment (local dev / CI)
        env_value = os.getenv(variant)
        if env_value:
            return env_value
    return None


@st.cache_resource
def _create_supabase_client(url: str, key: str) -> Client:
    # Cache keyed by (url, key). This prevents caching `None` forever when secrets
    # are added after first deploy/restart.
    return create_client(url, key)


def get_supabase_client() -> Optional[Client]:
    """Retorna cliente Supabase (cached por credenciais)."""
    if not SUPABASE_AVAILABLE:
        st.error("Biblioteca 'supabase' não instalada.")
        return None

    url = _get_secret("SUPABASE_URL") or _get_secret("supabase_url")

    # Prefer the least-privileged key for the Streamlit app.
    key = (
        _get_secret("SUPABASE_ANON_KEY")
        or _get_secret("SUPABASE_KEY")
        or _get_secret("SUPABASE_PUBLISHABLE_KEY")
        or _get_secret("SUPABASE_PUBLIC_KEY")
        or _get_secret("SUPABASE_ANON_KEY_LEGACY")
        or _get_secret("SUPABASE_SECRET_KEY")
        or _get_secret("SUPABASE_SERVICE_ROLE_KEY")
    )

    if not url or not key:
        # Tenta pegar das variaveis de ambiente setadas manualmente (local dev)
        return None

    try:
        return _create_supabase_client(url, key)
    except Exception as e:
        st.error(f"Erro ao conectar no Supabase: {e}")
        return None


def is_connected() -> bool:
    """Verifica se há conexão ativa com Supabase."""
    return get_supabase_client() is not None
