"""
Compatibility layer for older imports.

This module re-exports repository functions from `data.supabase_repo`
to avoid divergent implementations in multiple files.
"""

from data.supabase_repo import (  # noqa: F401
    SupabaseRepo,
    fetch_financeiro,
    fetch_kpis_financeiro,
    fetch_kpis_pedidos,
    fetch_pedidos,
)

__all__ = [
    "SupabaseRepo",
    "fetch_pedidos",
    "fetch_kpis_pedidos",
    "fetch_financeiro",
    "fetch_kpis_financeiro",
]
