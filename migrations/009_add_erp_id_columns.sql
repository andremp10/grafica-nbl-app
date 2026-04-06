-- =============================================================================
-- MIGRATION 009: Add ERP legacy identifiers to ETL tables
-- Data: 2026-04-05
-- Objetivo: preservar o id legado MySQL em colunas dedicadas `erp_id`
-- =============================================================================

ALTER TABLE public.is_clientes
    ADD COLUMN IF NOT EXISTS erp_id bigint;

ALTER TABLE public.is_financeiro_funcionarios
    ADD COLUMN IF NOT EXISTS erp_id bigint;

ALTER TABLE public.is_produtos
    ADD COLUMN IF NOT EXISTS erp_id bigint;

ALTER TABLE public.is_financeiro_lancamentos
    ADD COLUMN IF NOT EXISTS erp_id bigint;

ALTER TABLE public.is_pedidos
    ADD COLUMN IF NOT EXISTS erp_id bigint;

ALTER TABLE public.is_pedidos_itens
    ADD COLUMN IF NOT EXISTS erp_id bigint;

ALTER TABLE public.is_pedidos_pagamentos
    ADD COLUMN IF NOT EXISTS erp_id bigint;

ALTER TABLE public.is_pedidos_historico
    ADD COLUMN IF NOT EXISTS erp_id bigint;
