-- =============================================================================
-- MIGRATION 002: Dashboard Views + RPC + Grants
-- Data: 2026-02-04
-- Objetivo:
--   1) Garantir schema esperado pelo app Streamlit (PCP e Financeiro)
--   2) Garantir RPC get_finance_kpis(start_date, end_date)
--   3) Garantir permissoes para anon/authenticated
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VIEW: vw_dashboard_pedidos
-- Granularidade: 1 linha por pedido
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_dashboard_pedidos AS
WITH agg_itens AS (
    SELECT
        pedido_id,
        COUNT(*)::integer AS qtde_itens,
        COALESCE(SUM(valor * COALESCE(qtde, 1)), 0) AS valor_itens,
        MIN(previsao_producao) AS data_prazo,
        MAX(previsao_entrega) AS data_entrega,
        string_agg(DISTINCT status, ', ' ORDER BY status) AS status_agregado
    FROM public.is_pedidos_itens
    GROUP BY pedido_id
),
cliente_nome AS (
    SELECT
        c.id AS cliente_id,
        COALESCE(
            NULLIF(TRIM(COALESCE(pf.nome, '') || ' ' || COALESCE(pf.sobrenome, '')), ''),
            pj.razao_social,
            'Cliente #' || LEFT(c.id::text, 8)
        ) AS nome
    FROM public.is_clientes c
    LEFT JOIN public.is_clientes_pf pf ON pf.cliente_id = c.id
    LEFT JOIN public.is_clientes_pj pj ON pj.cliente_id = c.id
)
SELECT
    p.id AS pedido_id,
    p.cliente_id,
    cn.nome AS cliente_nome,
    p.created_at AS data_criacao,
    COALESCE(ai.data_entrega, ai.data_prazo) AS data_prazo_validada,
    COALESCE(ai.status_agregado, 'Sem Status') AS status_pedido,
    COALESCE(ai.qtde_itens, 0)::integer AS qtde_itens,
    p.total AS valor_total,
    p.frete_valor,
    CASE
        WHEN COALESCE(ai.status_agregado, '') ILIKE '%entregue%'
            OR COALESCE(ai.status_agregado, '') ILIKE '%finalizado%'
            OR COALESCE(ai.status_agregado, '') ILIKE '%concluid%'
            OR COALESCE(ai.status_agregado, '') ILIKE '%cancelad%'
        THEN TRUE
        ELSE FALSE
    END AS is_finalizado,
    CASE
        WHEN COALESCE(ai.data_entrega, ai.data_prazo) IS NOT NULL
             AND COALESCE(ai.data_entrega, ai.data_prazo) < CURRENT_TIMESTAMP
             AND NOT (
                COALESCE(ai.status_agregado, '') ILIKE '%entregue%'
                OR COALESCE(ai.status_agregado, '') ILIKE '%finalizado%'
                OR COALESCE(ai.status_agregado, '') ILIKE '%concluid%'
                OR COALESCE(ai.status_agregado, '') ILIKE '%cancelad%'
             )
        THEN TRUE
        ELSE FALSE
    END AS is_atrasado,
    CASE
        WHEN COALESCE(ai.data_entrega, ai.data_prazo) IS NOT NULL
             AND COALESCE(ai.data_entrega, ai.data_prazo) < CURRENT_TIMESTAMP
             AND NOT (
                COALESCE(ai.status_agregado, '') ILIKE '%entregue%'
                OR COALESCE(ai.status_agregado, '') ILIKE '%finalizado%'
                OR COALESCE(ai.status_agregado, '') ILIKE '%concluid%'
                OR COALESCE(ai.status_agregado, '') ILIKE '%cancelad%'
             )
        THEN GREATEST(0, CURRENT_DATE - COALESCE(ai.data_entrega, ai.data_prazo)::date)
        ELSE 0
    END AS dias_em_atraso
FROM public.is_pedidos p
LEFT JOIN agg_itens ai ON ai.pedido_id = p.id
LEFT JOIN cliente_nome cn ON cn.cliente_id = p.cliente_id;


-- -----------------------------------------------------------------------------
-- VIEW: vw_dashboard_financeiro
-- Granularidade: 1 linha por lancamento
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_dashboard_financeiro AS
SELECT
    fl.id AS lancamento_id,
    CASE fl.tipo
        WHEN 1 THEN 'Entrada'
        WHEN 2 THEN 'Saída'
        ELSE 'Outro'
    END AS tipo,
    fl.descricao,
    fl.valor,
    fl.data AS data_vencimento,
    fl.data_pagto AS data_pagamento,
    fl.data_emissao,
    fl.status AS status_codigo,
    CASE fl.status
        WHEN 0 THEN 'Cancelado'
        WHEN 1 THEN 'Aberto'
        WHEN 2 THEN 'Pago'
        ELSE 'Status ' || fl.status::text
    END AS status_texto,
    COALESCE(cat.titulo, 'Sem Categoria') AS categoria,
    DATE_TRUNC('month', COALESCE(fl.data, fl.data_emissao, fl.data_pagto))::date AS competencia_mes,
    CASE
        WHEN fl.status = 1
             AND fl.data IS NOT NULL
             AND fl.data < CURRENT_TIMESTAMP
        THEN TRUE
        ELSE FALSE
    END AS is_atrasado,
    CASE
        WHEN fl.status = 2 OR fl.data_pagto IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_realizado
FROM public.is_financeiro_lancamentos fl
LEFT JOIN public.is_financeiro_categorias cat ON cat.id = fl.categoria_id;


-- -----------------------------------------------------------------------------
-- RPC: get_finance_kpis(start_date, end_date)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_finance_kpis(start_date date, end_date date)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT jsonb_build_object(
        'entradas', COALESCE(SUM(CASE WHEN tipo = 'Entrada' THEN valor ELSE 0 END), 0),
        'saidas', COALESCE(SUM(CASE WHEN tipo = 'Saída' THEN valor ELSE 0 END), 0),
        'saldo', COALESCE(SUM(
            CASE
                WHEN tipo = 'Entrada' THEN valor
                WHEN tipo = 'Saída' THEN -valor
                ELSE 0
            END
        ), 0),
        'count', COUNT(*)
    )
    FROM public.vw_dashboard_financeiro
    WHERE competencia_mes >= COALESCE(start_date, DATE '1900-01-01')
      AND competencia_mes <= COALESCE(end_date, DATE '2100-12-31');
$$;


-- -----------------------------------------------------------------------------
-- Grants para consumo via API
-- -----------------------------------------------------------------------------
GRANT SELECT ON public.vw_dashboard_pedidos TO anon, authenticated;
GRANT SELECT ON public.vw_dashboard_financeiro TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_finance_kpis(date, date) TO anon, authenticated;
