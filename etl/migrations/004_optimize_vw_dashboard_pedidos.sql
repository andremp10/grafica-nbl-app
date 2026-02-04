-- =============================================================================
-- MIGRATION 004: Otimizar vw_dashboard_pedidos (evitar statement timeout)
-- Data: 2026-02-04
--
-- Contexto:
-- A view anterior agregava TODOS os itens (GROUP BY em is_pedidos_itens) e depois
-- fazia JOIN com pedidos. Isso pode causar "canceling statement due to statement timeout"
-- no PostgREST mesmo em consultas pequenas (LIMIT 5), pois o planner precisava
-- materializar a agregacao global.
--
-- Solucao:
-- Trocar a agregacao global por LATERAL, agregando apenas os itens do pedido da linha.
-- Assim, consultas paginadas (ORDER BY created_at DESC LIMIT N) conseguem usar indice
-- de pedidos e calcular os agregados somente para N pedidos.
-- =============================================================================

CREATE OR REPLACE VIEW public.vw_dashboard_pedidos AS
SELECT
    p.id AS pedido_id,
    p.cliente_id,
    COALESCE(
        NULLIF(TRIM(COALESCE(pf.nome, '') || ' ' || COALESCE(pf.sobrenome, '')), ''),
        pj.razao_social,
        'Cliente #' || LEFT(p.cliente_id::text, 8)
    ) AS cliente_nome,
    p.created_at AS data_criacao,
    COALESCE(ai.data_entrega, ai.data_prazo) AS data_prazo_validada,
    COALESCE(ai.status_agregado, 'Sem Status') AS status_pedido,
    COALESCE(ai.qtde_itens, 0)::integer AS qtde_itens,
    p.total AS valor_total,
    p.frete_valor,
    COALESCE(ai.is_finalizado, FALSE) AS is_finalizado,
    CASE
        WHEN COALESCE(ai.data_entrega, ai.data_prazo) IS NOT NULL
             AND COALESCE(ai.data_entrega, ai.data_prazo) < CURRENT_TIMESTAMP
             AND NOT COALESCE(ai.is_finalizado, FALSE)
        THEN TRUE
        ELSE FALSE
    END AS is_atrasado,
    CASE
        WHEN COALESCE(ai.data_entrega, ai.data_prazo) IS NOT NULL
             AND COALESCE(ai.data_entrega, ai.data_prazo) < CURRENT_TIMESTAMP
             AND NOT COALESCE(ai.is_finalizado, FALSE)
        THEN GREATEST(0, CURRENT_DATE - COALESCE(ai.data_entrega, ai.data_prazo)::date)
        ELSE 0
    END AS dias_em_atraso
FROM public.is_pedidos p
LEFT JOIN public.is_clientes_pf pf ON pf.cliente_id = p.cliente_id
LEFT JOIN public.is_clientes_pj pj ON pj.cliente_id = p.cliente_id
LEFT JOIN LATERAL (
    SELECT
        s.qtde_itens,
        s.data_prazo,
        s.data_entrega,
        s.status_agregado,
        (
            COALESCE(s.status_agregado, '') ILIKE '%entregue%'
            OR COALESCE(s.status_agregado, '') ILIKE '%finalizado%'
            OR COALESCE(s.status_agregado, '') ILIKE '%concluid%'
            OR COALESCE(s.status_agregado, '') ILIKE '%cancelad%'
        ) AS is_finalizado
    FROM (
        SELECT
            COUNT(*)::integer AS qtde_itens,
            MIN(CASE WHEN previsao_producao < '1900-01-01' THEN NULL ELSE previsao_producao END) AS data_prazo,
            MAX(CASE WHEN previsao_entrega < '1900-01-01' THEN NULL ELSE previsao_entrega END) AS data_entrega,
            string_agg(DISTINCT status, ', ' ORDER BY status) AS status_agregado
        FROM public.is_pedidos_itens i
        WHERE i.pedido_id = p.id
    ) s
) ai ON TRUE;

GRANT SELECT ON public.vw_dashboard_pedidos TO anon, authenticated;

-- Indices que ajudam o planner a empurrar ORDER BY/LIMIT (se ainda nao existirem).
CREATE INDEX IF NOT EXISTS idx_is_pedidos_created_at ON public.is_pedidos(created_at);
CREATE INDEX IF NOT EXISTS idx_is_pedidos_itens_pedido_id ON public.is_pedidos_itens(pedido_id);

