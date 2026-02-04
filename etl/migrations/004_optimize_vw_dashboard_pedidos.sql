-- =============================================================================
-- MIGRATION 004 (FINAL): Otimizar vw_dashboard_pedidos + Simplificar Status (Essenciais)
-- Data: 2026-02-04
-- Contexto:
-- O usuario solicitou manter apenas os status ESSENCIAIS.
-- Mapeamento baseados nos dados reais de uso (73k+ em 'Envio/conferencia').
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
    
    -- DATA PRAZO: Considera NULL se for muito antiga (< 2010)
    CASE 
        WHEN COALESCE(ai.data_entrega, ai.data_prazo) < '2010-01-01' THEN NULL 
        ELSE COALESCE(ai.data_entrega, ai.data_prazo) 
    END AS data_prazo_validada,

    -- STATUS SIMPLIFICADO (Mapeamento Final)
    CASE
        -- Finalizados / Entregues
        WHEN ai.status_agregado ILIKE '%Entregue%' 
          OR ai.status_agregado ILIKE '%Retirado%' 
          OR ai.status_agregado ILIKE '%Finalizado%' 
          OR ai.status_agregado ILIKE '%Concluído%' 
          OR ai.status_agregado ILIKE '%Cancelado%' 
          THEN 'Finalizado'

        -- Enviados / Logistica
        WHEN ai.status_agregado ILIKE '%Enviado%' 
          OR ai.status_agregado ILIKE '%Logística%' 
          OR ai.status_agregado ILIKE '%Serviço de Entrega%' 
          THEN 'Enviado'

        -- Produção (Aprovado = Início da Produção)
        WHEN ai.status_agregado ILIKE '%Produção%' 
          OR ai.status_agregado ILIKE '%Impressão%' 
          OR ai.status_agregado ILIKE '%Acabamento%' 
          OR ai.status_agregado ILIKE '%Arquivo aprovado%' 
          OR ai.status_agregado ILIKE '%Cartão em Produção%'
          OR ai.status_agregado ILIKE '%Pró-Solução%'
          THEN 'Em Produção'

        -- Problemas
        WHEN ai.status_agregado ILIKE '%Reenviar%' 
          OR ai.status_agregado ILIKE '%fora do Padrão%' 
          OR ai.status_agregado ILIKE '%Pendencia%' 
          OR ai.status_agregado ILIKE '%Erro%'
          THEN 'Problema no Arquivo'

        -- Análise (Default para o maior volume)
        -- Envio/conferencia, Arquivo OK, Pagamento, etc.
        ELSE 'Em Análise'
    END AS status_pedido,

    COALESCE(ai.qtde_itens, 0)::integer AS qtde_itens,
    p.total AS valor_total,
    p.frete_valor,
    
    -- IS_FINALIZED: Sincronizado com o status simplificado
    CASE 
        WHEN ai.status_agregado ILIKE '%Entregue%' 
          OR ai.status_agregado ILIKE '%Retirado%' 
          OR ai.status_agregado ILIKE '%Finalizado%' 
          OR ai.status_agregado ILIKE '%Concluído%' 
          OR ai.status_agregado ILIKE '%Cancelado%' 
          THEN TRUE
        ELSE FALSE
    END AS is_finalizado,

    -- IS_ATRASADO
    CASE
        WHEN (COALESCE(ai.data_entrega, ai.data_prazo) >= '2010-01-01')
             AND (COALESCE(ai.data_entrega, ai.data_prazo) < CURRENT_TIMESTAMP)
             AND NOT (
                ai.status_agregado ILIKE '%Entregue%' 
                OR ai.status_agregado ILIKE '%Retirado%' 
                OR ai.status_agregado ILIKE '%Finalizado%'
                OR ai.status_agregado ILIKE '%Cancelado%'
             )
        THEN TRUE
        ELSE FALSE
    END AS is_atrasado,

    CASE
        WHEN (COALESCE(ai.data_entrega, ai.data_prazo) >= '2010-01-01')
             AND (COALESCE(ai.data_entrega, ai.data_prazo) < CURRENT_TIMESTAMP)
             AND NOT (
                ai.status_agregado ILIKE '%Entregue%' 
                OR ai.status_agregado ILIKE '%Retirado%' 
                OR ai.status_agregado ILIKE '%Finalizado%'
                OR ai.status_agregado ILIKE '%Cancelado%'
             )
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
        -- is_finalizado interno do lateral (para uso auxiliar se necessario)
        (
            COALESCE(s.status_agregado, '') ILIKE '%entregue%'
            OR COALESCE(s.status_agregado, '') ILIKE '%finalizado%'
            OR COALESCE(s.status_agregado, '') ILIKE '%conclui%'
            OR COALESCE(s.status_agregado, '') ILIKE '%cancelad%'
            OR COALESCE(s.status_agregado, '') ILIKE '%retirado%'
        ) AS is_finalizado
    FROM (
        SELECT
            COUNT(*)::integer AS qtde_itens,
            MIN(CASE WHEN previsao_producao < '2010-01-01' THEN NULL ELSE previsao_producao END) AS data_prazo,
            MAX(CASE WHEN previsao_entrega < '2010-01-01' THEN NULL ELSE previsao_entrega END) AS data_entrega,
            -- JOIN para pegar o NOME do status
            string_agg(DISTINCT COALESCE(st.nome, i.status), ', ' ORDER BY COALESCE(st.nome, i.status)) AS status_agregado
        FROM public.is_pedidos_itens i
        LEFT JOIN public.is_extras_status st ON CAST(st.id AS text) = i.status
        WHERE i.pedido_id = p.id
    ) s
) ai ON TRUE;

GRANT SELECT ON public.vw_dashboard_pedidos TO anon, authenticated;
