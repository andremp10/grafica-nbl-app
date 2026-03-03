-- =============================================================================
-- MIGRATION 006: Rebuild vw_dashboard_financeiro (Deep Refactor)
-- Data: 2026-02-04
-- Objetivo: Recriar a view financeira com status determinístico e limpeza de dados.
-- =============================================================================

DROP VIEW IF EXISTS public.vw_dashboard_financeiro;

CREATE OR REPLACE VIEW public.vw_dashboard_financeiro AS
SELECT
    l.id AS lancamento_id,
    TRIM(l.descricao) AS descricao,
    
    -- Normalização de Tipo
    CASE 
        WHEN l.tipo = 1 THEN 'Entrada' 
        WHEN l.tipo = 2 THEN 'Saída' 
        ELSE 'Outro' 
    END AS tipo,

    -- Valores e Datas
    l.valor,
    l.data::date AS data_vencimento,
    l.data_pagto::date AS data_pagamento,
    DATE_TRUNC('month', l.data)::date AS competencia_mes,

    -- Categorização
    COALESCE(c.titulo, 'Sem Categoria') AS categoria,
    cc.titulo AS centro_custo,
    f.nome AS fornecedor,

    -- Status Determinístico
    CASE
        WHEN l.data_pagto IS NOT NULL THEN 'PAGO'
        WHEN l.data::date < CURRENT_DATE THEN 'ATRASADO'
        ELSE 'ABERTO'
    END AS status,

    -- Flags Auxiliares
    (l.data_pagto IS NOT NULL) AS is_realizado,
    (l.data_pagto IS NULL AND l.data::date < CURRENT_DATE) AS is_atrasado

FROM public.is_financeiro_lancamentos l
LEFT JOIN public.is_financeiro_categorias c ON c.id = l.categoria_id
LEFT JOIN public.is_financeiro_centros_custo cc ON cc.id = l.centro_custo_id
LEFT JOIN public.is_financeiro_fornecedores f ON f.id = l.fornecedor_id;

-- Grant permissions (just in case)
GRANT SELECT ON public.vw_dashboard_financeiro TO authenticated;
GRANT SELECT ON public.vw_dashboard_financeiro TO service_role;
