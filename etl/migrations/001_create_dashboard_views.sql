-- =============================================================================
-- MIGRATION: Create Dashboard Views
-- Projeto: NBL Gráfica
-- Data: 2026-02-04
-- Descrição: Cria views para dashboards de Pedidos (PCP) e Financeiro
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VIEW 1: vw_dashboard_pedidos
-- Granularidade: 1 linha por pedido
-- Uso: Dashboard PCP / Chão de Fábrica
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.vw_dashboard_pedidos AS
WITH
-- Agregação de itens por pedido
agg_itens AS (
    SELECT
        pedido_id,
        COUNT(*) AS qtde_itens,
        COALESCE(SUM(valor * COALESCE(qtde, 1)), 0) AS valor_itens,
        MIN(previsao_producao) AS data_prazo,
        MAX(previsao_entrega) AS data_entrega,
        -- Status mais crítico (concatena distintos)
        string_agg(DISTINCT status, ', ' ORDER BY status) AS status_agregado
    FROM public.is_pedidos_itens
    GROUP BY pedido_id
),
-- Nome do cliente (PF ou PJ)
cliente_nome AS (
    SELECT
        c.id AS cliente_id,
        c.tipo AS cliente_tipo,
        COALESCE(
            pf.nome || ' ' || COALESCE(pf.sobrenome, ''),
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
    cn.cliente_tipo,
    p.created_at AS data_criacao,
    ai.data_prazo,
    ai.data_entrega,
    COALESCE(ai.status_agregado, 'Sem Status') AS status_pedido,
    COALESCE(ai.qtde_itens, 0)::integer AS qtde_itens,
    p.total AS valor_total,
    COALESCE(ai.valor_itens, 0) AS valor_itens,
    p.frete_valor,
    -- Flag de atraso
    CASE
        WHEN ai.data_prazo IS NOT NULL
             AND ai.data_prazo < CURRENT_TIMESTAMP
             AND COALESCE(ai.status_agregado, '') NOT ILIKE '%entregue%'
             AND COALESCE(ai.status_agregado, '') NOT ILIKE '%finalizado%'
        THEN TRUE
        ELSE FALSE
    END AS is_atrasado,
    -- Dias em atraso
    CASE
        WHEN ai.data_prazo IS NOT NULL AND ai.data_prazo < CURRENT_TIMESTAMP
        THEN GREATEST(0, (CURRENT_DATE - ai.data_prazo::date))
        ELSE 0
    END AS dias_em_atraso
FROM public.is_pedidos p
LEFT JOIN agg_itens ai ON ai.pedido_id = p.id
LEFT JOIN cliente_nome cn ON cn.cliente_id = p.cliente_id;

-- -----------------------------------------------------------------------------
-- VIEW 2: vw_dashboard_financeiro
-- Granularidade: 1 linha por lançamento
-- Uso: Dashboard Financeiro / Controladoria
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
    COALESCE(cc.titulo, 'Sem Centro de Custo') AS centro_custo,
    COALESCE(forn.razao_social, NULL) AS fornecedor,
    -- Competência mensal
    DATE_TRUNC('month', COALESCE(fl.data, fl.data_emissao, fl.data_pagto))::date AS competencia_mes,
    -- Flags
    CASE
        WHEN fl.status = 1
             AND fl.data IS NOT NULL
             AND fl.data < CURRENT_TIMESTAMP
        THEN TRUE
        ELSE FALSE
    END AS is_atrasado,
    CASE
        WHEN fl.data_pagto IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_realizado
FROM public.is_financeiro_lancamentos fl
LEFT JOIN public.is_financeiro_categorias cat ON cat.id = fl.categoria_id
LEFT JOIN public.is_financeiro_centros_custo cc ON cc.id = fl.centro_custo_id
LEFT JOIN public.is_financeiro_fornecedores forn ON forn.id = fl.fornecedor_id;

-- -----------------------------------------------------------------------------
-- ÍNDICES RECOMENDADOS (opcional, melhora performance)
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_pedidos_cliente_id ON is_pedidos(cliente_id);
CREATE INDEX IF NOT EXISTS idx_pedidos_created_at ON is_pedidos(created_at);
CREATE INDEX IF NOT EXISTS idx_pedidos_itens_pedido_id ON is_pedidos_itens(pedido_id);
CREATE INDEX IF NOT EXISTS idx_pedidos_itens_status ON is_pedidos_itens(status);
CREATE INDEX IF NOT EXISTS idx_financeiro_tipo ON is_financeiro_lancamentos(tipo);
CREATE INDEX IF NOT EXISTS idx_financeiro_status ON is_financeiro_lancamentos(status);
CREATE INDEX IF NOT EXISTS idx_financeiro_data ON is_financeiro_lancamentos(data);
CREATE INDEX IF NOT EXISTS idx_financeiro_categoria ON is_financeiro_lancamentos(categoria_id);

-- -----------------------------------------------------------------------------
-- VALIDAÇÕES (execute após criar as views)
-- -----------------------------------------------------------------------------

-- Verificar duplicidade em pedidos (deve retornar 0 linhas)
-- SELECT pedido_id, COUNT(*) FROM vw_dashboard_pedidos GROUP BY 1 HAVING COUNT(*)>1;

-- Verificar duplicidade em financeiro (deve retornar 0 linhas)
-- SELECT lancamento_id, COUNT(*) FROM vw_dashboard_financeiro GROUP BY 1 HAVING COUNT(*)>1;

-- Totais por mês
-- SELECT competencia_mes, tipo, SUM(valor) FROM vw_dashboard_financeiro GROUP BY 1,2 ORDER BY 1 DESC;
