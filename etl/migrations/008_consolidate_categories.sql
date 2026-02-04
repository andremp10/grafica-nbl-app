-- =============================================================================
-- MIGRATION 008: Consolidate Expense Categories
-- Data: 2026-02-04
-- Objetivo: Unificar categorias mistas (Insumos/Manut/Peças) em grupos limpos.
-- Solicitado: "uma so para insumos", "uma so para manutenções", "uma so para peças digitais"
-- =============================================================================

DO $$
DECLARE
    id_insumos uuid;
    id_manutencao uuid;
    id_pecas uuid;
    id_pecas_digitais uuid;
    rec record;
BEGIN
    -- 1. Ensure Target Categories Exist
    
    -- INSUMOS
    SELECT id INTO id_insumos FROM public.is_financeiro_categorias WHERE titulo = 'INSUMOS';
    IF id_insumos IS NULL THEN
        INSERT INTO public.is_financeiro_categorias (titulo) VALUES ('INSUMOS') RETURNING id INTO id_insumos;
    END IF;

    -- MANUTENÇÃO
    SELECT id INTO id_manutencao FROM public.is_financeiro_categorias WHERE titulo = 'MANUTENÇÃO';
    IF id_manutencao IS NULL THEN
        INSERT INTO public.is_financeiro_categorias (titulo) VALUES ('MANUTENÇÃO') RETURNING id INTO id_manutencao;
    END IF;

    -- PEÇAS
    SELECT id INTO id_pecas FROM public.is_financeiro_categorias WHERE titulo = 'PEÇAS';
    IF id_pecas IS NULL THEN
        INSERT INTO public.is_financeiro_categorias (titulo) VALUES ('PEÇAS') RETURNING id INTO id_pecas;
    END IF;

    -- PEÇAS DIGITAIS
    SELECT id INTO id_pecas_digitais FROM public.is_financeiro_categorias WHERE titulo = 'PEÇAS DIGITAIS';
    IF id_pecas_digitais IS NULL THEN
        INSERT INTO public.is_financeiro_categorias (titulo) VALUES ('PEÇAS DIGITAIS') RETURNING id INTO id_pecas_digitais;
    END IF;


    -- 2. Migrate Data (Consolidation Logic)

    -- A. PEÇAS DIGITAIS
    -- Mover tudo de "INSUMOS / MANUTENÇÃO / PEÇAS IMPRESSÃO DIGITAL"
    UPDATE public.is_financeiro_lancamentos l
    SET categoria_id = id_pecas_digitais
    FROM public.is_financeiro_categorias c
    WHERE l.categoria_id = c.id
    AND c.titulo = 'INSUMOS / MANUTENÇÃO / PEÇAS IMPRESSÃO DIGITAL';

    -- B. MANUTENÇÃO
    -- Mover Predial, Reformas, Veículos
    UPDATE public.is_financeiro_lancamentos l
    SET categoria_id = id_manutencao
    FROM public.is_financeiro_categorias c
    WHERE l.categoria_id = c.id
    AND c.titulo IN (
        'MANUTENÇÃO PREDIAL/REFORMAS', 
        'MANUTENÇÃO DE VEÍCULOS'
    );

    -- C. INSUMOS (Grande Consolidação)
    -- Mover Offset, Visula, Acabamento, Quimicos, Tintas, CTP, Limpeza
    UPDATE public.is_financeiro_lancamentos l
    SET categoria_id = id_insumos
    FROM public.is_financeiro_categorias c
    WHERE l.categoria_id = c.id
    AND c.titulo IN (
        'INSUMOS / MANUTENÇÃO / PEÇAS IMPRESSÃO OFFSET',
        'INSUMOS / MANUTENÇÃO / PEÇAS COMUNICAÇÃO VISUAL',
        'INSUMOS / MANUTENÇÃO / PEÇAS ACABAMENTO',
        'QUIMICOS IMPRESSÃO OFFSET',
        'TINTAS IMPRESSÃO OFFSET',
        'CTP - GRAVAÇÃO DE CHAPAS',
        'MATERIAL DE LIMPEZA'
    );

    -- D. PEÇAS (Genérico)
    -- Por enquanto, maquina e equipamentos movemos para peças? Ou Manutenção?
    -- "MAQUINAS E EQUIPAMENTOS" often implies purchase (CAPEX) or huge repairs.
    -- Vamos manter "MAQUINAS E EQUIPAMENTOS" separado pois pode ser CAPEX.
    
END $$;
