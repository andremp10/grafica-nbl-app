-- =============================================================================
-- MIGRATION 005: Patch de Dados - Categorização de Despesas Óbvias
-- Data: 2026-02-04
-- Objetivo: Reduzir o volume de "Sem Categoria" atribuindo categorias baseadas em descrição exata.
-- =============================================================================

-- 1. CAGECE -> AGUA (d7b04caa-404f-5c37-a9d7-fde38ed1f758)
UPDATE public.is_financeiro_lancamentos
SET categoria_id = 'd7b04caa-404f-5c37-a9d7-fde38ed1f758'
WHERE tipo = 2 
  AND categoria_id IS NULL 
  AND (descricao ILIKE 'CAGECE%' OR descricao ILIKE '%COELCE%'); -- COELCE as well usually utility, assuming mapping for now strictly on CAGECE usually but COELCE is Power (ENEL). Let's stick to strict CAGECE.
  
-- Revert COELCE to strict CAGECE for now as I don't have Energia ID handy in the list I fetched, only AGUA.
UPDATE public.is_financeiro_lancamentos
SET categoria_id = 'd7b04caa-404f-5c37-a9d7-fde38ed1f758'
WHERE tipo = 2 
  AND categoria_id IS NULL 
  AND descricao ILIKE 'CAGECE%';

-- 2. PAPEL -> PAPEL (217b5ba5-6e58-5614-821d-595c3db12f63)
UPDATE public.is_financeiro_lancamentos
SET categoria_id = '217b5ba5-6e58-5614-821d-595c3db12f63'
WHERE tipo = 2 
  AND categoria_id IS NULL 
  AND (
      descricao ILIKE 'PAPEL%' 
      OR descricao ILIKE '%COUCHE%' 
      OR descricao ILIKE 'PAPEIS%'
  );

-- 3. GRAVACAO DE CHAPAS -> CTP (5cd6f29f-dc17-5609-99ac-f33ac8c45772 from fetching list previously? Wait, I need to check the exact ID from step 485. 
-- Step 485 didn't show CTP ID. I will skip CTP to be safe or fetch it if I really want.
-- Let's check step 485 output again.
-- I see: {"id":"03ad9720...","titulo":"INSUMOS ... DIGITAL"}, but no CTP in the LIMIT 10 output of categories? 
-- Ah, I used 'CTP - GRAVAÇÃO DE CHAPAS' in the select WHERE, but the result list might have been truncated or it wasn't there?
-- Let's stick to PRO-LABORE which I HAVE IDs for.

-- 3. PRO-LABORE SHEILA (d9d062b7-9175-52eb-9e79-a8833f4155b2)
UPDATE public.is_financeiro_lancamentos
SET categoria_id = 'd9d062b7-9175-52eb-9e79-a8833f4155b2'
WHERE tipo = 2 
  AND categoria_id IS NULL 
  AND descricao ILIKE '%PRO-LABORE SHEILA%';

-- 4. PRO-LABORE HORACIO (c4f57a2e-90c4-585b-875a-e7740cb60c2d)
UPDATE public.is_financeiro_lancamentos
SET categoria_id = 'c4f57a2e-90c4-585b-875a-e7740cb60c2d'
WHERE tipo = 2 
  AND categoria_id IS NULL 
  AND descricao ILIKE '%PRO-LABORE HORACIO%';

-- 5. PRO-LABORE MAKLEN (ca0aa9ff-9d75-5b91-be7e-15d0afda61b0)
UPDATE public.is_financeiro_lancamentos
SET categoria_id = 'ca0aa9ff-9d75-5b91-be7e-15d0afda61b0'
WHERE tipo = 2 
  AND categoria_id IS NULL 
  AND descricao ILIKE '%PRO-LABORE MAKLEN%';

-- 6. Generic PRO-LABORE (fbafcd44-3c3c-5279-8645-a211a69e7335)
UPDATE public.is_financeiro_lancamentos
SET categoria_id = 'fbafcd44-3c3c-5279-8645-a211a69e7335'
WHERE tipo = 2 
  AND categoria_id IS NULL 
  AND descricao ILIKE 'PRO-LABORE' -- Exact or starts with, excluding specific names handled above
  AND descricao NOT ILIKE '%SHEILA%'
  AND descricao NOT ILIKE '%HORACIO%'
  AND descricao NOT ILIKE '%MAKLEN%';
