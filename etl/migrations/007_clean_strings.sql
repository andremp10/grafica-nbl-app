-- =============================================================================
-- MIGRATION 007: Data Cleaning (Encoding & Special Chars)
-- Data: 2026-02-04
-- Objetivo: Corrigir Mojibake (UTF-8 lido como Latin1) e remover caracteres especiais.
-- =============================================================================

-- Helper function to clean text
CREATE OR REPLACE FUNCTION public.cleanup_text(raw_text text) RETURNS text AS $$
DECLARE
    cleaned text;
BEGIN
    cleaned := raw_text;
    
    -- 1. Attempt to fix Mojibake (Double encoding hack)
    -- Se falhar, mantém original. A logica aqui é: se convertendo para latin1 e voltando pra utf8 der certo, usa.
    BEGIN
        cleaned := convert_from(convert_to(raw_text, 'latin1'), 'utf8');
    EXCEPTION WHEN OTHERS THEN
        cleaned := raw_text; -- Ignore errors, keep original
    END;

    -- 2. Remove special chars (%, @, _, $) but keep Accents, -, ., /
    -- Regex p/ manter letras (com acentos), numeros, espaço e pontuação básica
    -- O user pediu especificamente remover %, @, _
    cleaned := REGEXP_REPLACE(cleaned, '[%@_\$]', ' ', 'g');
    
    -- 3. Trim extra spaces
    cleaned := TRIM(REGEXP_REPLACE(cleaned, '\s+', ' ', 'g'));
    
    RETURN cleaned;
END;
$$ LANGUAGE plpgsql;

-- Apply to Financial
UPDATE public.is_financeiro_lancamentos
SET descricao = cleanup_text(descricao)
WHERE descricao ~ '[Ã%@_\$]';

UPDATE public.is_financeiro_lancamentos
SET obs = cleanup_text(obs)
WHERE obs ~ '[Ã%@_\$]';

UPDATE public.is_financeiro_categorias
SET titulo = cleanup_text(titulo)
WHERE titulo ~ '[Ã%@_\$]';

UPDATE public.is_financeiro_centros_custo
SET titulo = cleanup_text(titulo)
WHERE titulo ~ '[Ã%@_\$]';

UPDATE public.is_financeiro_fornecedores
SET nome = cleanup_text(nome)
WHERE nome ~ '[Ã%@_\$]';

-- Apply to Clients
UPDATE public.is_clientes_pf
SET nome = cleanup_text(nome),
    sobrenome = cleanup_text(sobrenome)
WHERE nome ~ '[Ã%@_\$]' OR sobrenome ~ '[Ã%@_\$]';

UPDATE public.is_clientes_pj
SET razao_social = cleanup_text(razao_social),
    fantasia = cleanup_text(fantasia)
WHERE razao_social ~ '[Ã%@_\$]' OR fantasia ~ '[Ã%@_\$]';
