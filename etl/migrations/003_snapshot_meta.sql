-- =============================================================================
-- MIGRATION 003: Snapshot Diario (Metadados do ETL)
-- Data: 2026-02-04
--
-- Objetivo:
-- - Guardar o "estado" do snapshot diario (1 carga por dia)
-- - Expor um RPC leve para o Streamlit invalidar cache quando o snapshot mudar
--
-- Como usar (no ETL):
-- 1) INSERT em public.etl_snapshots no inicio (status='running')
-- 2) UPDATE para status='success' e finished_at no final
-- 3) (opcional) row_counts em JSONB com contagens por tabela
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.etl_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    row_counts jsonb,
    note text
);

CREATE INDEX IF NOT EXISTS etl_snapshots_status_started_at_idx
    ON public.etl_snapshots (status, started_at DESC);

CREATE INDEX IF NOT EXISTS etl_snapshots_finished_at_idx
    ON public.etl_snapshots (finished_at DESC);


-- RPC para o app buscar metadados do ultimo snapshot (sem dar SELECT direto na tabela).
CREATE OR REPLACE FUNCTION public.get_snapshot_meta()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    WITH last_success AS (
        SELECT id, finished_at, row_counts
        FROM public.etl_snapshots
        WHERE status = 'success'
        ORDER BY finished_at DESC NULLS LAST, id DESC
        LIMIT 1
    ),
    last_run AS (
        SELECT id, started_at, finished_at, status
        FROM public.etl_snapshots
        ORDER BY started_at DESC, id DESC
        LIMIT 1
    )
    SELECT jsonb_build_object(
        'snapshot_id', (SELECT id FROM last_success),
        'snapshot_finished_at', (SELECT finished_at FROM last_success),
        'snapshot_row_counts', (SELECT row_counts FROM last_success),
        'last_run_id', (SELECT id FROM last_run),
        'last_run_status', (SELECT status FROM last_run),
        'last_run_started_at', (SELECT started_at FROM last_run),
        'last_run_finished_at', (SELECT finished_at FROM last_run),
        'is_running', COALESCE((SELECT status FROM last_run) = 'running', false)
    );
$$;

GRANT EXECUTE ON FUNCTION public.get_snapshot_meta() TO anon, authenticated;

