CREATE TABLE IF NOT EXISTS public.etl_error_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id text NOT NULL,
  script_name text NOT NULL,
  step_name text,
  phase text,
  event_type text NOT NULL,
  severity text NOT NULL DEFAULT 'error',
  table_name text,
  legacy_id text,
  error_class text,
  probable_constraint text,
  message text NOT NULL,
  traceback text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_etl_error_logs_run_id
  ON public.etl_error_logs (run_id);

CREATE INDEX IF NOT EXISTS idx_etl_error_logs_created_at
  ON public.etl_error_logs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_error_logs_event_type
  ON public.etl_error_logs (event_type);
