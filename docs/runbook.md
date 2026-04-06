# NBL ETL - Runbook Operacional

## Objetivo
Operar a carga automática noturna MySQL -> Supabase com validação pós-ETL e artifacts para auditoria.

## Janela de execução
- Workflow: `Nightly ETL`
- Trigger: `schedule` + `workflow_dispatch`
- Cron UTC: `0 4 * * *`
- Fortaleza (UTC-3): **01:00 diariamente**
- O agendamento roda no branch padrão (`main`).
- O workflow de produção não executa mais em `push`.
- Backup separado: `Nightly DB Backup`
- Cron UTC do backup: `30 3 * * *`
- Fortaleza (UTC-3): **00:30 diariamente**

## Fluxo executado
1. `check_env` em modo produção (fail-fast)
2. `probe_backup_source` valida autenticação FTP, diretório remoto e arquivo compatível
3. download do backup
4. import do dump no MySQL do runner
5. truncate no Supabase
6. ETL de carga
7. verificação pós-carga no Supabase
8. upload de artifacts

## Fail-fast obrigatório
`check_env.py --mode production` valida antes do job:
- `BACKUP_PROTOCOL` já fixado como `ftp` no workflow de produção
- credenciais de backup FTP
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_DB_URL`
- gate destrutivo: `TRUNCATE_ENABLED=1` exige `TRUNCATE_CONFIRM=YES`

`probe_backup_source.py` valida antes do `daily_job`:
- autenticação
- acesso ao diretório remoto
- presença de arquivo `nblgrafica_app-YYYY-MM-DD.sql`
- tamanho remoto maior que zero quando informado pelo servidor

## Verificação pós-carga
`scripts/verify_supabase_load.py` (somente SELECT):
- `COUNT(*)` mínimo em `is_pedidos` e `is_clientes`
- recência de dados (preferência: `updated_at`, fallback: `created_at`/`data`)
- exigência de pedidos após `2026-01-25`
- fallback baseline de rowcount quando não há coluna temporal

Configurações úteis:
- `VERIFY_TABLES`
- `VERIFY_MIN_ROWS`
- `VERIFY_RECENCY_TABLE`
- `VERIFY_RECENCY_COLUMNS`
- `VERIFY_MAX_AGE_HOURS`
- `VERIFY_BASELINE_PATH`

## Confiabilidade
- `concurrency`: evita sobreposição de runs noturnos
- `timeout-minutes`: 180
- retry simples do `daily_job`: até 2 tentativas com backoff
- checkout do repositório com retry explícito no workflow
- em falha de fetch/import, truncate e ETL não são executados

## Artifacts e observabilidade
Sempre publicados (`if: always()`):
- `logs/*.log`
- `logs/04_verify_diagnostics.json`
- `backups/manifest.json`
- `backups/verify_baseline.json`

Persistência no banco:
- `public.etl_error_logs` recebe falhas operacionais e rejeições capturadas do ETL
- usar `run_id` para correlacionar múltiplos eventos do mesmo workflow
- o campo `details jsonb` guarda manifesto, diagnostics, saída de log e contexto do GitHub Actions quando disponível

Marcos esperados no log:
- `[START]/[OK] 0. Validate env`
- `Probe OK: protocol=ftp ...`
- `[START]/[OK] 1. Fetch backup`
- `[START]/[OK] 2. Import dump MySQL`
- `[START]/[OK] 3. Truncate Supabase`
- `[START]/[OK] 4. ETL MySQL -> Supabase`
- `[VERIFY OK] ...`

## Troubleshooting
- `check_env` falhou: revisar secrets obrigatórios.
- `probe_backup_source` falhou: revisar `BACKUP_FTP_HOST`, `BACKUP_FTP_USER`, `BACKUP_FTP_PASSWORD` e `/public_html/.well-known/backup-jet`.
- `Fetch backup` falhou: host/usuário/senha/protocolo/caminho remoto.
- `Import dump` falhou: dump inválido ou indisponibilidade do MySQL service.
- `Truncate` falhou: `TRUNCATE_CONFIRM` não está `YES` ou URL do Postgres inválida.
- `Verify Supabase load` falhou: carga vazia, dados antigos ou baseline inconsistente.
- `verify_diagnostics.json`: mostra counts, coluna temporal usada, idade máxima calculada e resultado do `VERIFY_MIN_DATE`.
