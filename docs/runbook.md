# NBL ETL - Runbook Operacional

## Objetivo
Operar a carga automática noturna MySQL -> Supabase com validação pós-ETL e artifacts para auditoria.

## Janela de execução
- Workflow: `Nightly ETL`
- Trigger: `schedule`
- Cron UTC (temporário / burn-in): `*/15 * * * *`
- Cron UTC (final): `0 4 * * *`
- Fortaleza (UTC-3, final): **01:00 diariamente**
- O agendamento roda no branch padrão (`main`).

Após a primeira execução bem-sucedida, reverta o cron para o valor final.

## Fluxo executado
1. `check_env` em modo produção (fail-fast)
2. download do backup
3. import do dump no MySQL do runner
4. truncate no Supabase
5. ETL de carga
6. verificação pós-carga no Supabase
7. upload de artifacts

## Fail-fast obrigatório
`check_env.py --mode production` valida antes do job:
- `BACKUP_PROTOCOL`
- credenciais de backup (FTP/SFTP conforme protocolo)
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_DB_URL`
- gate destrutivo: `TRUNCATE_ENABLED=1` exige `TRUNCATE_CONFIRM=YES`

## Verificação pós-carga
`scripts/verify_supabase_load.py` (somente SELECT):
- `COUNT(*)` mínimo em `is_pedidos` e `is_clientes`
- recência de dados (preferência: `updated_at`, fallback: `created_at`/`data`)
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
- `timeout-minutes`: 90
- retry simples do `daily_job`: até 2 tentativas com backoff
- em falha de fetch/import, truncate e ETL não são executados

## Artifacts e observabilidade
Sempre publicados (`if: always()`):
- `logs/*.log`
- `backups/manifest.json`
- `backups/verify_baseline.json`

Marcos esperados no log:
- `[START]/[OK] 0. Validate env`
- `[START]/[OK] 1. Fetch backup`
- `[START]/[OK] 2. Import dump MySQL`
- `[START]/[OK] 3. Truncate Supabase`
- `[START]/[OK] 4. ETL MySQL -> Supabase`
- `[VERIFY OK] ...`

## Troubleshooting
- `check_env` falhou: revisar secrets obrigatórios.
- `Fetch backup` falhou: host/usuário/senha/protocolo/caminho remoto.
- `Import dump` falhou: dump inválido ou indisponibilidade do MySQL service.
- `Truncate` falhou: `TRUNCATE_CONFIRM` não está `YES` ou URL do Postgres inválida.
- `Verify Supabase load` falhou: carga vazia, dados antigos ou baseline inconsistente.
