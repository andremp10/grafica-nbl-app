# NBL ETL - MySQL to Supabase

Pipeline de produção para carga diária automática:
1. baixa backup SQL
2. importa no MySQL temporário do runner
3. trunca tabelas alvo no Supabase
4. executa ETL das 30 tabelas
5. valida pós-carga no Supabase

## Agendamento automático
- Workflow: `.github/workflows/nightly_etl.yml`
- Trigger: `schedule` + `workflow_dispatch`
- Cron UTC: `0 4 * * *`
- Horário Fortaleza (UTC-3): **01:00 da madrugada, diariamente**
- Execução ocorre no branch padrão (`main`) do repositório.
- O ETL de produção não roda mais em `push`.

## Backup diário
- Workflow dedicado: `.github/workflows/nightly_db_backup.yml`
- Cron UTC: `30 3 * * *`
- Horário Fortaleza (UTC-3): **00:30 da madrugada, diariamente**
- Gera `pg_dump` em formato custom, valida com `pg_restore -l` e publica artifact com retenção de 30 dias.

## Fonte de backup (produção)
- Protocolo oficial: `ftp`
- FTP host: `162.241.203.52`
- Diretório remoto: `/public_html/.well-known/backup-jet`
- Padrão: `nblgrafica_app-YYYY-MM-DD.sql`
- Preflight: `python scripts/probe_backup_source.py --date YYYY-MM-DD`

## Secrets obrigatórios (GitHub Actions)
- `BACKUP_FTP_HOST`
- `BACKUP_FTP_USER`
- `BACKUP_FTP_PASSWORD`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DB_URL`
- `TRUNCATE_CONFIRM` (`YES`)

Em produção, o workflow fixa `BACKUP_PROTOCOL=ftp`. Quando for necessário executar manualmente em `sftp`, também configurar:
- `BACKUP_SFTP_HOST`
- `BACKUP_SFTP_USER`
- `BACKUP_SSH_KEY` (ou senha SFTP)
- opcional: `BACKUP_KNOWN_HOSTS`

## Logs e artifacts
Em todo run (sucesso ou falha), o workflow publica:
- `logs/*.log`
- `logs/04_verify_diagnostics.json`
- `backups/manifest.json`
- `backups/verify_baseline.json`

Artifact:
- `etl-logs-<run_id>`

## Marcos do job
Logs mostram marcos claros:
- `0. Validate env`
- `Probe backup source`
- `1. Fetch backup`
- `2. Import dump MySQL`
- `3. Truncate Supabase`
- `4. ETL MySQL -> Supabase`
- `Verify Supabase load`

## Verificação pós-carga
O script `scripts/verify_supabase_load.py` valida:
- contagem mínima em tabelas-chave (`is_pedidos`, `is_clientes`)
- recência por coluna temporal (`updated_at`, `created_at` ou `data`)
- existência de pedidos após `2026-01-25` (fail se não houver)
- fallback por baseline de rowcount quando não há coluna temporal

Se qualquer check falhar, o workflow falha.

## Troubleshooting rápido
- Falha em `check_env`: secret ausente/inválido (principalmente `SUPABASE_DB_URL`, `TRUNCATE_CONFIRM` ou credenciais FTP).
- Falha em `probe_backup_source`: autenticação FTP, diretório remoto ou ausência de arquivo compatível.
- Falha em fetch: credenciais FTP/SFTP, diretório remoto inválido ou arquivo remoto vazio/inválido.
- Falha em truncate: `TRUNCATE_CONFIRM` diferente de `YES` ou `SUPABASE_DB_URL` inválida.
- Falha em verify: carga subiu incompleta, dados vazios ou recência acima do limite (`VERIFY_MAX_AGE_HOURS`).

Detalhes operacionais: `docs/runbook.md` e `docs/github-secrets.md`.
