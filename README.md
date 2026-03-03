# NBL ETL - MySQL to Supabase

ETL pipeline for Grafica NBL:
- fetch daily SQL backup
- import into local MySQL service (GitHub runner)
- truncate Supabase target tables
- load 30 tables via ETL

## Current production source
The valid SQL dumps are in FTP:
- host: `162.241.203.52`
- remote dir: `/public_html/.well-known/backup-jet`
- filename pattern: `nblgrafica_app-YYYY-MM-DD.sql`

## Local setup
```bash
pip install -r requirements.txt
cp .env.example .env
```

Required real values in `.env`:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DB_URL`
- `BACKUP_FTP_PASSWORD`

## Run manually
Dry-run:
```bash
python scripts/daily_job.py --dry-run
```

Real run:
```bash
TRUNCATE_CONFIRM=YES python scripts/daily_job.py --backup-date 2026-03-02
```

## GitHub Actions
Workflow: `.github/workflows/nightly_etl.yml`

Triggers:
- schedule: daily at `04:00 UTC`
- manual: `workflow_dispatch`

Manual run inputs:
- `dry_run` (`true` or `false`)
- `backup_date` (`YYYY-MM-DD`)
- `backup_protocol` (`ftp`, `sftp`, `ftps`)
- `confirm_prod` (`YES` required only when `dry_run=false`)

Production safety:
- manual real run is blocked unless `confirm_prod=YES`
- if `dry_run=true`, workflow validates and runs ETL in validation mode only

### One-shot production run (yesterday)
Use these exact inputs in GitHub UI:
- `dry_run=false`
- `backup_date=2026-03-02`
- `backup_protocol=ftp`
- `confirm_prod=YES`

## Required GitHub Secrets
- `BACKUP_SSH_KEY` (used only when protocol is sftp)
- `BACKUP_SFTP_HOST`
- `BACKUP_SFTP_USER`
- `BACKUP_FTP_HOST`
- `BACKUP_FTP_USER`
- `BACKUP_FTP_PASSWORD`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DB_URL`

See details in `docs/github-secrets.md`.

## Artifacts and logs
After each run, GitHub uploads:
- `logs/*.log`
- `backups/manifest.json`

Artifact name pattern:
- `etl-logs-<run_id>`
