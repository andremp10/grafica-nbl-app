# NBL ETL - Runbook

Operational guide for daily ETL:
1. fetch SQL backup
2. import into MySQL
3. truncate Supabase
4. run ETL load

## Production backup source
Current valid source is FTP:
- host: `162.241.203.52`
- remote dir: `/public_html/.well-known/backup-jet`
- file format: `.sql`
- naming: `nblgrafica_app-YYYY-MM-DD.sql`

## Preconditions
- Python 3.11
- Docker running
- `.env` configured
- GitHub secrets configured

## .env bootstrap
```bash
cp .env.example .env
```

Minimum required values:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DB_URL`
- `BACKUP_FTP_PASSWORD`
- `TRUNCATE_CONFIRM=YES` (for real run)

## Manual commands
Validate env:
```bash
python scripts/check_env.py
```

Dry-run:
```bash
python scripts/daily_job.py --dry-run
```

Real run (specific date):
```bash
TRUNCATE_CONFIRM=YES python scripts/daily_job.py --backup-date 2026-03-02
```

## GitHub Actions run
Workflow: `Nightly ETL`

Manual trigger inputs:
- `dry_run`
- `backup_date`
- `backup_protocol`
- `confirm_prod`

Example production run:
- `dry_run=false`
- `backup_date=2026-03-02`
- `backup_protocol=ftp`
- `confirm_prod=YES`

## Artifacts
Each run uploads:
- `logs/*.log`
- `backups/manifest.json`

Artifact name:
- `etl-logs-<run_id>`

## Optional SFTP key config
Only needed when `BACKUP_PROTOCOL=sftp`.

```bash
ssh-keygen -t ed25519 -C "github-actions-nbl-etl" -f ./nbl_etl_ed25519
ssh-copy-id -i ./nbl_etl_ed25519.pub root@49.12.151.235
ssh-keyscan -p 22 49.12.151.235
```
