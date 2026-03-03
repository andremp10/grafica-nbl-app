# GitHub Secrets - NBL ETL

Single source of truth for secrets used by `nightly_etl.yml`.
Never commit secret values.

## Secrets matrix

| Secret | Used by | Required | Format example | Notes |
|---|---|---|---|---|
| `BACKUP_SSH_KEY` | workflow (SFTP mode only) | Optional in FTP mode | `-----BEGIN OPENSSH PRIVATE KEY----- ...` | Keep full multiline key |
| `BACKUP_SFTP_HOST` | `fetch_backup.py` | Optional in FTP mode | `49.12.x.x` | SFTP source host |
| `BACKUP_SFTP_USER` | `fetch_backup.py` | Optional in FTP mode | `root` | SFTP username |
| `BACKUP_FTP_HOST` | `fetch_backup.py` | Yes | `162.241.x.x` | HostGator FTP host |
| `BACKUP_FTP_USER` | `fetch_backup.py` | Yes | `nblgra65` | FTP user |
| `BACKUP_FTP_PASSWORD` | `fetch_backup.py` | Yes | `your-ftp-password` | Never print in logs |
| `SUPABASE_URL` | `etl/run.py` | Yes | `https://xxxx.supabase.co` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | `etl/run.py` | Yes | `eyJ...` | service role JWT |
| `SUPABASE_DB_URL` | `truncate_supabase.py` | Yes | `postgresql://postgres:pwd@db.xxxx.supabase.co:5432/postgres` | direct postgres URL |
| `BACKUP_KNOWN_HOSTS` | workflow SSH setup | Optional | `49.12.x.x ssh-ed25519 AAAA...` | only used in SFTP mode |

## Non-secret workflow variables (already in workflow)
These are not secrets and are hardcoded in workflow env:
- `BACKUP_FTP_REMOTE_DIR=/public_html/.well-known/backup-jet`
- `BACKUP_FILENAME_PREFIX=nblgrafica_app-`
- `BACKUP_EXTENSION=.sql`

Manual `workflow_dispatch` inputs (non-secret):
- `dry_run` (`true|false`)
- `backup_date` (`YYYY-MM-DD`)
- `backup_protocol` (`ftp|sftp|ftps`)
- `confirm_prod` (`YES` required for manual real run)

## Expected checklist
- [ ] `BACKUP_FTP_HOST`
- [ ] `BACKUP_FTP_USER`
- [ ] `BACKUP_FTP_PASSWORD`
- [ ] `SUPABASE_URL`
- [ ] `SUPABASE_SERVICE_ROLE_KEY`
- [ ] `SUPABASE_DB_URL`
- [ ] (optional) `BACKUP_SSH_KEY`
- [ ] (optional) `BACKUP_SFTP_HOST`
- [ ] (optional) `BACKUP_SFTP_USER`
- [ ] (optional) `BACKUP_KNOWN_HOSTS`

## Add secrets via GitHub UI
1. Repository -> `Settings`
2. `Secrets and variables` -> `Actions`
3. `New repository secret`
4. Add each name/value from matrix above

## Add secrets via GitHub CLI
```bash
gh auth login
gh secret set BACKUP_FTP_HOST -R owner/repo
gh secret set BACKUP_FTP_USER -R owner/repo
gh secret set BACKUP_FTP_PASSWORD -R owner/repo
gh secret set SUPABASE_URL -R owner/repo
gh secret set SUPABASE_SERVICE_ROLE_KEY -R owner/repo
gh secret set SUPABASE_DB_URL -R owner/repo
```

## Manual production run in GitHub UI
1. Open `Actions` -> `Nightly ETL` -> `Run workflow`
2. Set inputs:
   - `dry_run=false`
   - `backup_date=2026-03-02`
   - `backup_protocol=ftp`
   - `confirm_prod=YES`
3. Click `Run workflow`

## Optional SFTP key setup (ed25519)
```bash
ssh-keygen -t ed25519 -C "github-actions-nbl-etl" -f ./nbl_etl_ed25519
ssh-copy-id -i ./nbl_etl_ed25519.pub root@49.12.151.235
ssh-keyscan -p 22 49.12.151.235
```
