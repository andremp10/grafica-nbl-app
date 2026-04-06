# GitHub Secrets - NBL ETL

Fonte única de verdade para variáveis sensíveis do workflow `nightly_etl.yml`.
O workflow de produção fixa `BACKUP_PROTOCOL=ftp` no próprio YAML e executa `scripts/probe_backup_source.py` antes do `daily_job`.

## Secrets obrigatórios

| Secret | Obrigatório | Usado por | Observações |
|---|---|---|---|
| `BACKUP_FTP_HOST` | Sim | `check_env.py`, `probe_backup_source.py`, `fetch_backup.py` | Host do backup FTP |
| `BACKUP_FTP_USER` | Sim | `check_env.py`, `probe_backup_source.py`, `fetch_backup.py` | Usuário FTP |
| `BACKUP_FTP_PASSWORD` | Sim | `check_env.py`, `probe_backup_source.py`, `fetch_backup.py` | Rotacionar quando houver `530 Login authentication failed` |
| `SUPABASE_URL` | Sim | `etl/run.py` | URL do projeto |
| `SUPABASE_SERVICE_ROLE_KEY` | Sim | `etl/run.py` | Chave service role |
| `SUPABASE_DB_URL` | Sim | `truncate_supabase.py`, `verify_supabase_load.py` | Conexão Postgres direta |
| `TRUNCATE_CONFIRM` | Sim | `check_env.py`, `truncate_supabase.py` | Deve ser `YES` em produção |

## Secrets adicionais para SFTP
Necessários apenas para uso manual fora do workflow oficial de produção:

| Secret | Obrigatório no modo SFTP | Uso |
|---|---|---|
| `BACKUP_SFTP_HOST` | Sim | origem SFTP |
| `BACKUP_SFTP_USER` | Sim | autenticação SFTP |
| `BACKUP_SSH_KEY` | Sim (ou senha SFTP) | chave privada para criar `~/.ssh/id_rsa` |
| `BACKUP_KNOWN_HOSTS` | Opcional | pin de host key |

## Variáveis não sensíveis (fixas no workflow)
- `BACKUP_PROTOCOL=ftp`
- `BACKUP_FTP_REMOTE_DIR=/public_html/.well-known/backup-jet`
- `BACKUP_FILENAME_PREFIX=nblgrafica_app-`
- `BACKUP_EXTENSION=.sql`
- `TRUNCATE_ENABLED=1`

## Checklist
- [ ] `BACKUP_FTP_HOST`
- [ ] `BACKUP_FTP_USER`
- [ ] `BACKUP_FTP_PASSWORD`
- [ ] `SUPABASE_URL`
- [ ] `SUPABASE_SERVICE_ROLE_KEY`
- [ ] `SUPABASE_DB_URL`
- [ ] `TRUNCATE_CONFIRM` (`YES`)
- [ ] se houver uso manual em `sftp`: `BACKUP_SFTP_HOST`, `BACKUP_SFTP_USER`, `BACKUP_SSH_KEY`

## Cadastro via GitHub UI
1. Repositório -> `Settings`
2. `Secrets and variables` -> `Actions`
3. `New repository secret`
4. Cadastrar cada secret da tabela acima

## Cadastro via GitHub CLI
```bash
gh auth login
gh secret set BACKUP_FTP_HOST -R owner/repo
gh secret set BACKUP_FTP_USER -R owner/repo
gh secret set BACKUP_FTP_PASSWORD -R owner/repo
gh secret set SUPABASE_URL -R owner/repo
gh secret set SUPABASE_SERVICE_ROLE_KEY -R owner/repo
gh secret set SUPABASE_DB_URL -R owner/repo
gh secret set TRUNCATE_CONFIRM -R owner/repo
```

## Observação operacional
- O ETL roda automaticamente no `schedule` diário e pode ser reexecutado por `workflow_dispatch`.
- Pushes normais não devem disparar ETL de produção.
