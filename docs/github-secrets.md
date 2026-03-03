# GitHub Secrets - NBL ETL (Schedule-Only)

Fonte única de verdade para variáveis sensíveis do workflow `nightly_etl.yml`.

## Secrets obrigatórios

| Secret | Obrigatório | Usado por | Observações |
|---|---|---|---|
| `BACKUP_PROTOCOL` | Sim | `check_env.py`, `fetch_backup.py` | `ftp`, `sftp` ou `ftps` |
| `BACKUP_FTP_HOST` | Sim (ftp/ftps) | `fetch_backup.py` | Host do backup FTP |
| `BACKUP_FTP_USER` | Sim (ftp/ftps) | `fetch_backup.py` | Usuário FTP |
| `BACKUP_FTP_PASSWORD` | Sim (ftp/ftps) | `fetch_backup.py` | Nunca logar valor |
| `SUPABASE_URL` | Sim | `etl/run.py` | URL do projeto |
| `SUPABASE_SERVICE_ROLE_KEY` | Sim | `etl/run.py` | Chave service role |
| `SUPABASE_DB_URL` | Sim | `truncate_supabase.py`, `verify_supabase_load.py` | Conexão Postgres direta |
| `TRUNCATE_CONFIRM` | Sim | `check_env.py`, `truncate_supabase.py` | Deve ser `YES` em produção |

## Secrets adicionais para SFTP
Necessários quando `BACKUP_PROTOCOL=sftp`:

| Secret | Obrigatório no modo SFTP | Uso |
|---|---|---|
| `BACKUP_SFTP_HOST` | Sim | origem SFTP |
| `BACKUP_SFTP_USER` | Sim | autenticação SFTP |
| `BACKUP_SSH_KEY` | Sim (ou senha SFTP) | chave privada para criar `~/.ssh/id_rsa` |
| `BACKUP_KNOWN_HOSTS` | Opcional | pin de host key |

## Variáveis não sensíveis (fixas no workflow)
- `BACKUP_FTP_REMOTE_DIR=/public_html/.well-known/backup-jet`
- `BACKUP_FILENAME_PREFIX=nblgrafica_app-`
- `BACKUP_EXTENSION=.sql`
- `TRUNCATE_ENABLED=1`

## Checklist
- [ ] `BACKUP_PROTOCOL`
- [ ] `BACKUP_FTP_HOST`
- [ ] `BACKUP_FTP_USER`
- [ ] `BACKUP_FTP_PASSWORD`
- [ ] `SUPABASE_URL`
- [ ] `SUPABASE_SERVICE_ROLE_KEY`
- [ ] `SUPABASE_DB_URL`
- [ ] `TRUNCATE_CONFIRM` (`YES`)
- [ ] se `BACKUP_PROTOCOL=sftp`: `BACKUP_SFTP_HOST`, `BACKUP_SFTP_USER`, `BACKUP_SSH_KEY`

## Cadastro via GitHub UI
1. Repositório -> `Settings`
2. `Secrets and variables` -> `Actions`
3. `New repository secret`
4. Cadastrar cada secret da tabela acima

## Cadastro via GitHub CLI
```bash
gh auth login
gh secret set BACKUP_PROTOCOL -R owner/repo
gh secret set BACKUP_FTP_HOST -R owner/repo
gh secret set BACKUP_FTP_USER -R owner/repo
gh secret set BACKUP_FTP_PASSWORD -R owner/repo
gh secret set SUPABASE_URL -R owner/repo
gh secret set SUPABASE_SERVICE_ROLE_KEY -R owner/repo
gh secret set SUPABASE_DB_URL -R owner/repo
gh secret set TRUNCATE_CONFIRM -R owner/repo
```

## Observação operacional
Não há dependência de disparo manual: o workflow roda automaticamente no `schedule` diário.
