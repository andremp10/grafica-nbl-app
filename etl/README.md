# ETL MySQL → Supabase

## Visão Geral

ETL diário que carrega dados do `dump.sql` (MySQL) para PostgreSQL/Supabase.

**Estratégia**: MySQL Docker temporário → Python transform → Supabase

## Pré-requisitos

- Python 3.10+ com `psycopg2-binary`, `mysql-connector-python`
- Docker e Docker Compose
- Acesso ao Supabase (credenciais)

## Quick Start

```bash
# 1. Configurar ambiente
export DATABASE_URL="postgresql://user:pass@db.xxx.supabase.co:5432/postgres"

# 2. Colocar dump.sql na raiz do projeto
# dump.sql deve estar em ../dump.sql relativo a este diretório

# 3. Executar ETL
./run.sh
```

## Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `run.sh` | Script principal (inicia Docker, executa Python) |
| `docker-compose.yml` | MySQL temporário |
| `run_etl_from_mysql.py` | Lógica de transformação |
| `validation_queries.sql` | Queries de validação pós-ETL |
| `runbook.md` | Procedimento operacional detalhado |

## Fluxo

```
dump.sql → MySQL Docker (porta 3307) → Python ETL → Supabase
              ↓                           ↓
         Carrega dump              Transforma + UUID5
                                        ↓
                                   INSERT ON CONFLICT
```

## Variáveis de Ambiente

| Variável | Descrição |
|----------|-----------|
| `DATABASE_URL` | URL completa do Supabase |
| `PGHOST` | Host do PostgreSQL (alternativa) |
| `PGDATABASE` | Nome do banco |
| `PGUSER` | Usuário |
| `PGPASSWORD` | Senha |

## Troubleshooting

### MySQL não inicia
```bash
docker-compose logs mysql-legacy
```

### Erro de conexão PostgreSQL
Verifique se as credenciais estão corretas e se o IP está liberado no Supabase.

### Registros rejeitados
Consulte a tabela `etl_rejections`:
```sql
SELECT * FROM etl_rejections ORDER BY created_at DESC LIMIT 100;
```
