# ETL MySQL (.sql) → Supabase (Postgres)

Este repositório contém um ETL de **full refresh** que importa um único dump MySQL (`.sql`) e carrega os dados no **Supabase (Postgres)**.

## Como funciona (visão geral)
1) Sobe um MySQL local via Docker Compose.
2) Importa o dump `.sql` nesse MySQL (recria o schema local).
3) Executa a migração MySQL → Supabase com:
   - UUID v5 determinístico (idempotência)
   - *upsert* em batch com fallback (linha a linha, quando necessário)
4) (Opcional) Valida integridade por contagem (MySQL vs Postgres) e gera evidências em `output/`.

## Requisitos
- Python **3.10+**
- Docker (Docker Desktop no Windows/macOS)
- Um projeto no Supabase com o **schema já criado**
  - `schema.sql` é um snapshot **de referência** (não é aplicado automaticamente pelo ETL).

## Uso rápido
1) Instale dependências:
   - `pip install -r requirements.txt`
2) Configure variáveis:
   - copie `.env.example` → `.env` e preencha (não commite)
3) Coloque **exatamente 1** arquivo `.sql` em `sql_input/` (sempre o dump mais recente).
4) Execute:
   - `python -m src.main`

Saídas ficam em `output/` (gitignored): log do run, relatório de contagens e erros por tabela (se existirem).

## Flags úteis
- `--sql <caminho>`: usa um dump específico (se omitido, usa o único `.sql` em `sql_input/`)
- `--skip-mysql-load`: não reimporta o dump no MySQL (assume MySQL já pronto)
- `--skip-etl`: não roda o ETL (apenas carrega dump e/ou valida)
- `--skip-validate`: não roda validação (reconcile de contagens)
- `--verbose`: logs mais detalhados

## Variáveis de ambiente (essenciais)
Destino (Supabase):
- `SUPABASE_URL`
- `SUPABASE_KEY` (**service_role**; nunca `anon`)

Validação via Postgres direto (apenas se você for rodar o reconcile):
- `PG_HOST`, `PG_USER`, `PG_PASSWORD`
- `PG_PORT` (default: 5432), `PG_DBNAME` (default: postgres), `PG_SSLMODE` (default: require)

Fonte (MySQL local via Docker Compose; defaults do projeto):
- `MYSQL_HOST=127.0.0.1`
- `MYSQL_PORT=3307`
- `MYSQL_USER=root`
- `MYSQL_PASSWORD=root`
- `MYSQL_DATABASE=legacy`

## Estrutura do repositório (enxuta)
- `sql_input/`: entrada local do dump (não vai para git)
- `output/`: logs/relatórios locais (não vai para git)
- `config/column_mapping.json`: mapeamento origem → destino
- `schema.sql`: snapshot do schema destino
- `src/`: código do ETL e pipeline
- `docs/CLIENTE.md`: documentação para o cliente (linguagem de negócio)
- `docs/TECNICA.md`: documentação técnica (devs)

## Política de dumps (GitHub-ready)
- Nunca commite dumps: `sql_input/*.sql` é ignorado por padrão (`.gitignore`).
- Mantenha apenas o dump mais recente em `sql_input/` (dumps antigos ficam fora do repositório).

## Troubleshooting rápido
- Porta `3307` ocupada: altere o mapeamento em `docker-compose.yml` ou libere a porta.
- `401` no Supabase: confirme que `SUPABASE_KEY` é `service_role`.
- Sem acesso ao Postgres direto: rode com `--skip-validate`.
