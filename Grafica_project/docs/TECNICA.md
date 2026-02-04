# Documentação Técnica - ETL (MySQL dump -> Supabase/Postgres)

## Visão geral
Pipeline "full refresh":
1) Sobe um MySQL local via Docker (service `mysql`).
2) Importa o dump `.sql` em `MYSQL_DATABASE` (padrão: `legacy`).
3) Executa o ETL (MySQL -> Supabase via API).
4) Valida integridade básica via *row counts* (MySQL vs Postgres) e salva evidências em `output/`.

## Execução (quickstart)
1) Dependências:
   - `pip install -r requirements.txt`
   - Docker instalado
2) Configurar `.env`:
   - copie `.env.example` -> `.env`
3) Colocar um único `.sql` em `sql_input/`
4) Rodar:
   - `python -m src.main`

## Estrutura do repositório
- `src/main.py`: entrypoint (carrega dump, roda ETL, valida)
- `src/etl/etl_v8.py`: ETL (schema-aware; uuid5 determinístico; upsert batch + fallback)
- `config/column_mapping.json`: mapeamento de colunas (origem -> destino)
- `sql_input/`: entrada local (não vai para git)
- `output/`: logs/relatórios locais (não vai para git)
- `docker-compose.yml`: MySQL local para ingestão do dump
- `schema.sql`: snapshot do schema destino (referência; aplicado fora do script)

## Variáveis de ambiente
Obrigatórias para rodar o ETL (Supabase):
- `SUPABASE_URL`
- `SUPABASE_KEY` (service_role)

Para validação de contagens (reconcile counts; Postgres direto):
- `PG_HOST`, `PG_USER`, `PG_PASSWORD`
- `PG_PORT` (default: 5432), `PG_DBNAME` (default: postgres), `PG_SSLMODE` (default: require)

Se você não tiver acesso ao Postgres direto, rode com `--skip-validate`.

Fonte (padrões funcionam com o Docker do projeto):
- `MYSQL_HOST=127.0.0.1`
- `MYSQL_PORT=3307`
- `MYSQL_USER=root`
- `MYSQL_PASSWORD=root`
- `MYSQL_DATABASE=legacy`

Ajustes do ETL:
- `ETL_BATCH_SIZE` (default: 500)
- `ETL_VALIDATE_FKS` (1/0) - valida FKs com cache local (quando possível)
- `ETL_ONLY_TABLES` (CSV) - execução parcial
- `ETL_PRINT_ORDER` (1/0) - imprime ordem topológica e sai
- `OUTPUT_DIR`, `ETL_MAPPING_PATH`, `ETL_ERROR_DIR`

## Ordem de carga (constraints/FKs)
O ETL lê o schema do Postgres e monta um grafo de dependências por FK para gerar uma ordem topológica de carga.

Observações:
- Dumps legados frequentemente têm referências órfãs. Nesses casos, a estratégia recomendada é tornar a FK *nullable* (temporariamente ou por decisão de produto) para não perder linhas.
- O ETL pode "zerar" FKs inválidas (setar NULL) quando a validação de FK estiver habilitada e houver evidência suficiente.

## Logs e debug
- Log geral: `output/run_*.log`
- Erros por tabela: `output/errors/<tabela>.log` (uma linha por registro rejeitado)

## Como adicionar/ajustar mapeamentos
1) Editar `config/column_mapping.json`
2) Rodar com `ETL_ONLY_TABLES=...` para validar uma tabela específica
3) Reexecutar o ETL completo

## Validações incluídas
- Row-count reconciliation (MySQL vs Postgres) em `output/reconciliation_counts_*.md/.json`
