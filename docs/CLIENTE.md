# ETL de Migração (MySQL -> Supabase)

## O que este ETL faz
Este projeto importa um backup **.sql** do seu sistema antigo (MySQL) e carrega os dados no **Supabase (Postgres)**, mantendo as tabelas completas e gerando um relatório de conferência.

## O que você precisa antes de rodar
- Um computador com **Python** instalado.
- **Docker** instalado (o projeto usa um MySQL local temporário para ler o dump).
- Acesso ao seu projeto no **Supabase** (URL + chave de serviço).
- O Supabase precisa estar com a **estrutura (tabelas)** já criada (consulte `schema.sql` como referência).

## O que você fornece (entrada)
1) Um único arquivo **.sql** (dump/backup) na pasta `sql_input/`.
2) Um arquivo `.env` (configuração) com as credenciais do Supabase (e conexão do Postgres).

Obs.: mantenha apenas o dump mais recente dentro de `sql_input/` (dumps antigos não fazem parte do projeto).

## O que você recebe (saídas)
Na pasta `output/` o ETL salva:
- Um log completo da execução: `run_YYYYMMDD_HHMMSS.log`
- Um relatório de conferência de contagens (origem vs destino): `reconciliation_counts_YYYYMMDD_HHMMSS.md` e `.json`
- Um diretório `output/errors/` com erros por tabela (se houver)

## Como executar (passo a passo)
1) Coloque **apenas 1 arquivo .sql** dentro de `sql_input/`.
2) Copie `.env.example` para `.env` e preencha os campos.
3) Rode o comando:

   `python -m src.main`

   (Se preferir indicar o arquivo diretamente: `python -m src.main --sql ./sql_input/SEU_DUMP.sql`)

   Se você não tiver acesso ao Postgres direto para a validação de contagens, rode com:
   `python -m src.main --skip-validate`

## Se der erro, o que enviar para suporte
Envie:
- O arquivo `output/run_*.log`
- O arquivo `output/reconciliation_counts_*.md`
- O nome do arquivo `.sql` usado (sem anexar o dump se for sensível)
