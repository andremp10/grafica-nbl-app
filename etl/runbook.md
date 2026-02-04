# ETL Runbook: Pipeline Diário (dump.sql → Postgres)

**Data**: 2025-01-16  
**Objetivo**: Documentar processo completo do ETL diário para carga do dump MySQL no Postgres/Supabase

---

## Sumário Executivo

- **Input**: `dump.sql` MySQL (DDL+DML completo, >200MB)
- **Estratégia**: Docker MySQL temporário + Python ETL + TRUNCATE+RELOAD
- **Output**: Postgres/Supabase com schema normalizado
- **Frequência**: Diária (1x por dia)
- **Tempo estimado**: 10-30 minutos (dependendo do tamanho do dump)

---

## 1. Pré-requisitos

### 1.1 Software Necessário

- **Docker**: Instalado e rodando (para MySQL temporário)
- **Python 3.8+**: Com dependências:
  - `pymysql` - Conectar ao MySQL
  - `psycopg2` ou `psycopg2-binary` - Conectar ao Postgres
  - `python-dotenv` - Carregar variáveis de ambiente (opcional)
- **PostgreSQL/Supabase**: Acesso ao banco de dados de destino
- **Credenciais**: Configuradas (ver seção 2)

### 1.2 Arquivos Necessários

- `dump.sql` - Dump MySQL diário (recebido do cliente)
- `base_schema_corrigido.sql` ou `schema_final.sql` - Schema Postgres aplicado
- `staging_schema.sql` - Schema staging aplicado
- `mapping_legacy_to_new.md` - Mapeamento legado→novo (referência)
- Script ETL: `run_daily_sql_etl.py` (quando implementado)

---

## 2. Configuração

### 2.1 Variáveis de Ambiente

Criar arquivo `.env` (ou configurar no sistema):

```bash
# MySQL Docker (temporário)
MYSQL_ROOT_PASSWORD=temp_password
MYSQL_DATABASE=legacy_db
MYSQL_PORT=3307

# Postgres/Supabase
POSTGRES_HOST=your-supabase-host.supabase.co
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your-password

# ETL
ETL_LOG_LEVEL=INFO
ETL_BATCH_SIZE=10000
ETL_VALIDATE_AFTER_LOAD=true
```

### 2.2 Credenciais Supabase

Obter credenciais do Supabase:
1. Dashboard Supabase → Settings → Database
2. Connection string ou host/user/password separados
3. Configurar variáveis de ambiente (ver 2.1)

---

## 3. Processo Completo

### 3.1 Validação do Dump

**Objetivo**: Validar que `dump.sql` está íntegro antes de processar

**Comandos**:
```bash
# Validar tamanho mínimo (ex: >10MB)
if ((Get-Item dump.sql).Length -lt 10MB) {
    Write-Error "Dump muito pequeno - possível problema"
    exit 1
}

# Validar encoding UTF-8
$content = Get-Content dump.sql -Encoding UTF8 -Raw
if ($content -match "[^\x00-\x7F]") {
    Write-Warning "Dump contém caracteres não-ASCII - validar encoding"
}

# Validar estrutura básica (tem CREATE TABLE)
$createCount = (Select-String -Path dump.sql -Pattern "^CREATE TABLE" -AllMatches).Matches.Count
if ($createCount -eq 0) {
    Write-Error "Dump não contém CREATE TABLE - possível problema"
    exit 1
}

Write-Host "Dump validado: $createCount tabelas encontradas"
```

**Validações**:
- ✅ Tamanho mínimo (ex: >10MB)
- ✅ Encoding UTF-8 válido
- ✅ Contém `CREATE TABLE` statements
- ✅ Contém `INSERT` statements (opcional - pode estar vazio)

### 3.2 Restore no Docker MySQL

**Objetivo**: Carregar dump MySQL em container temporário para extração

**Comandos**:
```bash
# 1. Parar container anterior (se existir)
docker stop mysql-temp 2>$null
docker rm mysql-temp 2>$null

# 2. Criar diretório para dump
mkdir -p temp/dump

# 3. Copiar dump para diretório do container
Copy-Item dump.sql temp/dump/dump.sql

# 4. Iniciar MySQL Docker com dump
docker run -d \
  --name mysql-temp \
  -e MYSQL_ROOT_PASSWORD=$env:MYSQL_ROOT_PASSWORD \
  -e MYSQL_DATABASE=$env:MYSQL_DATABASE \
  -p $env:MYSQL_PORT:3306 \
  -v "${PWD}/temp/dump:/docker-entrypoint-initdb.d" \
  mysql:8.0 \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_unicode_ci

# 5. Aguardar MySQL estar pronto (30-60 segundos)
Write-Host "Aguardando MySQL iniciar..."
Start-Sleep -Seconds 30

# 6. Verificar conexão
docker exec mysql-temp mysql -uroot -p$env:MYSQL_ROOT_PASSWORD -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$env:MYSQL_DATABASE';"
```

**Notas**:
- Container será removido após execução (ver 3.6)
- Dump é carregado automaticamente via `/docker-entrypoint-initdb.d/`
- Charset `utf8mb4` garante compatibilidade com UTF-8

### 3.3 Executar ETL

**Objetivo**: Extrair dados do MySQL, transformar e carregar no Postgres

**Script Python** (`run_daily_sql_etl.py`):
```python
#!/usr/bin/env python3
"""
ETL diário: dump.sql MySQL → Postgres/Supabase
"""
import pymysql
import psycopg2
import uuid
from datetime import datetime
from pathlib import Path

# TODO: Implementar pipeline completo
# 1. Conectar MySQL Docker
# 2. Conectar Postgres/Supabase
# 3. TRUNCATE tabelas staging
# 4. Extrair dados do MySQL (com transformações)
# 5. Carregar no staging (stg_*)
# 6. Transformar e carregar no schema final (is_*)
# 7. Validar integridade
# 8. Registrar em etl_runs
```

**Comando** (quando script estiver implementado):
```bash
python run_daily_sql_etl.py \
  --dump-file dump.sql \
  --mysql-host localhost \
  --mysql-port $env:MYSQL_PORT \
  --mysql-user root \
  --mysql-password $env:MYSQL_ROOT_PASSWORD \
  --postgres-host $env:POSTGRES_HOST \
  --postgres-port $env:POSTGRES_PORT \
  --postgres-db $env:POSTGRES_DB \
  --postgres-user $env:POSTGRES_USER \
  --postgres-password $env:POSTGRES_PASSWORD \
  --batch-size $env:ETL_BATCH_SIZE \
  --validate
```

**Processo interno** (algoritmo):
1. **TRUNCATE staging**: Limpar todas as tabelas `staging.stg_*`
2. **Extração MySQL**: Para cada tabela legada:
   - Conectar ao MySQL Docker
   - `SELECT * FROM <tabela>`
   - Converter todos os valores para `text` (staging)
   - Inserir em `staging.stg_<tabela>`
3. **Transformação**: Para cada tabela destino (`is_*`):
   - Ler de `staging.stg_<tabela>`
   - Transformar valores (tipos, UUIDs, FKs)
   - Validar constraints (NOT NULL, UNIQUE, FK)
   - Inserir em `is_<tabela>` (UPSERT via `ON CONFLICT`)
4. **Validação**: Executar `validation_queries.sql` (ver 3.5)

### 3.4 Transformações Principais

**UUIDs determinísticos**:
```python
import uuid

NAMESPACE = uuid.UUID("2a6b2c31-0f2a-4dfd-8cde-7b4b9b3f1c5a")

def uuid5_for(table: str, legacy_id: int) -> uuid.UUID:
    """Gera UUID determinístico para ID legado."""
    return uuid.uuid5(NAMESPACE, f"is_{table}:{legacy_id}")

# Exemplo: is_clientes.id = 123 → uuid5(NAMESPACE, "is_clientes:123")
```

**Tratamento de zero dates**:
```python
from datetime import datetime

def convert_zero_date(value: str) -> datetime | None:
    """Converte '0000-00-00' para None."""
    if not value or value.startswith('0000-00-00'):
        return None
    return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
```

**FKs derivadas**:
```python
# Exemplo: is_pedidos.cliente (int) → is_pedidos.cliente_id (uuid)
legacy_cliente_id = row['cliente']  # int
if legacy_cliente_id:
    new_cliente_id = uuid5_for('clientes', legacy_cliente_id)
    # Verificar se FK existe (lookup ou cache)
    if not fk_exists('is_clientes', new_cliente_id):
        reject_row(row, reason="FK is_clientes not found")
    row['cliente_id'] = new_cliente_id
```

**Encoding**:
```python
def fix_encoding(value: bytes | str) -> str:
    """Corrige encoding latin1→UTF-8."""
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return value.decode('latin1', errors='replace')
    return value
```

### 3.5 Validação Pós-Carga

**Objetivo**: Validar integridade após carga completa

**Queries** (executar manualmente ou via script):
```sql
-- validation_queries.sql

-- 1. Contagens por tabela
SELECT 'is_clientes' as table_name, COUNT(*) as row_count FROM is_clientes
UNION ALL
SELECT 'is_pedidos', COUNT(*) FROM is_pedidos
UNION ALL
SELECT 'is_pedidos_itens', COUNT(*) FROM is_pedidos_itens
-- ... (todas as tabelas principais)

-- 2. FKs órfãs
SELECT 'is_pedidos.cliente_id' as fk, COUNT(*) as orphaned
FROM is_pedidos p
LEFT JOIN is_clientes c ON p.cliente_id = c.id
WHERE p.cliente_id IS NOT NULL AND c.id IS NULL;

-- 3. Uniques violados
SELECT 'is_clientes.email_log' as unique_col, COUNT(*) - COUNT(DISTINCT email_log) as duplicates
FROM is_clientes
WHERE email_log IS NOT NULL;

-- 4. NOT NULL violados (não deve haver se ETL validou)
SELECT 'is_clientes.email_log' as not_null_col, COUNT(*) as nulls
FROM is_clientes
WHERE email_log IS NULL;

-- 5. Somatórios financeiros (validação de integridade)
SELECT 
    'is_pedidos' as table_name,
    SUM(total) as total_pedidos,
    SUM(acrescimo) as total_acrescimos,
    SUM(desconto) as total_descontos
FROM is_pedidos;
```

**Comando**:
```bash
psql -h $env:POSTGRES_HOST -U $env:POSTGRES_USER -d $env:POSTGRES_DB -f validation_queries.sql
```

### 3.6 Limpeza

**Objetivo**: Remover recursos temporários

**Comandos**:
```bash
# 1. Parar e remover MySQL Docker
docker stop mysql-temp
docker rm mysql-temp

# 2. Remover dump temporário (opcional - manter para debug)
Remove-Item temp/dump/dump.sql -Force

# 3. Remover diretório temporário (se vazio)
Remove-Item temp/dump -Force -ErrorAction SilentlyContinue
```

---

## 4. Troubleshooting

### 4.1 Problemas Comuns

#### MySQL Docker não inicia
**Sintoma**: Container para imediatamente após iniciar

**Causa**: Dump muito grande ou formato inválido

**Solução**:
```bash
# Ver logs do container
docker logs mysql-temp

# Verificar formato do dump
head -100 dump.sql
```

#### ETL falha com "FK not found"
**Sintoma**: Rejeições em `etl_rejections` com `reason="FK not found"`

**Causa**: Ordem de carga incorreta ou FK inexistente no legado

**Solução**:
1. Verificar ordem de carga (dimensões antes de fatos)
2. Verificar se FK existe no legado (query MySQL)
3. Ajustar `mapping_legacy_to_new.md` se necessário

#### Encoding inválido
**Sintoma**: Erros de encoding durante extração

**Causa**: Dump em latin1, mas lendo como UTF-8

**Solução**:
```python
# No script ETL, ajustar encoding
conn_mysql = pymysql.connect(
    host='localhost',
    charset='latin1',  # ou 'utf8mb4'
    ...
)
```

#### Timeout na carga
**Sintoma**: ETL demora muito (>1 hora)

**Causa**: Batch size muito pequeno ou falta de índices

**Solução**:
1. Aumentar `ETL_BATCH_SIZE` (ex: 50000)
2. Remover índices temporariamente durante carga
3. Usar `COPY FROM` ao invés de `INSERT` (mais rápido)

### 4.2 Logs e Debug

**Logs do ETL**:
- Console output (stdout/stderr)
- Arquivo `etl_<timestamp>.log` (se configurado)
- Tabela `etl_runs` (status geral)
- Tabela `etl_rejections` (detalhes de rejeições)

**Debug**:
```bash
# Verificar conexão MySQL
docker exec mysql-temp mysql -uroot -p$env:MYSQL_ROOT_PASSWORD -e "SELECT COUNT(*) FROM is_clientes;"

# Verificar conexão Postgres
psql -h $env:POSTGRES_HOST -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT COUNT(*) FROM is_clientes;"

# Ver rejeições recentes
psql -h $env:POSTGRES_HOST -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT * FROM etl_rejections ORDER BY created_at DESC LIMIT 10;"
```

---

## 5. Rollback

### 5.1 Em Caso de Falha

**Estratégia**: Como usamos TRUNCATE+RELOAD, não há "rollback" de dados, mas podemos:

1. **Restaurar backup**: Se houver backup anterior do Postgres
2. **Reexecutar ETL**: Corrigir problema e reexecutar
3. **Manter staging**: Manter dados em `staging.stg_*` para debug

**Comandos** (restaurar backup):
```bash
# Supabase: usar dashboard ou CLI
supabase db restore <backup-file>

# Postgres direto:
psql -h $env:POSTGRES_HOST -U $env:POSTGRES_USER -d $env:POSTGRES_DB < backup.sql
```

### 5.2 Validação Antes de Completar

**Checklist**:
- ✅ Contagens corretas (comparar com dump original)
- ✅ Sem FKs órfãs
- ✅ Sem UNIQUE violados
- ✅ Sem NOT NULL violados
- ✅ Somatórios financeiros corretos

**Se algum item falhar**: Não completar ETL, investigar problema

---

## 6. Performance e Otimização

### 6.1 Otimizações Recomendadas

**Durante carga**:
- Desabilitar índices temporariamente (DROP → CREATE após carga)
- Usar `COPY FROM` ao invés de `INSERT` (10-100x mais rápido)
- Aumentar `ETL_BATCH_SIZE` (ex: 50000)
- Usar transações em batches (não uma por linha)

**Post-carga**:
- Recriar índices
- Executar `ANALYZE` em todas as tabelas
- Validar constraints (`VALIDATE CONSTRAINT`)

### 6.2 Tempo Estimado

| Etapa | Tempo Estimado |
|-------|----------------|
| Validação dump | <1 min |
| MySQL Docker restore | 2-5 min |
| Extração MySQL | 5-15 min |
| Carga staging | 2-5 min |
| Transformação + Carga final | 10-20 min |
| Validação | 1-2 min |
| **Total** | **20-50 min** |

*Nota: Tempos podem variar com tamanho do dump e performance do servidor*

---

**Gerado em**: 2025-01-16  
**Próxima fase**: FASE 4 (Mapeamento legado → novo + NLQ)
