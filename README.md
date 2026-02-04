# Gráfica NBL Admin

Sistema de gestão inteligente para gráfica com dashboard, pedidos e chat IA.

## 🚀 Live Demo
**Acesse aqui:** [https://arthurpessoaa.github.io/Grafica_project/](https://arthurpessoaa.github.io/Grafica_project/)

## 🚀 Stack

- **Frontend**: React + TypeScript + Vite
- **Styling**: Tailwind CSS
- **IA**: Google Gemini via Netlify Functions
- **Backend/ETL**: Python (migração MySQL → Supabase)

## ⚡ Quick Start

### Frontend (React)

```bash
# Instalar dependências
npm install

# Rodar em desenvolvimento
npm run dev

# Build para produção
npm run build
```

### ETL (Python)

```bash
# Criar ambiente virtual
python -m venv venv
venv\Scripts\activate  # Windows

# Instalar dependências
pip install -r requirements.txt

# Configurar variáveis
cp .env.example .env
# Editar .env com suas credenciais

# Rodar migração
python -m src.main --sql ./sql_input/seu_dump.sql
```

## 🔧 Configuração

### Variáveis de Ambiente

Copie `.env.example` para `.env` e configure:

| Variável | Descrição |
|----------|-----------|
| `SUPABASE_URL` | URL do projeto Supabase |
| `SUPABASE_KEY` | Service role key |
| `PG_HOST` | Host do Postgres |
| `GEMINI_API_KEY` | Chave da API Gemini |
| `VITE_WEBHOOK_URL` | (Opcional) Webhook externo para IA |

## 📁 Estrutura

```
├── components/     # Componentes React
├── services/       # Serviços (IA, API)
├── src/            # ETL Python
│   ├── etl/        # Migração MySQL→Supabase
│   ├── adapters/   # Conectores
│   └── utils/      # Utilitários
├── config/         # Mapeamentos
├── docs/           # Documentação
└── netlify/        # Funções serverless
```

## 🌐 Deploy

### GitHub Pages (Automático)

1. O deploy é feito automaticamente a cada push na branch `main`.
2. Configure o segredo `VITE_WEBHOOK_URL` em **Settings > Secrets and variables > Actions**.
3. Acesse a aba **Actions** para conferir o status.

## 📄 Licença

MIT
