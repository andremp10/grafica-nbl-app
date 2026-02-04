# Guia de Integração: Gemini + Netlify Functions

A integração foi modificada para ser **segura** e **profissional**. Agora, a lógica de inteligência artificial roda no backend (Netlify Functions), protegendo sua chave de API e permitindo controle total.

## 🚀 Como Rodar o Projeto

Para testar a integração localmente, você precisa simular o ambiente da Netlify.

### 1. Pré-requisitos
- Node.js instalado.
- Uma chave de API do Google Gemini (obtenha em [aistudio.google.com](https://aistudio.google.com/)).

### 2. Configurar Variáveis de Ambiente
Crie um arquivo chamado `.env` na raiz do projeto (ao lado do `package.json`) e adicione sua chave:

```env
GEMINI_API_KEY=sua_chave_comecando_com_AIzb...
```

### 3. Instalar Dependências
Abra o terminal na pasta do projeto e execute:

```bash
npm install
npm install -g netlify-cli
```
*Note: O `netlify-cli` é necessário para rodar as funções localmente.*

### 4. Rodar a Aplicação
Como agora usamos Funções Serverless, não use apenas `npm run dev`. Use o comando da Netlify:

```bash
netlify dev
```

O Netlify irá iniciar:
- O servidor Frontend (Vite)
- O servidor Backend (Functions)
- Um Proxy local (geralmente em `http://localhost:8888`)

**Acesse o projeto pela URL fornecida pelo Netlify (ex: `http://localhost:8888`) para que a comunicação funcione.**

## 🛠️ Como Funciona
1. O Frontend envia a mensagem para `/.netlify/functions/chat`.
2. A Função `chat.js`:
    - Recebe a mensagem.
    - Carrega os dados simulados (`MOCK_ORDERS`).
    - Monta um prompt de "Consultor Especializado".
    - Envia para o Google Gemini de forma segura.
3. A resposta volta para o frontend.

## 📦 Deploy
Para colocar no ar:
1. Faça commit das alterações.
2. Conecte seu repositório à Netlify.
3. Nas configurações do site na Netlify, vá em **Site configuration > Environment variables** e adicione `GEMINI_API_KEY`.
