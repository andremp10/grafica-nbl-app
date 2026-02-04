# 🚨 ATENÇÃO: COMO LIGAR O CÉREBRO DO AGENTE 🚨

> **LEGADO:** este documento é do stack antigo (Netlify + Gemini). No app Streamlit (`streamlit_app.py`), a integração atual é via webhook N8N (`WEBHOOK_URL`).

O agente está "desligado" porque o servidor da Netlify não tem a senha dele. O arquivo `.env` que está no seu computador **NÃO** sobe para internet (por segurança).

Você precisa fazer isso manualmente UMA VEZ. Siga os passos:

### PASSO 1: Copie esta Chave
Selecione e copie o código abaixo (essa é a senha que está no seu computador):

`<SUA_CHAVE_GEMINI_AQUI>`

---

### PASSO 2: Coloque na Netlify
1. Abra o painel do seu site: [https://app.netlify.com/](https://app.netlify.com/)
2. Clique no seu projeto (**grafica-nbl**).
3. No menu lateral esquerdo, clique em **Site configuration**.
4. Depois clique em **Environment variables** (Variáveis de Ambiente).
5. Clique no botão azul **Add a variable**.
6. Preencha assim:
   - **Key:** `GEMINI_API_KEY`  (Escreva exatamente assim, tudo maiúsculo)
   - **Value:** (Cole a chave que você copiou no PASSO 1)
7. Clique em **Create variable**.

---

### PASSO 3: Reiniciar o Site
Depois de criar a variável, o site não percebe na hora. Você precisa "avisar" ele.

1. Vá na aba **Deploys** (no menu superior).
2. Clique no primeiro item da lista ("Production").
3. Clique em **Retry deploy** > **Clear cache and deploy site**.

**OU** apenas rode este comando no seu terminal agora, que eu forço essa atualização para você:

```bash
git commit --allow-empty -m "trigger: Ligar Agente"
git push
```

⏳ Espere uns 2 minutos e teste. O agente VAI funcionar.
