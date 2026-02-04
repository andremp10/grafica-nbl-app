# Passo a Passo Atualizado (Foco em Produção) 🚀

> **LEGADO:** este documento é do stack antigo (Netlify). O app atual publicado é o **Streamlit** (`streamlit_app.py`) e usa `WEBHOOK_URL` (N8N) via Secrets/variáveis de ambiente.

Como o simulador local (`netlify dev`) deu erro no seu Windows, o caminho mais fácil e confiável é atualizar o site real (`nbl.golfine.com.br`).

## 1. Enviar as Mudanças para o GitHub

No terminal do seu VS Code, execute estes 3 comandos em ordem:

```bash
git add .
git commit -m "fix(deps): Adiciona tipos do React e expande Banco de Dados"
git push
```

*Isso enviará o novo cérebro do agente para a nuvem.*

## 2. Configurar a Chave na Netlify (CRÍTICO ⚠️)

O novo código **precisa** da chave da API para funcionar. Como não enviamos o arquivo `.env` (por segurança), você precisa colocar a chave lá manualmente.

1.  Acesse [app.netlify.com](https://app.netlify.com).
2.  Entre no site **nbl** (ou **grafica-nbl**).
3.  Vá em **Site configuration** > **Environment variables**.
4.  Clique em **Add a variable**.
    -   **Key**: `GEMINI_API_KEY`
    -   **Value**: `<SUA_CHAVE_GEMINI_AQUI>`
5.  Clique em **Create variable**.
6.  Vá na aba **Deploys** e clique em **Trigger deploy** (ou espere ele detectar o push do passo 1).

## 3. Testar

Assim que o deploy terminar na Netlify (leva ~1 minuto):
1.  Acesse `https://nbl.golfine.com.br/`.
2.  Abra o chat e teste:
    -   "Quais pedidos estão em produção?"
    -   "Qual o valor total desses pedidos?"

Se responder corretamente, sua Consultoria AI está completa e no ar! 🎉
