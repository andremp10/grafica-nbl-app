# Gráfica NBL Admin 🎨

Sistema de gestão premium e Chat Inteligente para gráficas. Desenvolvido em **Streamlit**.

## 🚀 Como Rodar (Local)

1.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure o Ambiente:**
    *   Crie um arquivo `.env` na raiz.
    *   Adicione sua URL do N8N:
        ```bash
        WEBHOOK_URL="https://webhook-pre.golfine.com.br/webhook/..."
        ```
        *(Compatibilidade: também aceitamos `VITE_WEBHOOK_URL`.)*

3.  **Execute o App:**
    ```bash
    streamlit run streamlit_app.py
    ```

---

## ☁️ Como Fazer Deploy (Streamlit Cloud)

O jeito mais fácil, gratuito e rápido de colocar este app no ar é usando a **Streamlit Cloud**.

1.  Acesse: [share.streamlit.io](https://share.streamlit.io/)
2.  Faça login com seu GitHub.
3.  Clique em **"New App"**.
4.  Selecione este repositório.
5.  **Main file path:** `streamlit_app.py`
6.  **Advanced Settings (Secrets):**
    *   Configure em formato TOML (ex.: `WEBHOOK_URL = "..."`).
7.  Clique em **Deploy!** 🚀

O App ficará online em minutos com HTTPS automático.

---

## 📂 Estrutura

*   `streamlit_app.py`: Interface principal e lógica.
*   `.streamlit/config.toml`: Configuração do Tema Dark Premium.
*   `services/`: Integração com N8N.
