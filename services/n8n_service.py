from __future__ import annotations

import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests
import streamlit as st

DEFAULT_CONNECT_TIMEOUT_S = 30  # Conexão inicial
DEFAULT_READ_TIMEOUT_S = 600  # 10 minutos - sempre aguarda o agente
DEFAULT_MAX_MESSAGE_CHARS = 8000


def _get_secret(key: str) -> Optional[str]:
    try:
        value = st.secrets.get(key)  # type: ignore[attr-defined]
    except Exception:
        value = None

    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def get_webhook_url() -> Optional[str]:
    """Retorna a URL do webhook, preferindo secrets do Streamlit Cloud."""
    url = (
        _get_secret("WEBHOOK_URL")
        or _get_secret("VITE_WEBHOOK_URL")
        or os.getenv("WEBHOOK_URL")
        or os.getenv("VITE_WEBHOOK_URL")
        or os.getenv("N8N_WEBHOOK_URL")
    )
    if not url:
        return None

    return url.strip().strip('"').strip("'")


def parse_n8n_response(data: Any) -> str:
    """
    Parseia a resposta do N8N de forma robusta.
    Suporta múltiplos formatos de output.
    """
    if data is None:
        return ""
    
    # Se for string direta, retorna
    if isinstance(data, str):
        return data.strip()
    
    # Se for lista, tenta extrair o primeiro elemento
    if isinstance(data, list):
        if len(data) == 0:
            return ""
        first_item = data[0]
        if isinstance(first_item, str):
            return first_item.strip()
        if isinstance(first_item, dict):
            # Tenta múltiplos campos comuns
            for key in ['text', 'output', 'response', 'message', 'content', 'result']:
                if key in first_item and first_item[key]:
                    return str(first_item[key]).strip()
            return str(first_item)
        return str(first_item)
    
    # Se for dict, tenta extrair campos comuns
    if isinstance(data, dict):
        # Ordem de prioridade para campos de resposta
        for key in ['text', 'output', 'response', 'message', 'content', 'result', 'data']:
            if key in data and data[key]:
                value = data[key]
                # Se o valor for uma lista ou dict, processa recursivamente
                if isinstance(value, (list, dict)):
                    return parse_n8n_response(value)
                return str(value).strip()
        # Fallback: retorna representação do dict
        return str(data)
    
    return str(data)


def _truncate(text: str, max_len: int = 400) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def probe_webhook(timeout_s: int = 8) -> Tuple[bool, str]:
    """
    Teste simples (manual) de conectividade com o webhook.
    Não roda automaticamente na UI para evitar execuções indesejadas no N8N.
    """
    url = get_webhook_url()
    if not url:
        return False, "Webhook não configurado (defina WEBHOOK_URL)."

    request_id = uuid.uuid4().hex
    payload = {
        "message": "__ping__",
        "history": [],
        "ping": True,
        "client_request_id": request_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    try:
        resp = requests.post(
            url,
            json=payload,
            timeout=(min(DEFAULT_CONNECT_TIMEOUT_S, timeout_s), timeout_s),
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code >= 400:
            return False, f"HTTP {resp.status_code}"
        return True, f"HTTP {resp.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.ConnectionError:
        return False, "Falha de conexão"
    except requests.exceptions.RequestException as e:
        return False, _truncate(str(e), 120)


def send_message_to_n8n(message: str, history: Optional[List[Dict[str, Any]]] = None) -> str:
    """
    Envia uma mensagem para o webhook do N8N e retorna a resposta.
    
    Args:
        message: A mensagem do usuário
        history: Histórico de conversas (opcional)
    
    Returns:
        Texto da resposta do N8N
    """
    message = (message or "").strip()
    if not message:
        return "⚠️ Digite uma mensagem antes de enviar."

    max_chars = int(os.getenv("MAX_MESSAGE_CHARS", str(DEFAULT_MAX_MESSAGE_CHARS)))
    if len(message) > max_chars:
        return f"⚠️ Mensagem muito longa ({len(message)} caracteres). Limite: {max_chars}."

    webhook_url = get_webhook_url()
    if not webhook_url:
        return (
            "⚙️ Webhook não configurado.\n\n"
            "Defina `WEBHOOK_URL` nas Secrets do Streamlit Cloud (ou no `.env` local) e tente novamente."
        )

    client_request_id = uuid.uuid4().hex
    
    # Prepara o payload
    payload = {
        "message": message,
        "history": history or [],
        "client_request_id": client_request_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    
    try:
        connect_timeout_s = float(os.getenv("CONNECT_TIMEOUT_S", str(DEFAULT_CONNECT_TIMEOUT_S)))
        read_timeout_s = float(os.getenv("READ_TIMEOUT_S", str(DEFAULT_READ_TIMEOUT_S)))

        # Faz a requisição com timeout (agentes podem demorar)
        response = requests.post(
            webhook_url, 
            json=payload, 
            timeout=(connect_timeout_s, read_timeout_s),
            headers={"Content-Type": "application/json"}
        )
        n8n_request_id = response.headers.get("x-request-id") or response.headers.get("x-amzn-trace-id") or ""
        
        # Tratamento de erros HTTP
        if response.status_code >= 400:
            error_detail = ""
            try:
                error_data = response.json()
                error_detail = error_data.get('message', '') or error_data.get('error', '')
            except Exception:
                error_detail = response.text[:200] if response.text else "Sem detalhes"

            req_ids = f"client_request_id={client_request_id}"
            if n8n_request_id:
                req_ids += f" | n8n_request_id={n8n_request_id}"
            
            if response.status_code == 500:
                return (
                    "⚠️ Erro no Servidor N8N (500)\n\n"
                    f"Detalhes: {_truncate(str(error_detail), 240)}\n"
                    f"IDs: {req_ids}\n\n"
                    "Possíveis causas:\n"
                    "• O fluxo do AI Agent não está conectado ao \"Respond to Webhook\"\n"
                    "• Erro interno no processamento do agente\n"
                    "• Timeout na execução do workflow\n\n"
                    "Tente novamente em alguns segundos."
                )
            
            return f"❌ Erro {response.status_code}: {_truncate(str(error_detail), 240)}\n\nIDs: {req_ids}"
        
        # Tenta parsear a resposta
        try:
            data = response.json()
            return parse_n8n_response(data) or "✅ Processado (sem resposta de texto)"
        except ValueError:
            # Se não for JSON, retorna o texto bruto
            return response.text.strip() if response.text else "✅ Processado"
        
    except requests.exceptions.Timeout:
        return (
            "⏳ O servidor demorou muito para responder.\n\n"
            f"ID: {client_request_id}\n\n"
            "O workflow pode estar processando uma tarefa complexa. Tente novamente."
        )
    
    except requests.exceptions.ConnectionError:
        return (
            "🔌 Não foi possível conectar ao servidor N8N.\n\n"
            f"ID: {client_request_id}\n\n"
            "Verifique se o serviço está online e se a URL está correta."
        )
    
    except requests.exceptions.RequestException as e:
        return f"❌ Erro de comunicação: {_truncate(str(e), 240)}\n\nID: {client_request_id}"
