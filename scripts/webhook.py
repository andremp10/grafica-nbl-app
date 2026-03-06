#!/usr/bin/env python3
"""
NBL ETL — Webhook HTTP
=======================
Permite que orquestradores externos (n8n, Zapier, CI/CD, etc.)
disparem a execução do ETL via POST HTTP sem acesso direto ao servidor.

Uso:
    python scripts/webhook.py [--port 8080] [--host 0.0.0.0]

Variáveis de ambiente:
    WEBHOOK_PORT   Porta TCP (default: 8080)
    WEBHOOK_TOKEN  Shared secret exigido no header X-Webhook-Token.
                   Se vazio, o endpoint aceita qualquer requisição (dev mode).

Endpoints:
    POST /webhook/etl   Dispara o ETL daily_job.py
    GET  /health        Health-check (retorna 200 + JSON)
    GET  /status        Status do último run (JSON)

Exemplo de chamada:
    curl -X POST http://localhost:8080/webhook/etl \\
         -H "X-Webhook-Token: meu_token_secreto" \\
         -H "Content-Type: application/json" \\
         -d '{"triggered_by": "n8n", "env": "production"}'
"""

from __future__ import annotations

import hashlib
import hmac
import http.server
import json
import logging
import os
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

WEBHOOK_PORT: int = int(os.getenv("WEBHOOK_PORT", "8080"))
WEBHOOK_TOKEN: str = os.getenv("WEBHOOK_TOKEN", "")

LOGS_DIR = Path(os.getenv("LOGS_DIR", "./logs"))
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "webhook.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("webhook")

# ── Estado global do ETL ─────────────────────────────────────────────────────
_etl_lock = threading.Lock()
_run_state: dict[str, Any] = {
    "status": "idle",         # idle | running | success | failed | error
    "started_at": None,
    "finished_at": None,
    "pid": None,
    "returncode": None,
    "triggered_by": None,
    "error": None,
    "run_count": 0,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _run_etl_background(triggered_by: str = "webhook") -> None:
    """Executa daily_job.py em background thread; atualiza _run_state."""
    global _run_state
    if not _etl_lock.acquire(blocking=False):
        log.warning("ETL já em execução — trigger de '%s' ignorado", triggered_by)
        return

    _run_state.update(
        status="running",
        started_at=_utc_now(),
        finished_at=None,
        pid=None,
        returncode=None,
        triggered_by=triggered_by,
        error=None,
    )
    _run_state["run_count"] += 1

    try:
        cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / "daily_job.py")]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(PROJECT_ROOT),
        )
        _run_state["pid"] = proc.pid
        log.info("ETL disparado (PID=%s) por '%s'", proc.pid, triggered_by)

        proc.wait()
        rc = proc.returncode
        _run_state["returncode"] = rc
        _run_state["status"] = "success" if rc == 0 else "failed"
        log.info("ETL finalizado (PID=%s, rc=%s)", proc.pid, rc)

    except Exception as exc:
        _run_state["status"] = "error"
        _run_state["error"] = str(exc)
        log.error("ETL erro interno: %s", exc)

    finally:
        _run_state["finished_at"] = _utc_now()
        _etl_lock.release()


def _verify_token(provided: str) -> bool:
    """Compara token com hmac.compare_digest para evitar timing attacks."""
    if not WEBHOOK_TOKEN:
        return True  # Dev mode: sem token configurado → aceitar tudo
    if not provided:
        return False
    return hmac.compare_digest(
        hashlib.sha256(provided.encode()).hexdigest(),
        hashlib.sha256(WEBHOOK_TOKEN.encode()).hexdigest(),
    )


class WebhookHandler(http.server.BaseHTTPRequestHandler):
    """Handler HTTP minimalista sem dependências externas."""

    server_version = "NBL-ETL-Webhook/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # suppress default access log
        log.debug(fmt, *args)

    # ── helpers ──────────────────────────────────────────────────────────────
    def _send_json(self, status: int, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}

    # ── GET ──────────────────────────────────────────────────────────────────
    def do_GET(self) -> None:
        if self.path in ("/health", "/health/"):
            self._send_json(200, {
                "status": "ok",
                "service": "nbl-etl-webhook",
                "etl_running": _run_state["status"] == "running",
            })

        elif self.path in ("/status", "/status/"):
            self._send_json(200, dict(_run_state))

        else:
            self._send_json(404, {"error": "endpoint not found", "path": self.path})

    # ── POST ─────────────────────────────────────────────────────────────────
    def do_POST(self) -> None:
        if self.path not in ("/webhook/etl", "/webhook/etl/"):
            self._send_json(404, {"error": "endpoint not found"})
            return

        # Token verification
        token = self.headers.get("X-Webhook-Token", "")
        if not _verify_token(token):
            log.warning(
                "Requisição não autorizada de %s:%s",
                *self.client_address,
            )
            self._send_json(401, {"error": "unauthorized — header X-Webhook-Token inválido"})
            return

        # Parse payload
        payload = self._read_json_body()
        triggered_by = payload.get("triggered_by", f"{self.client_address[0]}")

        # Conflict: já está rodando
        if _run_state["status"] == "running":
            self._send_json(409, {
                "message": "ETL já está em execução — aguarde o término.",
                "status": dict(_run_state),
            })
            return

        # Disparar em background
        thread = threading.Thread(
            target=_run_etl_background,
            args=(triggered_by,),
            daemon=True,
            name="etl-worker",
        )
        thread.start()

        log.info(
            "ETL disparado via webhook por '%s' (payload=%s)",
            triggered_by,
            json.dumps(payload),
        )
        self._send_json(202, {
            "message": "ETL aceito e em execução em background.",
            "status": "accepted",
            "triggered_by": triggered_by,
            "accepted_at": _utc_now(),
        })


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="NBL ETL Webhook Server")
    parser.add_argument("--port", type=int, default=WEBHOOK_PORT, help="Porta TCP (default: 8080)")
    parser.add_argument("--host", default="0.0.0.0", help="Interface de escuta")
    args = parser.parse_args()

    if not WEBHOOK_TOKEN:
        log.warning(
            "WEBHOOK_TOKEN não configurado — endpoint aceita qualquer requisição! "
            "Defina WEBHOOK_TOKEN no .env para produção."
        )

    server = http.server.HTTPServer((args.host, args.port), WebhookHandler)
    log.info("Webhook server iniciado em http://%s:%s", args.host, args.port)
    log.info("Endpoints:")
    log.info("  POST  /webhook/etl   (X-Webhook-Token: %s)", "<token>" if WEBHOOK_TOKEN else "<sem token>")
    log.info("  GET   /health")
    log.info("  GET   /status")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Webhook server encerrado.")
        server.server_close()


if __name__ == "__main__":
    main()
