#!/usr/bin/env python3
"""
daily_job.py — Orquestrador do job ETL diário.

Fluxo (fail-fast):
  1. Baixar backup (SFTP → FTP fallback)
  2. Validar backup (tamanho + magic bytes)
  3. Importar no MySQL Docker
  4. Truncar Supabase  ← só executa se passos 1-3 OK
  5. Rodar ETL         ← só executa se passo 4 OK
  6. Derrubar container MySQL
  7. Salvar resumo em logs/

Uso:
  python scripts/daily_job.py
  python scripts/daily_job.py --dry-run        # valida apenas (sem truncar/carregar)
  python scripts/daily_job.py --skip-fetch     # pula fetch (reutiliza backup existente)
  python scripts/daily_job.py --backup-date 2025-01-15
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Ajusta path para importar módulos locais
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from scripts.check_env import check_env
from scripts.fetch_backup import fetch_backup
from scripts.import_dump import import_dump, _wait_for_mysql
from scripts.truncate_supabase import truncate_supabase

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
COMPOSE_FILE    = os.getenv("DOCKER_COMPOSE_FILE", "docker-compose.yml")
LOGS_DIR        = Path(os.getenv("LOGS_DIR", "./logs"))
TRUNCATE_ENABLED = os.getenv("TRUNCATE_ENABLED", "0") == "1"
ETL_VALIDATE_ONLY = os.getenv("ETL_VALIDATE_ONLY", "0") == "1"

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
LOGS_DIR.mkdir(parents=True, exist_ok=True)
_log_file = LOGS_DIR / f"daily_{datetime.now().strftime('%Y%m%d_%H%M')}.log"

_handlers = [
    logging.StreamHandler(sys.stdout),
    logging.FileHandler(_log_file, encoding="utf-8"),
]
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=_handlers,
)
log = logging.getLogger("daily_job")


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
class StepTimer:
    def __init__(self, name: str):
        self.name = name
        self._start: float = 0.0
        self.elapsed: float = 0.0

    def __enter__(self):
        log.info("▶ Iniciando: %s", self.name)
        self._start = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.monotonic() - self._start
        if exc_type:
            log.error("✗ Falhou: %s (%.1fs)", self.name, self.elapsed)
        else:
            log.info("✔ Concluído: %s (%.1fs)", self.name, self.elapsed)
        return False  # re-raise exceptions


def _docker_compose_down_mysql() -> None:
    """Para o container MySQL (não remove volumes)."""
    log.info("Parando container MySQL...")
    try:
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "stop", "mysql"],
            check=True,
            capture_output=True,
        )
        log.info("Container MySQL parado.")
    except subprocess.CalledProcessError as exc:
        log.warning("Não foi possível parar o container MySQL: %s", exc.stderr)


def _run_etl(dry_run: bool = False) -> None:
    """Executa etl/run.py como subprocess para isolar imports e env vars."""
    etl_script = _ROOT / "etl" / "run.py"
    env = os.environ.copy()
    if dry_run:
        env["ETL_VALIDATE_ONLY"] = "1"

    log.info("Executando ETL: %s", etl_script)
    result = subprocess.run(
        [sys.executable, str(etl_script)],
        env=env,
        capture_output=False,  # herda stdout/stderr para aparecer no log
    )
    if result.returncode != 0:
        raise RuntimeError(f"ETL falhou com código de saída {result.returncode}.")


def _write_summary(steps: list[dict]) -> None:
    """Escreve resumo do job no arquivo de log."""
    summary_path = LOGS_DIR / f"summary_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    total = sum(s["elapsed"] for s in steps)
    lines = [
        "=" * 60,
        f"NBL ETL Daily Job — {datetime.now().isoformat(timespec='seconds')}",
        "=" * 60,
    ]
    for s in steps:
        status = "OK  " if s["ok"] else "FAIL"
        lines.append(f"  [{status}] {s['name']:<35} {s['elapsed']:6.1f}s")
    lines += [
        "-" * 60,
        f"  Total: {total:.1f}s",
        "=" * 60,
    ]
    text = "\n".join(lines)
    log.info("\n%s", text)
    summary_path.write_text(text + "\n", encoding="utf-8")
    log.info("Resumo salvo em: %s", summary_path)


# ─────────────────────────────────────────────────────────────
# JOB PRINCIPAL
# ─────────────────────────────────────────────────────────────
def run_daily_job(
    dry_run: bool = False,
    skip_fetch: bool = False,
    backup_path_override: Path | None = None,
    backup_date: date | None = None,
) -> None:
    # ── Passo 0: Fail-fast env check ──────────────────────────
    check_env(dry_run=dry_run)

    steps: list[dict] = []
    backup_path: Path | None = backup_path_override

    # ── Passo 1: Fetch backup ──────────────────────────────────
    if not skip_fetch:
        with StepTimer("1. Fetch backup") as t:
            backup_path = fetch_backup(target_date=backup_date)
        steps.append({"name": "1. Fetch backup", "ok": True, "elapsed": t.elapsed})
    else:
        log.info("skip-fetch ativado — usando backup: %s", backup_path)
        steps.append({"name": "1. Fetch backup (skipped)", "ok": True, "elapsed": 0.0})

    if backup_path is None:
        raise RuntimeError("Nenhum backup disponível para importar.")

    # ── Passo 2: Import dump ──────────────────────────────────
    if not dry_run:
        with StepTimer("2. Importar dump MySQL") as t:
            import_dump(backup_path)
        steps.append({"name": "2. Importar dump MySQL", "ok": True, "elapsed": t.elapsed})
    else:
        log.info("dry-run: pulando importação MySQL.")
        steps.append({"name": "2. Importar dump MySQL (dry-run)", "ok": True, "elapsed": 0.0})

    # ── Passo 3: Truncate Supabase ────────────────────────────
    if TRUNCATE_ENABLED and not dry_run:
        with StepTimer("3. Truncar Supabase") as t:
            truncate_supabase()
        steps.append({"name": "3. Truncar Supabase", "ok": True, "elapsed": t.elapsed})
    else:
        reason = "dry-run" if dry_run else "TRUNCATE_ENABLED=0"
        log.info("Truncagem pulada (%s).", reason)
        steps.append({"name": "3. Truncar Supabase (skipped)", "ok": True, "elapsed": 0.0})

    # ── Passo 4: ETL ──────────────────────────────────────────
    with StepTimer("4. ETL MySQL → Supabase") as t:
        _run_etl(dry_run=dry_run)
    steps.append({"name": "4. ETL MySQL → Supabase", "ok": True, "elapsed": t.elapsed})

    # ── Passo 5: Docker down ──────────────────────────────────
    if not dry_run:
        with StepTimer("5. Parar MySQL Docker") as t:
            _docker_compose_down_mysql()
        steps.append({"name": "5. Parar MySQL Docker", "ok": True, "elapsed": t.elapsed})

    # ── Resumo ────────────────────────────────────────────────
    _write_summary(steps)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Orquestrador do job ETL diário.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida apenas (sem truncar/carregar). Implica ETL_VALIDATE_ONLY=1.",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Não baixa novo backup; reutiliza arquivo existente.",
    )
    parser.add_argument(
        "--backup-file",
        metavar="PATH",
        help="Usar este arquivo de backup (implica --skip-fetch).",
    )
    parser.add_argument(
        "--backup-date",
        metavar="YYYY-MM-DD",
        help="Data do backup a baixar (default: hoje).",
    )
    args = parser.parse_args()

    backup_path_override: Path | None = None
    skip_fetch = args.skip_fetch

    if args.backup_file:
        backup_path_override = Path(args.backup_file).resolve()
        skip_fetch = True

    backup_date: date | None = None
    if args.backup_date:
        backup_date = date.fromisoformat(args.backup_date)

    if args.dry_run:
        os.environ["ETL_VALIDATE_ONLY"] = "1"

    log.info(
        "=== NBL ETL Daily Job | %s | dry_run=%s ===",
        datetime.now().isoformat(timespec="seconds"),
        args.dry_run,
    )

    try:
        run_daily_job(
            dry_run=args.dry_run,
            skip_fetch=skip_fetch,
            backup_path_override=backup_path_override,
            backup_date=backup_date,
        )
        log.info("=== Job concluído com SUCESSO ===")
    except Exception as exc:
        log.error("=== Job FALHOU: %s ===", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
