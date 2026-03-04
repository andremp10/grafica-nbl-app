#!/usr/bin/env python3
"""
import_dump.py — Importa dump MySQL (.sql ou .sql.gz) no container Docker.

Fluxo:
1. Copia o arquivo para sql_input/ (volume montado no container mysql)
2. Sobe o container MySQL via docker compose (se não estiver rodando)
3. Aguarda healthcheck
4. Importa o dump (descomprimindo .gz via pipe se necessário)
5. Verifica que a importação gerou dados (SELECT COUNT(*) FROM is_pedidos)

Uso standalone:
  python scripts/import_dump.py --file ./backups/nblgrafica_app-2025-01-15.sql.gz
  python scripts/import_dump.py --file ./backups/nblgrafica_app-2025-01-15.sql.gz --skip-compose
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "nblgrafica_app")

COMPOSE_FILE   = os.getenv("DOCKER_COMPOSE_FILE", "docker-compose.yml")
SQL_INPUT_DIR  = Path(os.getenv("SQL_INPUT_DIR", "./sql_input"))

HEALTHCHECK_RETRIES = int(os.getenv("MYSQL_HEALTHCHECK_RETRIES", "30"))
HEALTHCHECK_SLEEP   = float(os.getenv("MYSQL_HEALTHCHECK_SLEEP", "2"))

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("import_dump")


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def _run(cmd: list[str], check: bool = True, capture: bool = False, **kw) -> subprocess.CompletedProcess:
    log.debug("$ %s", " ".join(cmd))
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
        **kw,
    )


def _mysql_is_ready() -> bool:
    """Tenta conectar no MySQL. Retorna True se bem-sucedido."""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            connection_timeout=3,
        )
        conn.close()
        return True
    except mysql.connector.Error:
        return False


def _wait_for_mysql() -> None:
    """Aguarda MySQL ficar disponível (healthcheck)."""
    log.info(
        "Aguardando MySQL em %s:%d (máx %d tentativas)...",
        MYSQL_HOST, MYSQL_PORT, HEALTHCHECK_RETRIES,
    )
    for attempt in range(1, HEALTHCHECK_RETRIES + 1):
        if _mysql_is_ready():
            log.info("MySQL disponível (tentativa %d).", attempt)
            return
        log.debug("MySQL não disponível ainda (tentativa %d/%d).", attempt, HEALTHCHECK_RETRIES)
        time.sleep(HEALTHCHECK_SLEEP)
    raise TimeoutError(
        f"MySQL não ficou disponível após {HEALTHCHECK_RETRIES} tentativas."
    )


def _compose_up_mysql() -> None:
    """Sobe o serviço mysql via docker compose."""
    log.info("docker compose up -d mysql...")
    _run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "mysql"])


def _import_dump(dump_path: Path) -> None:
    """Importa o dump no MySQL. Suporta .sql e .sql.gz."""
    is_gz = dump_path.suffix == ".gz"
    log.info("Importando dump: %s (%s)", dump_path.name, "gzip" if is_gz else "plain sql")

    mysql_cmd = [
        "docker", "compose", "-f", COMPOSE_FILE,
        "exec", "-T", "mysql",
        "mysql",
        f"-u{MYSQL_USER}",
        f"-p{MYSQL_PASSWORD}",
        MYSQL_DATABASE,
    ]

    if is_gz:
        # zcat dump.sql.gz | docker exec -i mysql mysql ...
        zcat_proc = subprocess.Popen(
            ["zcat", str(dump_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        mysql_proc = subprocess.Popen(
            mysql_cmd,
            stdin=zcat_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        zcat_proc.stdout.close()
        mysql_stdout, mysql_stderr = mysql_proc.communicate()
        zcat_rc = zcat_proc.wait()

        if zcat_rc != 0:
            _, zcat_err = zcat_proc.communicate()
            raise subprocess.CalledProcessError(
                zcat_rc, "zcat", stderr=zcat_err.decode(errors="replace") if isinstance(zcat_err, bytes) else zcat_err
            )
        if mysql_proc.returncode != 0:
            raise subprocess.CalledProcessError(
                mysql_proc.returncode, "mysql", stderr=mysql_stderr
            )
    else:
        with open(dump_path, "rb") as f:
            proc = subprocess.run(
                mysql_cmd,
                stdin=f,
                capture_output=True,
                text=True,
            )
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode, "mysql", stderr=proc.stderr
            )

    log.info("Dump importado com sucesso.")


def _verify_import() -> None:
    """Verifica que is_pedidos tem dados após importação."""
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM is_pedidos")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError("is_pedidos está vazia após importação — dump pode estar corrompido.")
        log.info("Verificação OK: is_pedidos contém %d registros.", count)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
# PONTO DE ENTRADA
# ─────────────────────────────────────────────────────────────
def import_dump(dump_path: Path, skip_compose: bool = False) -> None:
    """
    Importa dump no MySQL Docker.

    Args:
        dump_path: Caminho para o arquivo .sql ou .sql.gz
        skip_compose: Se True, não executa docker compose up (MySQL já está rodando)
    """
    dump_path = Path(dump_path).resolve()
    if not dump_path.exists():
        raise FileNotFoundError(f"Dump não encontrado: {dump_path}")

    # Copiar para sql_input/ (volume do container)
    SQL_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = SQL_INPUT_DIR / dump_path.name
    if dest != dump_path:
        log.info("Copiando dump para sql_input/: %s", dest)
        shutil.copy2(dump_path, dest)

    if not skip_compose:
        _compose_up_mysql()

    _wait_for_mysql()
    _import_dump(dump_path)
    _verify_import()


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa dump MySQL no container Docker.")
    parser.add_argument(
        "--file",
        required=True,
        metavar="PATH",
        help="Caminho do arquivo dump (.sql ou .sql.gz).",
    )
    parser.add_argument(
        "--skip-compose",
        action="store_true",
        help="Não sobe o container MySQL (já está rodando).",
    )
    args = parser.parse_args()

    try:
        import_dump(Path(args.file), skip_compose=args.skip_compose)
    except Exception as exc:
        log.error("Falha fatal: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
