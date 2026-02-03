from __future__ import annotations

from pathlib import Path

import re
import time

from src.adapters.docker_compose import compose_run


_SAFE_DB_RE = re.compile(r"^[A-Za-z0-9_]+$")


def ensure_mysql_up(*, repo_root: Path) -> None:
    compose_run(["up", "-d", "mysql"], cwd=repo_root)
    wait_for_mysql(repo_root=repo_root)


def wait_for_mysql(*, repo_root: Path, timeout_s: int = 120) -> None:
    """Wait until the MySQL container responds to ping."""
    start = time.time()
    last_err: Exception | None = None
    while time.time() - start < timeout_s:
        try:
            compose_run(
                ["exec", "-T", "mysql", "sh", "-lc", "mysqladmin ping -h localhost -p$MYSQL_ROOT_PASSWORD --silent"],
                cwd=repo_root,
            )
            return
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"MySQL não ficou pronto em {timeout_s}s") from last_err


def reset_and_import_dump(*, repo_root: Path, sql_filename: str, mysql_db: str) -> None:
    if not _SAFE_DB_RE.match(mysql_db or ""):
        raise ValueError(f"MYSQL_DATABASE inválido: {mysql_db!r}")
    # Drop + recreate db (safe: local docker only)
    compose_run(
        [
            "exec",
            "-T",
            "mysql",
            "sh",
            "-lc",
            f"mysql -uroot -p$MYSQL_ROOT_PASSWORD -e \"DROP DATABASE IF EXISTS {mysql_db}; CREATE DATABASE {mysql_db};\"",
        ],
        cwd=repo_root,
    )
    compose_run(
        [
            "exec",
            "-T",
            "mysql",
            "sh",
            "-lc",
            f"mysql -uroot -p$MYSQL_ROOT_PASSWORD {mysql_db} < /sql_input/{sql_filename}",
        ],
        cwd=repo_root,
    )
