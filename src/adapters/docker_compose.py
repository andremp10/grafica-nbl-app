from __future__ import annotations

from pathlib import Path
from typing import Sequence

from src.utils.subprocess_utils import run_cmd


def detect_compose_cmd() -> list[str]:
    """Return the docker compose command as a list.

    Prefers: `docker compose`
    Fallback: `docker-compose`
    """
    try:
        run_cmd(["docker", "compose", "version"], check=True)
        return ["docker", "compose"]
    except Exception:
        return ["docker-compose"]


def compose_run(args: Sequence[str], *, cwd: Path) -> None:
    cmd = detect_compose_cmd() + list(args)
    run_cmd(cmd, cwd=cwd, check=True)

