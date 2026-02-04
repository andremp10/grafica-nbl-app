from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Sequence


class CommandError(RuntimeError):
    def __init__(self, cmd: Sequence[str], returncode: int, stdout: str, stderr: str):
        super().__init__(f"Command failed ({returncode}): {' '.join(cmd)}\n{stderr}".strip())
        self.cmd = list(cmd)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def run_cmd(
    cmd: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    text: bool = True,
) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        list(cmd),
        cwd=str(cwd) if cwd else None,
        env=env,
        check=False,
        capture_output=True,
        text=text,
    )
    if check and proc.returncode != 0:
        raise CommandError(list(cmd), proc.returncode, proc.stdout or "", proc.stderr or "")
    return proc

