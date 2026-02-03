from __future__ import annotations

from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    """Find the repository root by walking up from `start`/cwd.

    We consider the root to be the first directory that contains:
    - `src/`
    - `config/`
    """
    here = (start or Path.cwd()).resolve()
    for p in (here, *here.parents):
        if (p / "src").is_dir() and (p / "config").is_dir():
            return p
    return here


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path

