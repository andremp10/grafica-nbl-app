from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(*, log_file: Path | None = None, verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("etl")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    # Avoid duplicate logs when imported by other tools
    logger.propagate = False
    return logger

