"""Logging setup for the daily stock pipeline."""

from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(log_dir: Path, log_file: str) -> logging.Logger:
    """Configure file and console logging once and return the app logger."""

    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("stock_pipeline")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_dir / log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
