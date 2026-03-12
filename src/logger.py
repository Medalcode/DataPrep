"""
src/logger.py
-------------
Centralized logging utility for DataPrep Pipeline.
"""
import logging
import sys
from pathlib import Path
from datetime import datetime


def get_logger(name: str = "dataprep", log_dir: Path = None) -> logging.Logger:
    """
    Returns a configured logger that writes to both console and a log file.

    Args:
        name:    Logger name (shows up in log output).
        log_dir: Directory where log files are stored. If None, logs only to console.

    Returns:
        logging.Logger instance.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        # Avoid adding duplicate handlers on re-import
        return logger

    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console handler ───────────────────────────────────────────────────────
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # ── File handler ──────────────────────────────────────────────────────────
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
