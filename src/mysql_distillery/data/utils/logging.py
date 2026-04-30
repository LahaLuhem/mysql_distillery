"""Per-component logger — writes to both ``logs/<component>.log`` and stdout."""
from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logger(component: str, out_dir: Path) -> logging.Logger:
    """Per-component logger: writes to logs/<component>.log AND stdout.

    The logger name encodes both the out_dir (i.e. the target DB subdir) and
    the component so that two runs targeting different DBs produce independent
    loggers with independent handlers. Without this, the second call would
    hit the handler cache and keep writing to the first run's log file.
    """
    log_dir = out_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(f"mysql_distillery.{out_dir.name}.{component}")
    logger.setLevel(logging.INFO)
    # Avoid double-adding handlers if run() is called multiple times in a test.
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        f"%(asctime)s [{component}] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    fh = logging.FileHandler(log_dir / f"{component}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger
