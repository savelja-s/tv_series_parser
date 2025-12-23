import logging
from pathlib import Path


def setup_logger(serial_slug: str, base_dir: Path) -> logging.Logger:
    serial_dir = base_dir / serial_slug
    serial_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(serial_slug)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(serial_dir / "parser.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger
