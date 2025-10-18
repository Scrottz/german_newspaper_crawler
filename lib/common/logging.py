from __future__ import annotations
import logging
from typing import Optional, Dict

# Define common levels plus a SILENT level that suppresses all output
_LEVELS: Dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "SILENT": 100,  # higher than CRITICAL; used to disable all logging
}
logging.addLevelName(_LEVELS["SILENT"], "SILENT")


def setup_logging(level: str = "INFO") -> None:
    """
    Configure root logging for the application.
    If level == "SILENT", logging output is completely suppressed.
    """
    lvl_name = (level or "INFO").upper()
    root = logging.getLogger()

    # Always clear any existing handlers to avoid duplicate output
    root.handlers.clear()
    # Reset any previous global disable
    logging.disable(logging.NOTSET)

    if lvl_name == "SILENT":
        # Disable all logging at or below the SILENT threshold (i.e. everything)
        logging.disable(_LEVELS["SILENT"])
        return

    numeric_level = _LEVELS.get(lvl_name, logging.INFO)

    handler = logging.StreamHandler()
    fmt = "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s(): %(message)s"
    handler.setFormatter(logging.Formatter(fmt))

    root.addHandler(handler)
    root.setLevel(numeric_level)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Return a logger for the given name. Consumers should call this to obtain loggers.
    """
    return logging.getLogger(name)
