# python
from __future__ import annotations
import logging
import os
import sys
from typing import Optional, Dict

_LEVELS: Dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "SILENT": 100,
}
logging.addLevelName(_LEVELS["SILENT"], "SILENT")


def setup_logging(level: str = "INFO") -> None:
    """
    Robustes Logging-Setup:
    - erzeugt Datei-Handler (DEBUG) falls möglich
    - erzeugt Console-Handler (StreamHandler -> stderr) mit dem gewünschten Level
    - entfernt nur bestehende StreamHandler, um Duplikate zu vermeiden
    - sorgt dafür, dass bei Fehlern die Konsole trotzdem Logging erhält
    """
    lvl_name = (level or "INFO").upper()
    numeric_level = _LEVELS.get(lvl_name, logging.INFO)

    root = logging.getLogger()
    # Deaktiviere globale Deaktivierung und setze vorab Root-Level so, dass Handler alles bekommen kann
    logging.disable(logging.NOTSET)
    root.setLevel(logging.DEBUG)

    # Entferne nur vorhandene StreamHandler (vermeidet Entfernen von z.B. file handlers hinzugefügt vom System)
    for h in list(root.handlers):
        if isinstance(h, logging.StreamHandler):
            root.removeHandler(h)

    # Sicherstellen, dass logs-Verzeichnis existiert
    logs_dir = os.path.join(os.getcwd(), "logs")
    try:
        os.makedirs(logs_dir, exist_ok=True)
    except Exception:
        # Falls Verzeichnis nicht erstellt werden kann, Ausgabe auf stderr, Console-Handler später sorgt für Logs
        print(f"Could not create logs directory {logs_dir}", file=sys.stderr)

    log_file_path = os.path.join(logs_dir, "german_newspapaer_crawler.log")
    fmt = "%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s(): %(message)s"
    formatter = logging.Formatter(fmt)

    # Datei-Handler versuchen (DEBUG)
    try:
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        root.addHandler(file_handler)
    except Exception as e:
        # sichtbar machen, damit man weiss warum die Datei-Logs fehlen
        print(f"Failed to create file log handler at {log_file_path}: {e}", file=sys.stderr)

    # SILENT: keine Console-Ausgabe, Datei-Handler bleibt falls verfügbar
    if lvl_name == "SILENT":
        return

    # Console-Handler immer hinzufügen (explizit stderr)
    console = logging.StreamHandler(stream=sys.stderr)
    console.setFormatter(formatter)
    console.setLevel(numeric_level)
    root.addHandler(console)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    return logging.getLogger(name)
