# python
from __future__ import annotations
import logging
import logging.handlers
import os
import sys
from typing import Optional, Dict, Any

from lib.common.config_handler import load_config

_FMT = "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"
_FORMATTER = logging.Formatter(_FMT)

_configured = False


def _parse_level(level: Optional[str]) -> Optional[int]:
    if not level:
        return logging.INFO
    lvl = str(level).strip().upper()
    if lvl in ("SILENT", "SILTENT"):
        return None
    try:
        return getattr(logging, lvl)
    except Exception:
        return logging.INFO


def _ensure_logdir(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        os.makedirs(path, exist_ok=True)
        return path
    except Exception:
        return None


def _remove_stream_handlers(root: logging.Logger) -> None:
    for h in list(root.handlers):
        if isinstance(h, logging.StreamHandler):
            try:
                root.removeHandler(h)
            except Exception:
                pass


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Zentraler Logger-Fabrik. Liest `logging.level` und `logging.logdir` aus der Config.
    Wenn level == 'SILENT' (oder 'SILTENT') wird kein StreamHandler angelegt.
    """
    global _configured
    cfg = load_config() or {}
    logging_cfg: Dict[str, Any] = cfg.get("logging", {}) if isinstance(cfg, dict) else {}

    raw_level = logging_cfg.get("level")
    console_level = _parse_level(raw_level)  # None => SILENT (keine Console)
    file_level_raw = logging_cfg.get("file_level", "DEBUG")
    try:
        file_level = _parse_level(file_level_raw) or logging.DEBUG
    except Exception:
        file_level = logging.DEBUG

    # determine logdir: prefer config.logdir, fallback to ./logs
    cfg_logdir = logging_cfg.get("logdir")
    default_logdir = os.path.join(os.getcwd(), "logs")
    logdir = cfg_logdir or default_logdir
    logdir_real = _ensure_logdir(logdir) or default_logdir

    root = logging.getLogger()
    if not _configured:
        # remove all handlers to avoid duplicates; keep only file handlers if SILENT later
        root.setLevel(logging.DEBUG)  # allow handlers to filter
        # add rotating file handler (always)
        try:
            logfile = os.path.join(logdir_real, "german_newspapaer_crawler.log")
            fh = logging.handlers.RotatingFileHandler(
                logfile, maxBytes=10_485_760, backupCount=5, encoding="utf-8"
            )
            fh.setFormatter(_FORMATTER)
            fh.setLevel(file_level)
            root.addHandler(fh)
        except Exception as e:
            print(f"Failed to create file log handler at {logdir_real}: {e}", file=sys.stderr)

        # add console handler only if not SILENT
        if console_level is not None:
            ch = logging.StreamHandler(sys.stderr)
            ch.setFormatter(_FORMATTER)
            ch.setLevel(console_level)
            root.addHandler(ch)

        _configured = True
    else:
        # Already configured: ensure console handlers match current config.
        if console_level is None:
            _remove_stream_handlers(root)
        else:
            # ensure at least one StreamHandler with correct level/formatter exists
            has_console = any(isinstance(h, logging.StreamHandler) for h in root.handlers)
            if not has_console:
                ch = logging.StreamHandler(sys.stderr)
                ch.setFormatter(_FORMATTER)
                ch.setLevel(console_level)
                root.addHandler(ch)
            else:
                for h in root.handlers:
                    if isinstance(h, logging.StreamHandler):
                        try:
                            h.setFormatter(_FORMATTER)
                            h.setLevel(console_level)
                        except Exception:
                            pass

    return logging.getLogger(name)
