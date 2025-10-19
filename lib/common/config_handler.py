from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import os
import logging
import yaml

logger = logging.getLogger(__name__)

_CACHED_CONFIG: Optional[Dict[str, Any]] = None
_CACHED_CONFIG_PATH: Optional[str] = None


@dataclass
class MongoConfig:
    uri: Optional[str]
    database_name: Optional[str]


def _default_config_path() -> str:
    """
    Return the default configuration path (project root / configs / config.yaml)
    calculated relative to this file.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(project_root, "configs", "config.yaml")


def load_config(path: Optional[str] = None, force_reload: bool = False) -> Dict[str, Any]:
    """
    Load YAML configuration and return the raw mapping parsed by PyYAML.

    Caches the last loaded file (by path) unless `force_reload=True` is given.
    Returns an empty dict on error or when top-level object is not a mapping.
    """
    global _CACHED_CONFIG, _CACHED_CONFIG_PATH

    cfg_path = path or _default_config_path()

    # Return cached when available for the same path
    if not force_reload and _CACHED_CONFIG is not None and _CACHED_CONFIG_PATH == cfg_path:
        return _CACHED_CONFIG

    if not os.path.exists(cfg_path):
        logger.error("Configuration file not found at %s", cfg_path)
        _CACHED_CONFIG = {}
        _CACHED_CONFIG_PATH = cfg_path
        return {}

    try:
        with open(cfg_path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except Exception:
        logger.exception("Failed to read/parse configuration file %s", cfg_path)
        _CACHED_CONFIG = {}
        _CACHED_CONFIG_PATH = cfg_path
        return {}

    if raw is None:
        raw = {}

    if not isinstance(raw, dict):
        logger.error("Top-level configuration is not a mapping/dict in %s", cfg_path)
        _CACHED_CONFIG = {}
        _CACHED_CONFIG_PATH = cfg_path
        return {}

    logger.info("Configuration loaded from %s", cfg_path)
    _CACHED_CONFIG = raw
    _CACHED_CONFIG_PATH = cfg_path
    return raw


def load_mongodb_config(path: Optional[str] = None) -> MongoConfig:
    """
    Read the 'mongodb' section from the raw configuration and return a MongoConfig.

    Minimal processing: looks up common keys and falls back to environment variables.
    Does not raise; returns None fields when not found.
    """
    cfg = load_config(path)
    mdb = cfg.get("mongodb") if isinstance(cfg, dict) else None

    if not isinstance(mdb, dict):
        logger.warning("No valid 'mongodb' mapping found in configuration")
        return MongoConfig(uri=None, database_name=None)

    uri = mdb.get("uri") or mdb.get("connection_string") or os.environ.get("MONGODB_URI")
    dbname = (
        mdb.get("database_name")
        or mdb.get("database")
        or mdb.get("db")
        or os.environ.get("MONGODB_DATABASE")
    )

    if not uri:
        logger.debug("MongoDB URI not present in config or environment")
    if not dbname:
        logger.debug("MongoDB database name not present in config or environment")

    return MongoConfig(uri=uri, database_name=dbname)
