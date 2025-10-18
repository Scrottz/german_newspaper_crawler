# python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set
import os
import glob
import json

from lib.common.config_handler import load_config, get_config_path
from lib.common.logging import get_logger

logger = get_logger(__name__)

# optional import of pymongo
try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except Exception:  # pragma: no cover - pymongo may be missing in some environments
    MongoClient = None  # type: ignore
    PyMongoError = Exception  # type: ignore


@dataclass
class MongoDBConfig:
    """Holds relevant MongoDB settings extracted from the application config."""
    path: Optional[str] = None
    database_name: Optional[str] = None


def _find_database_name(mongodb_cfg: Dict[str, Any]) -> Optional[str]:
    if not isinstance(mongodb_cfg, dict):
        return None
    candidates = [
        "database_name",
        "databese_name",  # handle common typo in config
        "db_name",
        "dbname",
        "database",
    ]
    for key in candidates:
        val = mongodb_cfg.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def load_mongodb_config(config_path: Optional[str] = None) -> MongoDBConfig:
    cfg = load_config(config_path)
    mongodb_cfg = cfg.get("mongodb", {}) if isinstance(cfg, dict) else {}

    path = mongodb_cfg.get("path")
    if path is not None and not isinstance(path, str):
        logger.warning("mongodb.path has unexpected type: %r", type(path))
        path = None

    db_name = _find_database_name(mongodb_cfg)
    if db_name is None:
        logger.warning("MongoDB database name not found in config (checked common keys).")

    config = MongoDBConfig(path=path, database_name=db_name)
    logger.debug("Loaded MongoDBConfig: path=%s database_name=%s", config.path, config.database_name)
    return config


def get_domain_collection_name(domain_cfg: Dict[str, Any]) -> Optional[str]:
    if not isinstance(domain_cfg, dict):
        return None
    col = domain_cfg.get("mongodb_collection_name")
    if isinstance(col, str) and col.strip():
        return col.strip()
    return None


def _create_mongo_client_from_env_or_default() -> Optional["MongoClient"]:
    """
    Try to create a MongoClient using MONGODB_URI env var or a default client.
    Returns None if pymongo is not available or creation fails.
    """
    if MongoClient is None:
        logger.warning("pymongo not available; cannot connect to MongoDB.")
        return None
    uri = os.environ.get("MONGODB_URI")
    try:
        if uri:
            logger.debug("Creating MongoClient from MONGODB_URI")
            return MongoClient(uri)
        logger.debug("Creating default MongoClient (localhost:27017)")
        return MongoClient()
    except Exception:
        logger.exception("Failed to create MongoClient")
        return None


def collect_content_hashes_from_db(client: "MongoClient", db_name: str) -> Set[str]:
    """
    Iterate all collections in the given database and collect all non-null
    string values of the `content_hash` field.
    """
    hashes: Set[str] = set()
    if client is None:
        logger.debug("No MongoClient provided, skipping DB hash collection.")
        return hashes
    if not db_name:
        logger.debug("No database name provided, skipping DB hash collection.")
        return hashes

    try:
        db = client[db_name]
        coll_names = db.list_collection_names()
        logger.info("Scanning %d collections in DB %s for content_hash", len(coll_names), db_name)
        for coll in coll_names:
            try:
                cursor = db[coll].find({"content_hash": {"$exists": True, "$ne": None}}, {"content_hash": 1})
                for doc in cursor:
                    ch = doc.get("content_hash")
                    if isinstance(ch, str) and ch:
                        hashes.add(ch)
            except PyMongoError:
                logger.exception("Error scanning collection %s", coll)
    except PyMongoError:
        logger.exception("Error accessing database %s", db_name)

    logger.debug("Collected %d unique hashes from DB %s", len(hashes), db_name)
    return hashes


def collect_content_hashes_from_dir(path: str) -> Set[str]:
    """
    Recursively scan directory for .json and .ndjson files and collect
    `content_hash` values. Supports JSON arrays, single JSON objects and NDJSON.
    """
    hashes: Set[str] = set()
    if not path:
        return hashes
    if not os.path.exists(path):
        logger.debug("Data directory does not exist: %s", path)
        return hashes

    logger.info("Scanning files under %s for content_hash", path)
    # patterns for JSON/NDJSON files
    patterns = [os.path.join(path, "**", "*.json"), os.path.join(path, "**", "*.ndjson")]
    files = []
    for p in patterns:
        files.extend(glob.glob(p, recursive=True))

    for fname in files:
        try:
            if fname.endswith(".ndjson"):
                with open(fname, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            doc = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        ch = _extract_hash_from_doc(doc)
                        if ch:
                            hashes.add(ch)
            else:  # .json
                with open(fname, "r", encoding="utf-8") as fh:
                    try:
                        data = json.load(fh)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(data, list):
                        for doc in data:
                            ch = _extract_hash_from_doc(doc)
                            if ch:
                                hashes.add(ch)
                    elif isinstance(data, dict):
                        ch = _extract_hash_from_doc(data)
                        if ch:
                            hashes.add(ch)
        except Exception:
            logger.exception("Failed to scan file %s for content_hash", fname)

    logger.debug("Collected %d unique hashes from files under %s", len(hashes), path)
    return hashes


def _extract_hash_from_doc(doc: Any) -> Optional[str]:
    """
    Normalize extraction of content hash from a document object.
    Checks common keys.
    """
    if not isinstance(doc, dict):
        return None
    for key in ("content_hash", "hash", "contentHash"):
        val = doc.get(key)
        if isinstance(val, str) and val:
            return val
    return None


def collect_all_content_hashes(config_path: Optional[str] = None, data_dir: Optional[str] = None) -> Set[str]:
    """
    High level helper: collect hashes from the configured MongoDB (if reachable)
    and from a local data directory (if provided or discovered as ./data).
    """
    hashes: Set[str] = set()
    cfg = load_mongodb_config(config_path)
    client = _create_mongo_client_from_env_or_default()
    if client and cfg.database_name:
        hashes |= collect_content_hashes_from_db(client, cfg.database_name)
    else:
        if client is None:
            logger.debug("Skipping DB hash collection because client could not be created.")
        else:
            logger.debug("Skipping DB hash collection because no database_name configured.")

    # determine data directory if not explicitly provided
    if not data_dir:
        # derive project root from config file location
        try:
            project_root = os.path.abspath(os.path.join(os.path.dirname(get_config_path()), os.pardir, os.pardir))
            data_dir = os.path.join(project_root, "data")
        except Exception:
            data_dir = None

    if data_dir:
        hashes |= collect_content_hashes_from_dir(data_dir)

    logger.info("Total unique content_hash values collected: %d", len(hashes))
    return hashes


def main() -> int:
    """CLI helper: load config and print relevant MongoDB values and hash summary."""
    cfg = load_mongodb_config()
    print("MongoDB path:", cfg.path)
    print("MongoDB database_name:", cfg.database_name)

    hashes = collect_all_content_hashes()
    print("Collected content_hash count:", len(hashes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
