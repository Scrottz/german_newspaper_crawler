from __future__ import annotations
import os
from typing import Any, Dict, Optional, Set, List
from dataclasses import dataclass

import yaml


def get_config_path() -> str:
    """Return absolute path to `configs/config.yaml` relative to project root."""
    package_dir = os.path.dirname(__file__)  # e.g. .../lib/common
    project_root = os.path.abspath(os.path.join(package_dir, os.pardir, os.pardir))
    return os.path.join(project_root, "configs", "config.yaml")


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load YAML config from the given path (or from the default config path).
    Returns a dict; on error returns an empty dict.
    """
    cfg_path = path or get_config_path()
    try:
        with open(cfg_path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


@dataclass
class MongoDBConfig:
    """Lightweight container for MongoDB connection settings from config."""
    uri: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    auth_source: Optional[str] = None
    replica_set: Optional[str] = None
    database_name: Optional[str] = None


def load_mongodb_config(path: Optional[str] = None) -> MongoDBConfig:
    """
    Load mongodb configuration from the YAML config.
    Looks under top-level key 'mongodb' and returns a MongoDBConfig instance.
    """
    cfg = load_config(path)
    mcfg = cfg.get("mongodb", {}) if isinstance(cfg, dict) else {}
    # support both nested and flat representations
    uri = mcfg.get("uri") or mcfg.get("url")
    host = mcfg.get("host")
    port = None
    if mcfg.get("port") is not None:
        try:
            port = int(mcfg.get("port"))
        except Exception:
            port = None
    username = mcfg.get("username") or mcfg.get("user")
    password = mcfg.get("password") or mcfg.get("pass")
    auth_source = mcfg.get("auth_source") or mcfg.get("authSource")
    replica_set = mcfg.get("replica_set") or mcfg.get("replicaSet")
    database_name = mcfg.get("database") or mcfg.get("database_name") or mcfg.get("db")
    return MongoDBConfig(
        uri=uri,
        host=host,
        port=port,
        username=username,
        password=password,
        auth_source=auth_source,
        replica_set=replica_set,
        database_name=database_name,
    )


def get_domain_collection_name(domain_cfg: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Return a collection name for a domain configuration.
    Accepts common keys: 'collection', 'collection_name', 'collectionName'.
    """
    if not domain_cfg or not isinstance(domain_cfg, dict):
        return None
    for key in ("collection", "collection_name", "collectionName", "collection-name"):
        val = domain_cfg.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def collect_content_hashes_from_db(mongo_client, database_name: Optional[str]) -> Set[str]:
    """
    Collect all distinct 'content_hash' values from all collections in the given database.
    Returns a set of hashes. If anything fails, returns an empty set.
    """
    hashes: Set[str] = set()
    if not mongo_client or not database_name:
        return hashes
    try:
        db = mongo_client[database_name]
        # iterate collections and collect content_hash values
        coll_names: List[str] = db.list_collection_names()
        for coll_name in coll_names:
            try:
                coll = db[coll_name]
                cursor = coll.find({"content_hash": {"$exists": True}}, {"content_hash": 1})
                for doc in cursor:
                    ch = doc.get("content_hash")
                    if ch:
                        hashes.add(ch)
            except Exception:
                # skip problematic collections but continue
                continue
    except Exception:
        return set()
    return hashes


def main() -> int:
    """Small CLI: load config and print top-level keys when executed directly."""
    cfg = load_config()
    print("Loaded config keys:", list(cfg.keys()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
