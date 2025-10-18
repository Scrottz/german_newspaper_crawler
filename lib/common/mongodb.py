# python
from __future__ import annotations
from typing import Optional, Dict, Any, Set
from dataclasses import dataclass
import hashlib
import logging

from pymongo import MongoClient, ASCENDING
from pymongo.database import Database
from pymongo.collection import Collection

from lib.common.config_handler import load_config

logger = logging.getLogger(__name__)


@dataclass
class MongoConfig:
    host: str = "localhost"
    port: int = 27017
    username: Optional[str] = None
    password: Optional[str] = None
    database_name: Optional[str] = None
    options: Dict[str, Any] = None


def _sha256_hex(s: str) -> str:
    h = hashlib.sha256()
    h.update(s.encode("utf-8"))
    return h.hexdigest()


def load_mongodb_config() -> MongoConfig:
    """
    Load mongodb config from the project's config (via lib.common.config_handler.load_config()).
    Falls nichts konfiguriert ist, werden sinnvolle Defaults zurückgegeben.
    """
    try:
        cfg = load_config() or {}
    except Exception:
        logger.exception("Failed to load global config, using defaults for MongoDB")
        cfg = {}

    mcfg = cfg.get("mongodb") or cfg.get("mongo") or {}
    host = mcfg.get("host", "localhost")
    port = int(mcfg.get("port", 27017))
    username = mcfg.get("username") or mcfg.get("user")
    password = mcfg.get("password")
    database_name = mcfg.get("database_name") or mcfg.get("database") or mcfg.get("db")
    options = mcfg.get("options") or {}

    return MongoConfig(host=host, port=port, username=username, password=password, database_name=database_name, options=options)


def get_domain_collection_name(domain_cfg: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Determine the collection name for a domain configuration.
    Accepts domain_cfg as dict and returns the configured collection name or None.
    Common keys tried: 'collection_name', 'collection', 'name', fallback to None.
    """
    if not domain_cfg or not isinstance(domain_cfg, dict):
        return None
    return domain_cfg.get("collection_name") or domain_cfg.get("collection") or domain_cfg.get("name")


def _get_database_from_client(client: MongoClient, db_name: Optional[str]) -> Optional[Database]:
    if client is None or not db_name:
        return None
    try:
        return client[db_name]
    except Exception:
        logger.exception("Failed to get database %s from client", db_name)
        return None


def collect_content_hashes_from_db(mongo_client: Optional[MongoClient], database_name: Optional[str]) -> Set[str]:
    """
    Collect known content_hash values \- but only if the DB document actually contains extracted content
    (non-empty 'content_hash') or has non-empty 'text' or 'html'. Avoid treating placeholder/URL-only
    documents as 'known' to prevent skipping the real parsing later.
    """
    hashes: Set[str] = set()
    if mongo_client is None or not database_name:
        logger.debug("No mongo client or database_name provided to collect_content_hashes_from_db")
        return hashes

    db = _get_database_from_client(mongo_client, database_name)
    if db is None:
        return hashes

    try:
        coll_names = db.list_collection_names()
    except Exception:
        logger.exception("Failed to list collections for DB %s", database_name)
        return hashes

    for coll_name in coll_names:
        try:
            coll = db[coll_name]
            # request minimal fields but include text/html to decide whether the doc holds real content
            cursor = coll.find({}, {"content_hash": 1, "url": 1, "id": 1, "text": 1, "html": 1}, no_cursor_timeout=True)
            for doc in cursor:
                if not doc:
                    continue

                # If an explicit content_hash is present and non-empty, accept it.
                ch = doc.get("content_hash")
                if isinstance(ch, str) and ch:
                    hashes.add(ch)
                    continue

                # If the document carries extracted content (text or html non-empty), compute hash from url (if present)
                text_val = doc.get("text")
                html_val = doc.get("html")
                if (isinstance(text_val, str) and text_val.strip()) or (isinstance(html_val, str) and html_val.strip()):
                    # derive hash from url / legacy id if available
                    url_val = doc.get("url") or doc.get("id")
                    if isinstance(url_val, str) and url_val.startswith(("http://", "https://")):
                        try:
                            hashes.add(_sha256_hex(url_val))
                        except Exception:
                            logger.debug("Failed to compute sha256 for url/id field in %s:%s", coll_name, doc.get("_id"))
                    continue

                # otherwise skip: avoid adding hashes for placeholder/empty docs
            try:
                cursor.close()
            except Exception:
                pass
        except Exception:
            logger.exception("Error scanning collection %s for content hashes", coll_name)

    logger.info("Collected %d known content/url-hashes from DB %s (content-aware)", len(hashes), database_name)
    return hashes


def ensure_indexes_for_collections(mongo_client: Optional[MongoClient], database_name: Optional[str]) -> None:
    """
    Optional helper that creates useful indexes:
    - index on 'content_hash' (non-unique) for fast lookup
    - index on 'url' (unique=False) for quick URL-based lookup
    Call this during setup/migration if desired.
    """
    if mongo_client is None or not database_name:
        logger.debug("No mongo client or database_name provided to ensure_indexes_for_collections")
        return

    db = _get_database_from_client(mongo_client, database_name)
    if db is None:
        return

    try:
        for coll_name in db.list_collection_names():
            try:
                coll = db[coll_name]
                # create indexes if they don't exist; non-blocking
                coll.create_index([("content_hash", ASCENDING)], background=True, name="idx_content_hash")
                coll.create_index([("url", ASCENDING)], background=True, name="idx_url")
            except Exception:
                logger.exception("Failed to create indexes on collection %s", coll_name)
    except Exception:
        logger.exception("Failed to ensure indexes for DB %s", database_name)


def collect_content_hashes_from_collection(collection: Collection) -> Set[str]:
    """
    Sammle alle content_hash-Werte aus der angegebenen Collection.
    Nutzt eine explizite Session und no_cursor_timeout=True, um die PyMongo-Warnung zu vermeiden.
    Diese Funktion ist eine helper-Variante für den Fall, dass bereits eine `Collection`-Instanz vorliegt.
    """
    known_hashes = set()
    client = collection.database.client

    # explizite Session verwenden (empfohlen von PyMongo wenn no_cursor_timeout=True)
    with client.start_session() as session:
        cursor = collection.find({}, {"content_hash": 1}, no_cursor_timeout=True, session=session)
        try:
            for doc in cursor:
                if not doc:
                    continue
                ch = doc.get("content_hash")
                if ch:
                    known_hashes.add(ch)
        finally:
            try:
                cursor.close()
            except Exception:
                logger.debug("Failed to close cursor cleanly", exc_info=True)

    logger.info("Collected %d known content/url-hashes from DB %s", len(known_hashes), collection.name)
    return known_hashes
