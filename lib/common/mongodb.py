# python
from __future__ import annotations
from typing import Optional, Dict, Any, Set
from dataclasses import dataclass
import hashlib

from pymongo import MongoClient, ASCENDING
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, WriteError

from lib.common.config_handler import load_config
from lib.common.object_model import ObjectModel  # optional typing
from lib.common.logging import get_logger

logger = get_logger(__name__)


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


# --- New helpers for crawler extraction / storage ------------------------------------------------


def get_collection_for_domain(mongo_client: Optional[MongoClient], database_name: Optional[str], domain_cfg: Dict[str, Any]) -> Optional[Collection]:
    """
    Resolve and return a Collection for the given domain configuration.
    Uses get_domain_collection_name() and validates presence of a base/url key.
    Returns None and logs on failure.
    """
    if mongo_client is None or not database_name:
        logger.error("get_collection_for_domain: missing mongo_client or database_name")
        return None

    coll_name = get_domain_collection_name(domain_cfg)
    base_url = domain_cfg.get("base_url") or domain_cfg.get("url")
    if not coll_name or not base_url:
        logger.error("get_collection_for_domain: invalid domain config %s", domain_cfg)
        return None

    db = _get_database_from_client(mongo_client, database_name)
    if db is None:
        return None

    try:
        coll = db[coll_name]
        logger.debug("Resolved collection %s for domain %s", coll_name, domain_cfg.get("name"))
        return coll
    except Exception:
        logger.exception("Failed to get collection %s from DB %s", coll_name, database_name)
        return None


def refresh_known_hashes_for_collection(collection: Collection, known_hashes: Optional[Set[str]] = None) -> Set[str]:
    """
    Update (and return) the provided known_hashes set with hashes collected from the specific collection.
    If known_hashes is None, a new set is created and returned.
    This is a thin wrapper around collect_content_hashes_from_collection with logging.
    """
    if collection is None:
        logger.debug("refresh_known_hashes_for_collection: no collection provided")
        return known_hashes or set()

    try:
        coll_hashes = collect_content_hashes_from_collection(collection)
        if known_hashes is None:
            known_hashes = set(coll_hashes)
        else:
            known_hashes.update(coll_hashes)
        logger.debug("refresh_known_hashes_for_collection: collection=%s added %d hashes (total=%d)", collection.name, len(coll_hashes), len(known_hashes))
        return known_hashes
    except Exception:
        logger.exception("Failed to refresh known hashes from collection %s", getattr(collection, "name", "<unknown>"))
        return known_hashes or set()

def collect_known_hashes(mongo_client: Optional[MongoClient], database_name: Optional[str]) -> Set[str]:
    """
    Convenience wrapper: sammelt bekannte content/url-hashes aus der DB.
    Re-uses collect_content_hashes_from_db und gibt bei Fehlern ein leeres Set zurück.
    """
    try:
        hashes = collect_content_hashes_from_db(mongo_client, database_name) or set()
        logger.debug("collect_known_hashes: collected %d hashes", len(hashes))
        return hashes
    except Exception:
        logger.exception("collect_known_hashes: failed, returning empty set")
        return set()


def upsert_article(collection: Collection, obj: Any, input_url: str) -> bool:
    """
    Upsert the article represented by `obj` into `collection`.
    - Skips documents without extracted html/text (avoids placeholders).
    - Prefers `content_hash` as query key, falls back auf `url`/`id`.
    - Handles DuplicateKeyError by retrying with the URL-based query.
    Returns True on successful store/update, False if skipped or failed.
    """
    if collection is None:
        logger.error("no collection provided for url=%s", input_url)
        return False

    # prepare doc
    if hasattr(obj, "to_dict"):
        doc = obj.to_dict()
    else:
        doc = getattr(obj, "__dict__", {}).copy() if hasattr(obj, "__dict__") else dict(obj or {})
    doc.pop("_id", None)

    # skip empty extracts
    html_val = (getattr(obj, "html", "") or "").strip()
    text_val = (getattr(obj, "text", "") or "").strip()
    if not html_val and not text_val:
        return False

    # determine query: prefer content_hash
    content_hash = getattr(obj, "content_hash", None)
    if isinstance(content_hash, str) and content_hash:
        query = {"content_hash": content_hash}
    else:
        fallback_url = getattr(obj, "id", getattr(obj, "url", input_url))
        query = {"url": fallback_url}

    try:
        collection.update_one(query, {"$set": doc}, upsert=True)
        logger.info("stored/updated article; url=%s collection=%s", input_url, collection.name)
        return True
    except DuplicateKeyError:
        # retry with an explicit URL key
        try:
            fallback_url = getattr(obj, "id", getattr(obj, "url", input_url))
            fallback_query = {"url": fallback_url}
            collection.update_one(fallback_query, {"$set": doc}, upsert=True)
            logger.info("stored/updated article (fallback); url=%s collection=%s", input_url, collection.name)
            return True
        except Exception:
            logger.exception("fallback update failed for url=%s", input_url)
            return False
    except WriteError:
        logger.exception("write error storing article url=%s", input_url)
        return False
    except Exception:
        logger.exception("unexpected error storing article url=%s", input_url)
        return False


def close_mongo_client(mongo_client: Optional[MongoClient]) -> None:
    """
    Close the given MongoClient if present. Logs exceptions but does not raise.
    """
    try:
        if mongo_client:
            mongo_client.close()
            logger.debug("MongoClient closed")
    except Exception:
        logger.exception("close_mongo_client: error closing mongo client")