# python
# File: `lib/common/mongodb.py`
from __future__ import annotations
from typing import Any, Dict, Iterable, Optional, Set, List, Tuple
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.results import UpdateResult
import logging

from lib.common.logging import get_logger
from lib.common.object_model import ObjectModel

logger = get_logger(__name__)


def get_collection_for_domain(*args, **kwargs):
    """
    Flexible helper returning a Collection for a domain.
    Accepts either:
      - (domain_cfg: dict)
      - (mongo_client, db_name, domain_cfg)
    Returns the Collection object.
    """
    # signature variants
    if len(args) == 1 and isinstance(args[0], dict):
        domain_cfg = args[0]
        raise TypeError("get_collection_for_domain requires (mongo_client, db_name, domain_cfg) in this helper context")
    if len(args) >= 3:
        mongo_client = args[0]
        db_name = args[1]
        domain_cfg = args[2]
    else:
        # fallback to kwargs
        mongo_client = kwargs.get("mongo_client")
        db_name = kwargs.get("db_name")
        domain_cfg = kwargs.get("domain_cfg")
        if mongo_client is None or db_name is None or domain_cfg is None:
            raise TypeError("get_collection_for_domain requires (mongo_client, db_name, domain_cfg)")

    col_name = domain_cfg.get("collection") or domain_cfg.get("name")
    if not col_name:
        raise ValueError("Domain config has no 'collection' or 'name' to derive collection")
    col: Collection = mongo_client[db_name][col_name]
    return col


def collect_known_hashes(mongo_client: MongoClient, db_name: str) -> Set[str]:
    """
    Collect known hashes from all collections in the database.
    Returns a set of string hashes (excluding None).
    """
    known: Set[str] = set()
    try:
        db = mongo_client[db_name]
        for cname in db.list_collection_names():
            try:
                col = db[cname]
                for h in col.distinct("content_hash"):
                    if isinstance(h, str) and h:
                        known.add(h)
            except Exception:
                logger.exception("collect_known_hashes: failed collecting from collection %s", cname)
    except Exception:
        logger.exception("collect_known_hashes: failed for db %s", db_name)
    return known


def refresh_known_hashes_for_collection(mongo_client: MongoClient, db_name: str, collection_name: str) -> Set[str]:
    """
    Collect known hashes for a single collection.
    """
    known: Set[str] = set()
    try:
        col = mongo_client[db_name][collection_name]
        for h in col.distinct("content_hash"):
            if isinstance(h, str) and h:
                known.add(h)
    except Exception:
        logger.exception("refresh_known_hashes_for_collection: failed for %s.%s", db_name, collection_name)
    return known


def upsert_article(mongo_client: MongoClient, db_name: str, collection_name: str, obj: Any) -> Optional[UpdateResult]:
    """
    Upsert the article into the given collection.

    - Accepts an ObjectModel instance, a dict or anything with to_dict().
    - Uses 'content_hash' as primary key when present, otherwise 'url'.
    - Stores the full serialized document produced by ObjectModel.to_dict().
    """
    try:
        col: Collection = mongo_client[db_name][collection_name]
    except Exception:
        logger.exception("upsert_article: cannot get collection %s.%s", db_name, collection_name)
        return None

    # obtain serializable dict
    doc: Dict[str, Any]
    if isinstance(obj, ObjectModel):
        try:
            doc = obj.to_dict()
        except Exception:
            logger.exception("upsert_article: ObjectModel.to_dict() failed, falling back to attribute extraction")
            doc = {
                "url": getattr(obj, "url", None),
                "html": getattr(obj, "html", None),
                "text": getattr(obj, "text", None),
                "titel": getattr(obj, "titel", None),
                "teaser": getattr(obj, "teaser", None),
                "autor": getattr(obj, "autor", None),
                "category": getattr(obj, "category", None),
                "published_date": getattr(obj, "published_date", None),
                "parsed_date": getattr(obj, "parsed_date", None),
                "content_hash": getattr(obj, "content_hash", None),
                "ai_keywords": getattr(obj, "ai_keywords", []),
                "pos_taggs": getattr(obj, "pos_taggs", []),
            }
    elif isinstance(obj, dict):
        doc = dict(obj)
    else:
        # try to call to_dict()
        if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
            doc = obj.to_dict()
        else:
            logger.warning("upsert_article: received unsupported obj type %s; storing minimal fields", type(obj))
            doc = {"url": getattr(obj, "url", None), "html": getattr(obj, "html", None)}

    # remove internal _id from doc to let Mongo assign / avoid conflicts on upsert
    if "_id" in doc:
        try:
            doc.pop("_id", None)
        except Exception:
            pass

    # choose upsert key
    key: Dict[str, Any] = {}
    if isinstance(doc.get("content_hash"), str) and doc.get("content_hash"):
        key = {"content_hash": doc.get("content_hash")}
    elif isinstance(doc.get("url"), str) and doc.get("url"):
        key = {"url": doc.get("url")}
    else:
        # fallback: insert as new document with generated key
        try:
            res = col.insert_one(doc)
            logger.info("upsert_article: inserted new doc _id=%s into %s.%s", getattr(res, "inserted_id", None), db_name, collection_name)
            return None
        except Exception:
            logger.exception("upsert_article: failed to insert doc into %s.%s", db_name, collection_name)
            return None

    try:
        res = col.update_one(key, {"$set": doc}, upsert=True)
        logger.debug("upsert_article: update_one key=%s matched=%s modified=%s upserted_id=%s", key, getattr(res, "matched_count", None), getattr(res, "modified_count", None), getattr(res, "upserted_id", None))
        return res
    except Exception:
        logger.exception("upsert_article: update_one failed for key=%s in %s.%s", key, db_name, collection_name)
        return None


def ensure_indexes_for_collections(mongo_client: MongoClient, db_name: str, collections: Optional[List[str]] = None) -> None:
    """
    Ensure common indexes (content_hash, url) on given collections.
    If collections is None or empty, ensure on all collections in the DB.
    """
    try:
        db = mongo_client[db_name]
        target_cols = collections or db.list_collection_names()
        for cname in target_cols:
            try:
                col = db[cname]
                # content_hash index (non-unique to be safe), url index unique (sparse)
                col.create_index([("content_hash", ASCENDING)], name="idx_content_hash", background=True)
                col.create_index([("url", ASCENDING)], name="idx_url", unique=False, background=True)
            except Exception:
                logger.exception("ensure_indexes_for_collections: failed for %s.%s", db_name, cname)
    except Exception:
        logger.exception("ensure_indexes_for_collections: failed for db %s", db_name)


def close_mongo_client(mongo_client: Optional[MongoClient]) -> None:
    try:
        if mongo_client is not None:
            mongo_client.close()
            logger.info("Closed MongoClient")
    except Exception:
        logger.exception("close_mongo_client: failed")
