# python
# File: `lib/common/mongodb.py`
from __future__ import annotations
from typing import Any, Optional, Set, List, Dict, Iterable, Tuple
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, PyMongoError
from pymongo.results import UpdateResult, InsertOneResult
from lib.common.logging import get_logger

logger = get_logger(__name__)


def collect_known_hashes(mongo_client: MongoClient, db_name: str) -> Set[str]:
    """
    Collect all known content_hash values from all collections in the database.
    Returns a set of hashes (may be empty).
    """
    hashes: Set[str] = set()
    try:
        db = mongo_client[db_name]
        for coll_name in db.list_collection_names():
            try:
                coll = db[coll_name]
                for doc in coll.find({"content_hash": {"$exists": True, "$ne": None}}, {"content_hash": 1}):
                    ch = doc.get("content_hash")
                    if isinstance(ch, str) and ch:
                        hashes.add(ch)
            except Exception:
                logger.exception("collect_known_hashes: failed to read from collection %s", coll_name)
    except Exception:
        logger.exception("collect_known_hashes: failed to list collections for db %s", db_name)
    return hashes


def get_collection_for_domain(mongo_client: MongoClient, db_name: str, domain_cfg: Dict[str, Any]) -> Tuple[Collection, str]:
    """
    Resolve a collection object and its name for the given domain configuration.
    Returns (collection, collection_name).
    """
    db = mongo_client[db_name]
    col_name = domain_cfg.get("collection") or domain_cfg.get("name")
    if not isinstance(col_name, str) or not col_name:
        raise ValueError("get_collection_for_domain: domain_cfg must provide 'collection' or 'name'")
    coll = db[col_name]
    return coll, col_name


def refresh_known_hashes_for_collection(mongo_client: MongoClient, db_name: str, collection_name: str) -> Set[str]:
    """
    Refresh the set of known content hashes for a single collection.
    """
    hashes: Set[str] = set()
    try:
        coll = mongo_client[db_name][collection_name]
        for doc in coll.find({"content_hash": {"$exists": True, "$ne": None}}, {"content_hash": 1}):
            ch = doc.get("content_hash")
            if isinstance(ch, str) and ch:
                hashes.add(ch)
    except Exception:
        logger.exception("refresh_known_hashes_for_collection: failed for %s.%s", db_name, collection_name)
    return hashes


def ensure_indexes_for_collections(mongo_client: MongoClient, db_name: str, collection_names: Iterable[str]) -> None:
    """
    Ensure common indexes exist for the provided collections.
    - Creates a sparse unique index on 'content_hash' to avoid duplicates for articles
      while allowing documents without a content_hash.
    - Creates a unique index on 'url' where applicable (sparse to allow legacy docs).
    """
    try:
        db = mongo_client[db_name]
        for name in collection_names or []:
            try:
                coll = db[name]
                # content_hash: unique but sparse (no index entries for missing/null hashes)
                coll.create_index([("content_hash", 1)], unique=True, sparse=True, name="ix_content_hash_unique")
                # url: unique and sparse (some legacy docs might not have url)
                coll.create_index([("url", 1)], unique=True, sparse=True, name="ix_url_unique")
            except Exception:
                logger.exception("ensure_indexes_for_collections: failed to ensure indexes for %s.%s", db_name, name)
    except Exception:
        logger.exception("ensure_indexes_for_collections: failed to acquire db %s", db_name)


def close_mongo_client(mongo_client: Optional[MongoClient]) -> None:
    """Close the provided MongoClient if available."""
    if mongo_client is None:
        return
    try:
        mongo_client.close()
        logger.info("Closed MongoClient")
    except Exception:
        logger.exception("Failed to close MongoClient")


def upsert_article(mongo_client: Any, db_name: str, collection_name: str, obj: Any) -> Optional[Any]:
    """
    Upsert an article into the specified collection.

    - Avoids directly setting `_id` in the `$set` document to prevent DuplicateKeyError
      when the client-provided `_id` collides with an existing document.
    - Prefers to query by `content_hash`, then `url`, then `_id` (only if nothing else).
    - If a client-side `_id` exists and we're not querying by `_id`, it will be stored
      as `internal_id` via `$setOnInsert`.
    - On DuplicateKeyError during update, attempt a fallback insert without using `_id`.
    """
    db = mongo_client[db_name]
    coll = db[collection_name]

    # Normalize input to dict
    try:
        data: Dict[str, Any] = obj.to_dict() if hasattr(obj, "to_dict") else dict(obj)
    except Exception:
        logger.exception("upsert_article: failed to convert object to dict for %s", collection_name)
        return None

    # Remove _id from set-doc to avoid inserting it during upsert
    internal_id = data.pop("_id", None)

    # Build query: prefer content_hash, then url, then _id (only if nothing else)
    query: Dict[str, Any] = {}
    if data.get("content_hash"):
        query = {"content_hash": data["content_hash"]}
    elif data.get("url"):
        query = {"url": data["url"]}
    elif internal_id is not None:
        query = {"_id": internal_id}
    else:
        # Worst-case: use a query that will not match common docs, forcing insert
        query = {"url": None}

    # Prepare $set document without None values
    set_doc = {k: v for k, v in data.items() if v is not None}

    update_doc: Dict[str, Any] = {"$set": set_doc}
    # If we have an internal client id and we're not querying by that _id,
    # store it in `internal_id` only on insert.
    if internal_id is not None and query.get("_id") is None:
        update_doc["$setOnInsert"] = {"internal_id": internal_id}

    try:
        res: UpdateResult = coll.update_one(query, update_doc, upsert=True)
        logger.info("upsert_article: update_one matched=%d modified=%d upserted_id=%s for %s",
                    getattr(res, "matched_count", None),
                    getattr(res, "modified_count", None),
                    getattr(res, "upserted_id", None),
                    collection_name)
        return res
    except DuplicateKeyError as e:
        logger.warning("upsert_article: DuplicateKeyError while upserting into %s: %s", collection_name, e)
        # Fallback: try an insert without client _id; include internal_id as regular field
        try:
            fallback_doc = dict(set_doc)  # shallow copy
            if internal_id is not None and "internal_id" not in fallback_doc:
                fallback_doc["internal_id"] = internal_id
            insert_res: InsertOneResult = coll.insert_one(fallback_doc)
            logger.info("upsert_article: fallback insert succeeded with id=%s into %s", insert_res.inserted_id, collection_name)
            return insert_res
        except Exception:
            logger.exception("upsert_article: fallback insert also failed for collection %s", collection_name)
            raise
    except PyMongoError:
        logger.exception("upsert_article: unexpected pymongo error for %s", collection_name)
        raise
