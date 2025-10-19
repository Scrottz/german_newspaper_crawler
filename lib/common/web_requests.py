from __future__ import annotations
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
import re
import requests
import importlib
import hashlib
from pymongo.collection import Collection

from lib.common.logging import get_logger
from lib.common.object_model import ObjectModel

logger = get_logger(__name__)


def fetch_url(url: str, timeout: int = 15, headers: Optional[Dict[str, str]] = None) -> str:
    """Fetch a URL and return its response text. Raises on HTTP errors."""
    logger.info("fetch_url: fetching %s", url)
    resp = requests.get(url, timeout=timeout, headers=headers)
    resp.raise_for_status()
    return resp.text


def _coerce_to_objectmodel(result: Any, url: str, html: str) -> ObjectModel:
    """Coerce parser result into an ObjectModel. Accepts ObjectModel, dict or raw HTML."""
    if isinstance(result, ObjectModel):
        return result
    if isinstance(result, dict):
        return ObjectModel.from_dict(result)
    text = re.sub(r"<[^>]+>", " ", html or "")
    return ObjectModel(url=url, html=html, text=text)


def _resolve_collection(
    get_collection_for_domain: Callable[..., Any],
    mongo_client: Any,
    db_name: str,
    domain_cfg: Dict[str, Any],
) -> Tuple[Any, str]:
    """
    Resolve the result of get_collection_for_domain into (raw_collection, collection_name).
    Supports flexible signatures for get_collection_for_domain.
    """
    res = None
    try:
        res = get_collection_for_domain(domain_cfg)
    except TypeError:
        try:
            res = get_collection_for_domain(mongo_client, db_name, domain_cfg)
        except TypeError:
            raise

    col_name: Optional[str] = None
    if isinstance(res, str):
        col_name = res
    elif isinstance(res, (list, tuple)) and len(res) >= 2 and isinstance(res[1], str):
        col_name = res[1]
    elif isinstance(res, Collection):
        col_name = getattr(res, "name", None)
    else:
        col_name = getattr(res, "name", None) if res is not None else None

    if not col_name:
        col_name = domain_cfg.get("collection") or domain_cfg.get("name")

    return res, col_name


def process_domain_generic(
    domain_cfg: Dict[str, Any],
    get_collection_for_domain: Callable[..., Any] = None,
    refresh_known_hashes_for_collection: Optional[Callable[..., Optional[Set[str]]]] = None,
    upsert_article: Optional[Callable[..., Any]] = None,
    ensure_indexes_for_collections: Optional[Callable[..., Any]] = None,
    mongo_client: Any = None,
    db_name: str = None,
    known_hashes: Optional[Set[str]] = None,
    pos_tag_fn: Optional[Callable[[ObjectModel], Any]] = None,
    timeout: int = 15,
    headers: Optional[Dict[str, str]] = None,
    article_urls: Optional[Iterable[str]] = None,
) -> Optional[Set[str]]:
    """
    Process a domain configuration and upsert discovered articles.

    - article_urls: optional iterable of article URLs; if not provided the domain's
      'get_article_urls' callable from domain_cfg will be used.
    - domain_cfg may optionally include a callable 'parse_article' which will be
      invoked for each article (flexible signatures supported).
    """
    if not isinstance(domain_cfg, dict):
        raise ValueError("domain_cfg must be a dict")

    if get_collection_for_domain is None or upsert_article is None or mongo_client is None or db_name is None:
        raise ValueError("Required callbacks or Mongo client/db_name are missing")

    raw_collection, collection_name = _resolve_collection(get_collection_for_domain, mongo_client, db_name, domain_cfg)
    logger.info("process_domain_generic: processing domain %s -> collection %s", domain_cfg.get("name"), collection_name)

    if ensure_indexes_for_collections:
        try:
            ensure_indexes_for_collections(mongo_client, db_name, [collection_name] if collection_name else [])
        except TypeError:
            ensure_indexes_for_collections(mongo_client, db_name)

    if article_urls is not None:
        urls_iterable: Iterable[str] = article_urls
    else:
        fn = domain_cfg.get("get_article_urls")
        if not callable(fn):
            raise AttributeError("Either provide `article_urls` argument or domain_cfg must provide callable 'get_article_urls'")
        try:
            urls_iterable = fn(domain_cfg)
        except TypeError:
            urls_iterable = fn()

    urls: List[str] = list(urls_iterable)
    logger.info("process_domain_generic: discovered %d article urls for domain %s", len(urls), domain_cfg.get("name"))

    if known_hashes is None:
        known_hashes = set()

    # Optional parse callable provided in domain_cfg
    parse_fn = domain_cfg.get("parse_article")
    if parse_fn is not None and not callable(parse_fn):
        raise AttributeError("domain_cfg['parse_article'] must be callable if provided")

    for url in urls:
        logger.debug("process_domain_generic: fetching article %s", url)
        try:
            html = fetch_url(url, timeout=timeout, headers=headers)
        except Exception:
            logger.exception("process_domain_generic: fetch failed for %s", url)
            html = ""

        # If parse function present: support flexible signatures
        if parse_fn is not None:
            try:
                parsed_result = parse_fn(url, html)
            except TypeError:
                parsed_result = parse_fn(url)
            except Exception:
                logger.exception("process_domain_generic: parse_fn raised for %s", url)
                parsed_result = None
        else:
            parsed_result = None  # _coerce_to_objectmodel will build a minimal ObjectModel from html

        if parsed_result is None:
            logger.debug("process_domain_generic: no parse function, creating minimal ObjectModel for %s", url)

        obj = _coerce_to_objectmodel(parsed_result, url, html)

        # robust content_hash calculation and comparison against known_hashes
        try:
            # compute candidates
            u = getattr(obj, "url", None)
            data = (getattr(obj, "text", "") or getattr(obj, "html", "") or "").strip()

            url_hash: Optional[str] = None
            content_hash_calc: Optional[str] = None

            if isinstance(u, str) and u.startswith(("http://", "https://")):
                try:
                    h = hashlib.sha256()
                    h.update(u.encode("utf-8"))
                    url_hash = h.hexdigest()
                except Exception:
                    logger.exception("process_domain_generic: failed computing url_hash for %s", u)

            if data:
                try:
                    h2 = hashlib.sha256()
                    h2.update(data.encode("utf-8"))
                    content_hash_calc = h2.hexdigest()
                except Exception:
                    logger.exception("process_domain_generic: failed computing content_hash from data for %s", url)

            # If obj already has content_hash keep it; otherwise prefer content hash, fallback to url hash
            if not getattr(obj, "content_hash", None):
                obj.content_hash = content_hash_calc or url_hash

            # Check known_hashes against all variants
            if obj.content_hash and obj.content_hash in known_hashes:
                logger.debug("process_domain_generic: skipping known content %s (hash=%s)", url, obj.content_hash)
                continue

            if url_hash and url_hash in known_hashes:
                obj.content_hash = url_hash
                logger.debug("process_domain_generic: skipping known content (url-hash) %s (hash=%s)", url, url_hash)
                continue

            if content_hash_calc and content_hash_calc in known_hashes:
                obj.content_hash = content_hash_calc
                logger.debug("process_domain_generic: skipping known content (content-hash) %s (hash=%s)", url, content_hash_calc)
                continue
        except Exception:
            logger.exception("process_domain_generic: failed to compute or compare content_hash for %s", url)

        # only call pos_tag_fn if the article is not skipped
        if pos_tag_fn is not None:
            try:
                pos_tag_fn(obj)
            except Exception:
                logger.exception("process_domain_generic: pos_tag_fn failed for %s", url)

        # Re-check known hashes in case pos_tag_fn or other steps modified content_hash
        try:
            if getattr(obj, "content_hash", None) and obj.content_hash in known_hashes:
                logger.debug("process_domain_generic: skipping known content after tagging %s", obj.content_hash)
                continue
        except Exception:
            logger.exception("process_domain_generic: re-check of content_hash failed for %s", url)

        # upsert and register new hash
        try:
            upsert_article(mongo_client, db_name, collection_name, obj)
        except Exception:
            logger.exception("process_domain_generic: upsert_article failed for %s", url)
        if obj.content_hash:
            known_hashes.add(obj.content_hash)
        logger.info("process_domain_generic: upserted article _id=%s url=%s", getattr(obj, "_id", None), obj.url)

    if refresh_known_hashes_for_collection is not None:
        try:
            return refresh_known_hashes_for_collection(mongo_client, db_name, collection_name)
        except Exception:
            logger.exception("process_domain_generic: refresh_known_hashes_for_collection failed for %s", collection_name)
            return known_hashes

    return known_hashes


def extract_collection_name(result: Any, domain_cfg: Dict[str, Any]) -> Optional[str]:
    """Extract collection name from a get_collection result or domain config."""
    if result is None:
        return domain_cfg.get("collection") or domain_cfg.get("name")
    if isinstance(result, (list, tuple)) and len(result) >= 2:
        name = result[1]
        if isinstance(name, str) and name:
            return name
    name = getattr(result, "name", None)
    if isinstance(name, str) and name:
        return name
    return domain_cfg.get("collection") or domain_cfg.get("name")


def build_article_urls(domain_cfg: Dict[str, Any], known_hashes: Optional[Set[str]] = None) -> List[str]:
    """
    Load a domain module and call its get_article_urls function.
    If the module exports a callable 'parse_article', attach it to domain_cfg['parse_article'].
    Pass `known_hashes` into the domain config so domain-specific fetchers (z.B. TAZ)
    can skip known urls already during link-collection.
    Returns a list (possibly empty) of article URLs.
    """
    module_name = domain_cfg.get("module") or f"lib.domain.{domain_cfg.get('name')}"

    # work on a shallow copy to avoid mutating caller's dict
    cfg_for_module = dict(domain_cfg)
    if known_hashes is not None:
        # pass a copy into the module
        cfg_for_module["known_hashes"] = set(known_hashes)
        # also try to expose known_hashes on the original domain_cfg so other parts can see it
        try:
            domain_cfg["known_hashes"] = set(known_hashes)
        except Exception:
            logger.debug("build_article_urls: could not set known_hashes on original domain_cfg")

    try:
        mod = importlib.import_module(module_name)
    except Exception:
        logger.exception("Failed to import domain module %s; returning empty url list", module_name)
        return []

    # If the module exposes a parse_article callable, attach it to both the domain config copy
    # and the original domain_cfg so process_domain_generic can make use of it later.
    parse_fn = getattr(mod, "parse_article", None)
    if callable(parse_fn):
        cfg_for_module["parse_article"] = parse_fn
        try:
            domain_cfg["parse_article"] = parse_fn
        except Exception:
            logger.exception("Failed to attach parse_article to domain_cfg for module %s", module_name)

    fn = getattr(mod, "get_article_urls", None)
    if not callable(fn):
        logger.debug("Domain module %s does not provide get_article_urls; returning empty url list", module_name)
        return []

    try:
        # prefer calling with domain_cfg copy so domain modules can read known_hashes
        urls_iter = fn(cfg_for_module)
    except TypeError:
        try:
            urls_iter = fn()
        except Exception:
            logger.exception("Calling get_article_urls() failed for module %s", module_name)
            return []
    except Exception:
        logger.exception("Calling get_article_urls(domain_cfg) failed for module %s", module_name)
        return []

    try:
        return list(urls_iter or [])
    except Exception:
        logger.exception("Failed to iterate over URLs from %s; returning empty list", module_name)
        return []
