# python
from __future__ import annotations
from typing import Dict, Any, List, Set, Tuple, Optional
from importlib import import_module
import re
from datetime import datetime
from urllib.parse import urljoin
import os
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, ReturnDocument
from tqdm import tqdm

from lib.common.logging import get_logger, setup_logging
from lib.common.config_handler import load_config

# Robustes Importieren der Mongo-/Config-Funktionen:
try:
    from lib.common.config_handler import (
        load_mongodb_config,
        get_domain_collection_name,
        collect_content_hashes_from_db,
    )
except Exception:
    try:
        from lib.common.mongodb import (
            load_mongodb_config,
            get_domain_collection_name,
            collect_content_hashes_from_db,
        )
    except Exception:
        import lib.common.mongodb as _mongodb
        load_mongodb_config = getattr(_mongodb, "load_mongodb_config", None) or getattr(_mongodb, "load_mongo_config", None)
        get_domain_collection_name = getattr(_mongodb, "get_domain_collection_name", None)
        collect_content_hashes_from_db = getattr(_mongodb, "collect_content_hashes_from_db", None)
        if not load_mongodb_config or not get_domain_collection_name or not collect_content_hashes_from_db:
            raise ImportError(
                "Required symbols not found in lib.common.config_handler or lib.common.mongodb: expected "
                "load_mongodb_config (or load_mongo_config), get_domain_collection_name, collect_content_hashes_from_db"
            )

from lib.common.parallel_fetcher import download_urls


def _load_class(class_path: str):
    module_path, _, class_name = class_path.partition(":")
    if not module_path or not class_name:
        raise ValueError("Invalid class_path, expected 'module.path:ClassName'")
    module = import_module(module_path)
    return getattr(module, class_name)


def _unique_preserve_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def _normalize_flow(text: str) -> str:
    t = re.sub(r"\s+", " ", text).strip()
    t = re.sub(r"^([^\s])\s+([^\s])", r"\1\2", t, count=1)
    return t


def _extract_title_teaser(html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.find(
        lambda tag: tag.name == "span"
        and tag.get("class")
        and any("headline" in c for c in tag.get("class"))
    )
    title = title_el.get_text(" ", strip=True) if title_el else ""
    teaser_el = soup.find(
        lambda tag: tag.name == "p"
        and tag.get("class")
        and any("typo-r-subline-detail" in c for c in tag.get("class"))
    )
    teaser = teaser_el.get_text(" ", strip=True) if teaser_el else ""
    return _normalize_flow(title), _normalize_flow(teaser)


def _compute_content_hash(obj) -> Optional[str]:
    data = (getattr(obj, "text", None) or getattr(obj, "html", "") or "").strip()
    if not data:
        return None
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _get_next_sequence(db, collection_name: str) -> Optional[int]:
    """
    Atomically increment the counter for the given collection_name in the '__counters__' collection.
    Returns the new sequence number or None on error.
    """
    if db is None or not collection_name:
        return None
    try:
        coll = db["__counters__"]
        res = coll.find_one_and_update(
            {"_id": collection_name},
            {"$inc": {"seq": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res and "seq" in res:
            return int(res["seq"])
    except Exception:
        pass
    return None


def main() -> int:
    try:
        config: Dict[str, Any] = load_config()
        logging_cfg = config.get("logging", {}) if isinstance(config, dict) else {}
        level = logging_cfg.get("level", "INFO")
        setup_logging(level)
        logger = get_logger(__name__)

        # download config (defaults)
        download_cfg = config.get("downloads", {}) if isinstance(config, dict) else {}
        downloads_enabled = bool(download_cfg.get("enabled", True))
        download_dest_root = download_cfg.get("dest_dir", "data/downloads")
        download_workers = int(download_cfg.get("workers", 6))

        # load mongodb config and try to connect
        mongodb_cfg = load_mongodb_config()
        mongo_client: Optional[MongoClient] = None
        mongo_db = None
        try:
            mongo_client = MongoClient()  # connects to localhost:27017 by default
            if mongodb_cfg.database_name:
                mongo_db = mongo_client[mongodb_cfg.database_name]
                logger.info("Connected to MongoDB database %s", mongodb_cfg.database_name)
            else:
                logger.warning("No MongoDB database_name configured; articles will not be persisted.")
        except Exception:
            logger.exception("Failed to connect to MongoDB; articles will not be persisted.")
            mongo_client = None
            mongo_db = None

        # --- load all existing content_hash values up-front to avoid duplicates ---
        existing_hashes: Set[str] = set()
        if mongo_client is not None and mongodb_cfg.database_name:
            try:
                existing_hashes = collect_content_hashes_from_db(mongo_client, mongodb_cfg.database_name)
                logger.info("Loaded %d existing content_hash values from DB", len(existing_hashes))
            except Exception:
                logger.exception("Failed to collect existing content_hashes from DB")
                existing_hashes = set()
        else:
            logger.debug("Skipping DB hash collection (no client or no database configured).")
        # -----------------------------------------------------------------------

        domains_cfg = config.get("domains", {}) if isinstance(config, dict) else {}
        all_results: Dict[str, List[object]] = {}

        for domain_key, domain_cfg in domains_cfg.items():
            logger.debug("Processing domain config: %s", domain_key)
            class_path = domain_cfg.get("class_path")
            base_urls = domain_cfg.get("base_urls", []) or []

            if not class_path:
                logger.warning("No class_path for domain %s, skipping", domain_key)
                continue

            try:
                DomainClass = _load_class(class_path)
            except Exception:
                logger.exception("Failed to load class for domain %s (%s)", domain_key, class_path)
                continue

            # determine target collection for this domain (fallback to domain_key)
            collection_name = get_domain_collection_name(domain_cfg) or domain_key
            collection = None
            if mongo_db is not None and collection_name:
                collection = mongo_db[collection_name]

            domain_urls: List[str] = []
            for base_url in base_urls:
                try:
                    logger.info("Instantiating %s for %s", class_path, base_url)
                    instance = DomainClass(base_url)
                    try:
                        setattr(instance, "known_hashes", existing_hashes)
                    except Exception:
                        logger.debug("Could not set known_hashes attribute on domain instance %s", class_path)
                    found = instance.fetch_article_urls()
                    domain_urls.extend(found)
                    logger.info("Found %d URLs for %s", len(found), base_url)
                except Exception:
                    logger.exception("Error fetching URLs for domain %s base %s", domain_key, base_url)

            unique_urls = _unique_preserve_order(domain_urls)

            articles: List[object] = []
            download_candidates: List[str] = []

            def _fetch_and_parse(url: str) -> Dict[str, Any]:
                html: Optional[str] = None
                try:
                    logger.info("Fetching HTML for %s", url)
                    resp = requests.get(url, timeout=10)
                    resp.raise_for_status()
                    html = resp.text

                    title, teaser = _extract_title_teaser(html)

                    links: List[str] = []
                    if downloads_enabled and html:
                        try:
                            link_soup = BeautifulSoup(html, "html.parser")
                            for a_tag in link_soup.find_all("a", href=True):
                                href = a_tag["href"].strip()
                                if not href:
                                    continue
                                full = urljoin(url, href)
                                links.append(full)
                        except Exception:
                            logger.exception("Failed to collect download links from %s", url)

                    article_instance = DomainClass(url)
                    try:
                        setattr(article_instance, "known_hashes", existing_hashes)
                    except Exception:
                        logger.debug("Could not set known_hashes attribute on article parser %s", class_path)

                    parse_to_obj = getattr(article_instance, "parse_article_to_object", None)
                    if callable(parse_to_obj):
                        obj = parse_to_obj(url, html=html, title=title, teaser=teaser)
                    else:
                        text = article_instance.parse_article(html) or ""
                        obj = type("ObjectModel", (), {})()
                        setattr(obj, "id", url)
                        setattr(obj, "html", html)
                        setattr(obj, "text", text)
                        setattr(obj, "titel", title)
                        setattr(obj, "teaser", teaser)
                        setattr(obj, "parsed_date", datetime.utcnow())

                    obj.content_hash = _compute_content_hash(obj)

                    return {"status": "ok", "url": url, "obj": obj, "links": links}
                except Exception:
                    logger.exception("Failed to fetch/parse article %s for domain %s", url, domain_key)
                    empty_obj = type("ObjectModel", (), {})()
                    setattr(empty_obj, "id", url)
                    setattr(empty_obj, "html", html)
                    setattr(empty_obj, "text", "")
                    setattr(empty_obj, "titel", "")
                    setattr(empty_obj, "teaser", "")
                    setattr(empty_obj, "parsed_date", datetime.utcnow())
                    empty_obj.content_hash = _compute_content_hash(empty_obj)
                    return {"status": "error", "url": url, "obj": empty_obj, "links": []}

            if unique_urls:
                with ThreadPoolExecutor(max_workers=max(1, download_workers)) as exe:
                    futures = {exe.submit(_fetch_and_parse, u): u for u in unique_urls}
                    progress = tqdm(total=len(futures), desc=f"{domain_key} articles", unit="article")
                    for fut in as_completed(futures):
                        res = fut.result()
                        url = res.get("url")
                        obj = res.get("obj")
                        links = res.get("links", []) or []

                        if getattr(obj, "content_hash", None) and obj.content_hash in existing_hashes:
                            logger.info("Skipping already known article %s (content_hash=%s)", url, obj.content_hash)
                        else:
                            articles.append(obj)
                            if collection is not None:
                                try:
                                    doc = getattr(obj, "to_dict", lambda: None)()
                                    if not doc:
                                        doc = {k: getattr(obj, k) for k in ("id", "html", "text", "titel", "teaser", "parsed_date", "content_hash") if hasattr(obj, k)}

                                    # Always generate an incremental numeric _id from the counter for this collection.
                                    # If counter generation fails, fall back to existing id or URL.
                                    next_seq = None
                                    try:
                                        next_seq = _get_next_sequence(mongo_db, collection_name) if mongo_db is not None else None
                                    except Exception:
                                        next_seq = None

                                    if next_seq is not None:
                                        doc["_id"] = next_seq
                                    else:
                                        doc["_id"] = doc.get("id") or url

                                    collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                                    logger.info("Upserted article %s into %s.%s", doc["_id"], mongodb_cfg.database_name, collection_name)
                                    if getattr(obj, "content_hash", None):
                                        existing_hashes.add(obj.content_hash)
                                except Exception:
                                    logger.exception("Failed to write article %s to MongoDB", url)

                        download_candidates.extend(links)
                        progress.update(1)
                    progress.close()

            if downloads_enabled and download_candidates:
                try:
                    download_candidates = _unique_preserve_order(download_candidates)
                    dest_dir = os.path.join(download_dest_root, domain_key)
                    logger.info("Starting parallel downloads for domain %s: %d candidate links", domain_key, len(download_candidates))
                    results = download_urls(download_candidates, dest_dir, max_workers=download_workers, show_progress=True)
                    succ = sum(1 for r in results if r.get("success"))
                    logger.info("Parallel downloads finished for %s: %d/%d succeeded. Files in %s", domain_key, succ, len(results), dest_dir)
                except Exception:
                    logger.exception("Parallel download phase failed for domain %s", domain_key)

            all_results[domain_key] = articles

        if mongo_client is not None:
            try:
                mongo_client.close()
            except Exception:
                logger.exception("Error closing MongoDB client")

        return 0

    except Exception:
        get_logger(__name__).exception("Unhandled exception in main()")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
