# python
from __future__ import annotations
from typing import Dict, Any, List, Set, Tuple, Optional
from importlib import import_module
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

from lib.common.logging import get_logger, setup_logging
from lib.common.config_handler import load_config
from lib.common.object_model import ObjectModel, to_dict
from lib.common.mongodb import load_mongodb_config, get_domain_collection_name, collect_content_hashes_from_db

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


def main() -> int:
    try:
        config: Dict[str, Any] = load_config()
        logging_cfg = config.get("logging", {}) if isinstance(config, dict) else {}
        level = logging_cfg.get("level", "INFO")
        setup_logging(level)
        logger = get_logger(__name__)

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
        all_results: Dict[str, List[ObjectModel]] = {}

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
                    # inject known hashes so domain implementations can skip already seen articles
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

            articles: List[ObjectModel] = []
            for url in unique_urls:
                html: Optional[str] = None
                try:
                    logger.info("Fetching HTML for %s", url)
                    resp = requests.get(url, timeout=10)
                    resp.raise_for_status()
                    html = resp.text

                    title, teaser = _extract_title_teaser(html)

                    # instantiate domain parser for the specific URL
                    article_instance = DomainClass(url)
                    # inject known hashes so per-article parsing can check and skip
                    try:
                        setattr(article_instance, "known_hashes", existing_hashes)
                    except Exception:
                        logger.debug("Could not set known_hashes attribute on article parser %s", class_path)

                    # prefer parse_article_to_object when available
                    parse_to_obj = getattr(article_instance, "parse_article_to_object", None)
                    if callable(parse_to_obj):
                        obj = parse_to_obj(url, html=html, title=title, teaser=teaser)
                    else:
                        # fallback: get text and create ObjectModel here with parsed_date
                        text = article_instance.parse_article(html) or ""
                        obj = ObjectModel(
                            id=url,
                            html=html,
                            text=text,
                            titel=title,
                            teaser=teaser,
                            parsed_date=datetime.utcnow(),
                        )

                    # skip if content_hash already known
                    if obj.content_hash and obj.content_hash in existing_hashes:
                        logger.info("Skipping already known article %s (content_hash=%s)", url, obj.content_hash)
                        continue

                    articles.append(obj)
                    logger.info("Created ObjectModel for %s (text length: %d)", url, len(obj.text or ""))

                    # persist to MongoDB if possible
                    if collection is not None:
                        try:
                            doc = to_dict(obj)
                            doc["_id"] = doc.get("id") or url
                            collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                            logger.info("Upserted article %s into %s.%s", doc["_id"], mongodb_cfg.database_name, collection_name)
                            # ensure we don't re-process the same content in this run
                            if obj.content_hash:
                                existing_hashes.add(obj.content_hash)
                        except Exception:
                            logger.exception("Failed to write article %s to MongoDB", url)

                except Exception:
                    logger.exception("Failed to fetch/parse article %s for domain %s", url, domain_key)
                    empty_obj = ObjectModel(id=url, html=html, text="", titel="", teaser="", parsed_date=datetime.utcnow())
                    articles.append(empty_obj)
                    if collection is not None:
                        try:
                            doc = to_dict(empty_obj)
                            doc["_id"] = doc.get("id") or url
                            collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                            logger.info("Upserted (empty) article %s into %s.%s", doc["_id"], mongodb_cfg.database_name, collection_name)
                        except Exception:
                            logger.exception("Failed to write (empty) article %s to MongoDB", url)

            all_results[domain_key] = articles

        # close mongo client if opened
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
