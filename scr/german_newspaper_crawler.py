"""Entry point for the crawler. Configuration must be provided via configs/config.yaml
and loaded through lib.common.config_handler.
"""

from pathlib import Path
import sys
import importlib.util
import importlib
from typing import Set, Optional, Any, Iterable, List, Dict

from pymongo import MongoClient

from lib.common.logging import get_logger
from lib.common.web_requests import (
    process_domain_generic,
    extract_collection_name,
    build_article_urls,
)
from lib.common.mongodb import (
    collect_known_hashes,
    get_collection_for_domain,
    refresh_known_hashes_for_collection,
    upsert_article,
    ensure_indexes_for_collections,
    close_mongo_client,
)
from lib.common.config_handler import load_config, load_mongodb_config
from lib.common.pos_tagging import pos_tag_object

logger = get_logger(__name__)

def main() -> None:
    """
    Main entrypoint for the crawler script.
    Expects a valid configuration containing `domains` and a filled `mongodb` section.
    """
    cfg = load_config()
    if not isinstance(cfg, dict) or "domains" not in cfg:
        logger.error("Configuration is missing or does not contain the 'domains' key")
        return

    domains = cfg.get("domains")
    if not isinstance(domains, list) or not domains:
        logger.error("No domains configured in 'domains' section")
        return

    mcfg = load_mongodb_config()
    if not getattr(mcfg, "uri", None):
        logger.error("MongoDB URI missing in configuration ('mongodb.uri')")
        return
    if not getattr(mcfg, "database_name", None):
        logger.error("MongoDB database name missing in configuration ('mongodb.database_name')")
        return

    db_name = mcfg.database_name

    mongo_client: Optional[MongoClient] = None
    try:
        try:
            mongo_client = MongoClient(mcfg.uri)
            logger.info("Connected to MongoDB at %s", mcfg.uri)
        except Exception:
            logger.exception("Error creating MongoClient from configured URI")
            return

        # collect known hashes globally
        known_hashes: Set[str] = set()
        try:
            known = collect_known_hashes(mongo_client, db_name) or set()
            known_hashes = known
            logger.info("Collected %d known hashes", len(known_hashes))
        except Exception:
            logger.exception("Failed to collect known hashes, continuing with empty set")
            known_hashes = set()

        for domain in domains:
            try:
                logger.info("Starting crawl for domain %s", domain.get("name"))

                # Adapter: provide a callable that accepts only domain_cfg but calls the real get_collection_for_domain
                collection_for_domain = (
                    lambda domain_cfg, gfn=get_collection_for_domain, mc=mongo_client, db=db_name: gfn(mc, db, domain_cfg)
                )

                # Wrapper for ensure_indexes_for_collections: resolve collection name and call ensure_indexes
                def ensure_indexes_wrapper(mc: MongoClient, db: str) -> None:
                    try:
                        res = collection_for_domain(domain)
                        col_name = extract_collection_name(res, domain)
                        if col_name:
                            ensure_indexes_for_collections(mc, db, [col_name])
                        else:
                            ensure_indexes_for_collections(mc, db, [])
                    except Exception:
                        logger.exception("ensure_indexes_wrapper: failed to ensure indexes for domain %s", domain.get("name"))

                # Build the list of article URLs using the centralized helper
                article_urls = build_article_urls(domain)

                updated = process_domain_generic(
                    domain_cfg=domain,
                    get_collection_for_domain=collection_for_domain,
                    refresh_known_hashes_for_collection=refresh_known_hashes_for_collection,
                    upsert_article=upsert_article,
                    ensure_indexes_for_collections=ensure_indexes_wrapper,
                    mongo_client=mongo_client,
                    db_name=db_name,
                    known_hashes=known_hashes,
                    pos_tag_fn=pos_tag_object,
                    article_urls=article_urls,
                )
                if updated is not None:
                    known_hashes = updated
            except Exception:
                logger.exception("Error crawling domain %s", domain.get("name"))
    except Exception:
        logger.exception("Unhandled exception in main")
    finally:
        try:
            close_mongo_client(mongo_client)
        except Exception:
            logger.exception("Failed to close mongo client")


if __name__ == "__main__":
    main()
