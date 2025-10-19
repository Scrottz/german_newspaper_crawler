# python
"""Entry point for the crawler. Configuration must be provided via configs/config.yaml
and loaded through lib/common/config_handler. This module enforces strict use of the
configuration (no fallbacks) and uses process_domain_generic for domain processing.
"""

from typing import List, Dict, Any, Optional, Set

from pymongo import MongoClient

from lib.common.logging import get_logger
from lib.common.web_requests import process_domain_generic
from lib.domain.taz import TAZ
from lib.common.mongodb import (
    collect_known_hashes,
    get_collection_for_domain,
    refresh_known_hashes_for_collection,
    upsert_article,
    ensure_indexes_for_collections,
    close_mongo_client,
)
from lib.common.config_handler import load_config, load_mongodb_config

logger = get_logger(__name__)


def main() -> None:
    """
    Main entrypoint for the crawler script.
    Expects a valid configuration containing `domains` and a filled `mongodb` section
    (including a full `uri` and `database` name). If required config entries are
    missing the script will exit and log an error.
    """
    cfg = load_config()
    if not isinstance(cfg, dict) or "domains" not in cfg:
        logger.error("Configuration is missing or does not contain the `domains` key in configs/config.yaml")
        return

    domains = cfg.get("domains")
    # comment: ensure domains is a non-empty list
    if not isinstance(domains, list) or not domains:
        logger.error("Configuration value `domains` is missing, not a valid array, or empty")
        return

    mcfg = load_mongodb_config()
    # strict: require a complete MongoDB URI and a database name in mongodb config
    if not mcfg.uri:
        logger.error("MongoDB URI is missing in configuration (`mongodb.uri`)")
        return
    if not mcfg.database_name:
        logger.error("MongoDB database name is missing in configuration (`mongodb.database` / `database_name` / `db`)")
        return

    db_name = mcfg.database_name

    mongo_client: Optional[MongoClient] = None
    try:
        # comment: create MongoClient from the provided URI; fail fast on error
        try:
            mongo_client = MongoClient(mcfg.uri)
        except Exception:
            logger.exception("Error creating MongoClient from the configured URI")
            return

        known_hashes: Set[str] = set()
        try:
            known = collect_known_hashes(mongo_client, db_name) or set()
            known_hashes = known
            logger.info("main(): collected %d known hashes", len(known_hashes))
        except Exception:
            logger.exception("main(): failed to collect known hashes, continuing with empty set")
            known_hashes = set()

        for domain in domains:
            try:
                logger.info("main(): Starting crawl for domain %s", domain.get("name"))
                updated = process_domain_generic(
                    domain_cfg=domain,
                    parser_factory=TAZ,
                    get_collection_for_domain=get_collection_for_domain,
                    refresh_known_hashes_for_collection=refresh_known_hashes_for_collection,
                    upsert_article=upsert_article,
                    ensure_indexes_for_collections=ensure_indexes_for_collections,
                    mongo_client=mongo_client,
                    db_name=db_name,
                    known_hashes=known_hashes,
                )
                if updated is not None:
                    known_hashes = updated
            except Exception:
                logger.exception("main(): Error crawling domain %s", domain.get("name"))
    finally:
        try:
            close_mongo_client(mongo_client)
        except Exception:
            logger.exception("main(): failed to close mongo client")


if __name__ == "__main__":
    main()
