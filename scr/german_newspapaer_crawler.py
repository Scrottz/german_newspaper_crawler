import logging
from datetime import datetime
from typing import Dict, Any, Optional, Set, List

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, WriteError

from lib.common.logging import setup_logging, get_logger
from lib.common.mongodb import collect_content_hashes_from_db
from lib.domain.taz import TAZ
from lib.common.object_model import ObjectModel

logger = get_logger(__name__)
logger.debug("Logging initialized; root level=%s handlers=%s", logging.getLogger().level, [type(h).__name__ for h in logging.getLogger().handlers])

class GermanNewspaperCrawler:
    def __init__(self, mongo_uri: str, db_name: str):
        self._mongo_client = MongoClient(mongo_uri) if mongo_uri else None
        self._db_name = db_name
        self.known_hashes: Set[str] = set()

    def collect_known_hashes(self) -> Set[str]:
        try:
            hashes = collect_content_hashes_from_db(self._mongo_client, self._db_name)
            self.known_hashes = hashes or set()
            logger.debug("collect_known_hashes(): collected %d hashes", len(self.known_hashes))
            return self.known_hashes
        except Exception:
            logger.exception("Failed to collect known hashes from DB")
            return set()

    def crawl_domain(self, domain_cfg: Dict[str, Any]) -> None:
        coll_name = domain_cfg.get("collection_name") or domain_cfg.get("collection") or domain_cfg.get("name")
        base_url = domain_cfg.get("base_url") or domain_cfg.get("url")
        if not coll_name or not base_url:
            logger.error("Invalid domain configuration: %s", domain_cfg)
            return

        if not self._mongo_client:
            logger.error("No mongo client available")
            return

        db = self._mongo_client[self._db_name]
        coll = db[coll_name]

        taz = TAZ(base_url, known_hashes=self.known_hashes)
        urls = taz.fetch_article_urls()
        logger.info("Starting crawl for collection %s, found %d urls", coll_name, len(urls))

        for url in urls:
            try:
                obj = taz.parse_article_to_object(url)
                if not obj or not isinstance(obj, ObjectModel):
                    logger.warning("Parsed object invalid for %s", url)
                    continue

                # Skip storing obviously empty results to avoid placeholder documents in DB
                html_val = (getattr(obj, "html", "") or "").strip()
                text_val = (getattr(obj, "text", "") or "").strip()
                if not html_val and not text_val:
                    logger.warning("Skipping storage for %s: parsed html/text empty", url)
                    continue

                # Prepare document for DB: avoid bringing an _id field
                if hasattr(obj, "to_dict"):
                    doc = obj.to_dict()
                else:
                    doc = obj.__dict__.copy()
                doc.pop("_id", None)

                # Use content_hash as canonical key if available, otherwise fallback to url
                query = {"content_hash": getattr(obj, "content_hash", None)} if getattr(obj, "content_hash", None) else {"url": getattr(obj, "id", getattr(obj, "url", url))}

                # Upsert via update_one($set) to avoid inserting conflicting/immutable _id
                try:
                    coll.update_one(query, {"$set": doc}, upsert=True)
                    logger.info("Stored/updated article %s", url)
                except DuplicateKeyError:
                    logger.warning("DuplicateKeyError on update_one for %s; retrying with url key", url)
                    fallback_query = {"url": getattr(obj, "id", getattr(obj, "url", url))}
                    coll.update_one(fallback_query, {"$set": doc}, upsert=True)
                    logger.info("Stored/updated article (fallback) %s", url)
                except WriteError:
                    logger.exception("WriteError when storing article %s", url)

            except Exception:
                logger.exception("Failed to process article %s", url)

    def close(self) -> None:
        try:
            if self._mongo_client:
                self._mongo_client.close()
        except Exception:
            logger.exception("Error closing mongo client")


def main():
    mongo_uri = "mongodb://localhost:27017"
    db_name = "german_news_papaer"
    domain_cfgs: List[Dict[str, Any]] = [
        {"name": "taz", "base_url": "https://taz.de/"},
    ]

    crawler = GermanNewspaperCrawler(mongo_uri, db_name)
    try:
        crawler.collect_known_hashes()
        for domain in domain_cfgs:
            try:
                logger.info("main(): Starting crawl for domain %s", domain.get("name"))
                crawler.crawl_domain(domain)
            except Exception:
                logger.exception("main(): Error crawling domain %s", domain.get("name"))
    finally:
        crawler.close()


if __name__ == "__main__":
    main()
