from __future__ import annotations
from typing import Optional, Dict, Any, Set, List, Callable, Tuple
import urllib.request
import urllib.error
from pymongo import MongoClient
from lib.common.logging import get_logger

logger = get_logger(__name__)


def _fetch_bytes(url: str, timeout: int = 10, headers: Optional[Dict[str, str]] = None) -> Tuple[bytes, Optional[str]]:
    """Fetch raw bytes from `url`. Returns (bytes, charset_hint_from_headers)."""
    headers = headers or {"User-Agent": "german_newspaper_crawler/1.0 (+https://example.invalid)"}
    req = urllib.request.Request(url, headers=headers)
    logger.debug("Fetching URL %s with timeout=%s", url, timeout)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            # Try to get charset from Content-Type header if present
            charset = None
            try:
                charset = resp.headers.get_content_charset()  # type: ignore[attr-defined]
            except Exception:
                # Older Python or unexpected header object; fallback to parsing manually
                ctype = resp.headers.get("Content-Type")
                if ctype:
                    parts = [p.strip() for p in ctype.split(";")]
                    for part in parts[1:]:
                        if part.lower().startswith("charset="):
                            charset = part.split("=", 1)[1].strip()
                            break
            logger.debug("Fetched %d bytes from %s (charset_hint=%s)", len(body), url, charset)
            return body, charset
    except urllib.error.HTTPError as e:
        logger.warning("HTTP error fetching %s: %s", url, e)
        raise
    except urllib.error.URLError as e:
        logger.warning("Network error fetching %s: %s", url, e)
        raise


def get_html(url: str, timeout: int = 10, headers: Optional[Dict[str, str]] = None, encoding: Optional[str] = "utf-8") -> str:
    """
    Retrieve `url` and return the response body as a string.
    If `encoding` is provided it will be used preferentially; otherwise
    the Content-Type charset hint from the response is used; final fallback
    is UTF-8 with replacement for invalid bytes.
    """
    body, charset_hint = _fetch_bytes(url, timeout=timeout, headers=headers)

    # Determine which encoding to use: explicit param > header hint > utf-8
    use_enc = encoding or charset_hint or "utf-8"
    logger.debug("Decoding bytes using encoding=%s (header_hint=%s)", use_enc, charset_hint)

    try:
        text = body.decode(use_enc, errors="replace")
    except LookupError:
        # Unknown encoding name -> fallback to utf-8
        logger.warning("Unknown encoding %r for %s, falling back to utf-8", use_enc, url)
        text = body.decode("utf-8", errors="replace")

    logger.debug("Decoded HTML length=%d for %s", len(text), url)
    return text


def fetch_article_urls_with_parser(
    base_url: str,
    parser_factory: Callable[..., Any],
    known_hashes: Optional[Set[str]] = None,
) -> List[str]:
    """
    Erzeuge einen Parser über `parser_factory` und rufe `fetch_article_urls()` auf.
    Gibt eine Liste gefundener URLs zurück oder eine leere Liste bei Fehlern.
    """
    try:
        # parser_factory sollte mindestens (base_url, known_hashes=...) akzeptieren
        parser = parser_factory(base_url, known_hashes=known_hashes)
        urls = parser.fetch_article_urls()
        logger.debug("fetch_article_urls_with_parser: found %d urls for %s", len(urls) if urls else 0, base_url)
        return urls or []
    except Exception:
        logger.exception("fetch_article_urls_with_parser: failed for %s", base_url)
        return []


def process_domain_generic(
    domain_cfg: Dict[str, Any],
    parser_factory: Callable[..., Any],
    get_collection_for_domain: Callable[[Optional[MongoClient], Optional[str], Dict[str, Any]], Optional[Any]],
    refresh_known_hashes_for_collection: Callable[[Any, Set[str]], Set[str]],
    upsert_article: Callable[[Any, Any, str], bool],
    ensure_indexes_for_collections: Callable[[Optional[MongoClient], Optional[str]], None],
    mongo_client: Optional[MongoClient] = None,
    db_name: Optional[str] = None,
    known_hashes: Optional[Set[str]] = None,
) -> Optional[Set[str]]:
    """
    Generische Domain‑Verarbeitung, die die Logik aus dem bisherigen Crawler kapselt.

    - `parser_factory` erzeugt Parser-Instanzen für eine base_url.
    - DB-Funktionen werden als Callables injiziert (frei konfigurierbar / testbar).
    - Gibt das aktualisierte Set `known_hashes` zurück, oder None wenn die Domain nicht verarbeitet wurde.
    """
    known_hashes = known_hashes or set()

    try:
        ensure_indexes_for_collections(mongo_client, db_name)
    except Exception:
        logger.exception("process_domain_generic: ensure_indexes_for_collections failed (non-fatal)")

    coll = get_collection_for_domain(mongo_client, db_name, domain_cfg)
    if coll is None:
        logger.error("process_domain_generic: invalid domain config or DB unavailable: %s", domain_cfg)
        return None

    try:
        known_hashes = refresh_known_hashes_for_collection(coll, known_hashes)
    except Exception:
        logger.exception(
            "process_domain_generic: failed to refresh known hashes for collection %s",
            getattr(coll, "name", "<unknown>"),
        )

    base_url = domain_cfg.get("base_url") or domain_cfg.get("url")
    if not base_url:
        logger.error("process_domain_generic: missing base_url in domain config %s", domain_cfg)
        return None

    urls = fetch_article_urls_with_parser(base_url, parser_factory, known_hashes)
    logger.info("process_domain_generic: collection=%s found %d urls", getattr(coll, "name", "<unknown>"), len(urls))

    for url in urls:
        try:
            parser = parser_factory(base_url, known_hashes=known_hashes)
            obj = parser.parse_article_to_object(url)
            if not obj:
                logger.warning("process_domain_generic: parsed invalid object for %s", url)
                continue

            stored = upsert_article(coll, obj, url)
            if stored:
                ch = getattr(obj, "content_hash", None)
                if isinstance(ch, str) and ch:
                    known_hashes.add(ch)
        except Exception:
            logger.exception("process_domain_generic: failed processing article %s", url)

    return known_hashes
