from __future__ import annotations
from typing import Optional, Dict
import urllib.request
import urllib.error
from lib.common.logging import get_logger

logger = get_logger(__name__)


def _fetch_bytes(url: str, timeout: int = 10, headers: Optional[Dict[str, str]] = None) -> tuple[bytes, Optional[str]]:
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
    Retrieve `url` and return the response body as a UTF-8 string.
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
