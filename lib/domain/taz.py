# python
from __future__ import annotations
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin
import json
import re
from datetime import datetime
import hashlib

import requests
from bs4 import BeautifulSoup

from lib.common.logging import get_logger
from lib.common.object_model import ObjectModel

logger = get_logger(__name__)


def _extract_meta_from_soup(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    """
    Extrahiert published_date (ISO string) und autor (string) aus dem BeautifulSoup-Objekt.
    - sucht <time datetime="..."> und parst das Attribut in einen ISO-String (wenn möglich).
    - sucht <span class="...typo-name-detail-bold..."> für den Autortext.
    """
    published_iso: Optional[str] = None
    author: Optional[str] = None

    # <time datetime="...">
    time_tag = soup.find("time")
    if time_tag:
        dt_attr = (time_tag.get("datetime") or "").strip()
        if dt_attr:
            try:
                dt = datetime.fromisoformat(dt_attr)
                published_iso = dt.isoformat()
            except Exception:
                # Fallback: Datumsteil 'YYYY-MM-DD' extrahieren
                m = re.search(r"(\d{4}-\d{2}-\d{2})", dt_attr)
                if m:
                    published_iso = m.group(1)

    # <span class="...typo-name-detail-bold...">AUTOR</span>
    author_tag = soup.find("span", class_=lambda c: c and "typo-name-detail-bold" in c)
    if author_tag:
        txt = author_tag.get_text(" ", strip=True)
        if txt:
            # Minimal bereinigen
            author = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", txt)).strip()

    return published_iso, author


class TAZ:
    """
    TAZ crawler helper.

    Provides methods to collect article URLs and to parse article text.
    Accepts an optional `known_hashes` set so already-seen articles can be skipped.
    """

    def __init__(self, base_url: str, known_hashes: Optional[Set[str]] = None) -> None:
        self.base_url = base_url
        # allow injection via constructor or later via setattr(instance, "known_hashes", set)
        self.known_hashes: Set[str] = known_hashes or set()

    def fetch_article_urls(self, html: Optional[str] = None) -> List[str]:
        try:
            if html is None:
                logger.debug("Fetching base URL: %s", self.base_url)
                resp = requests.get(self.base_url, timeout=10)
                resp.raise_for_status()
                html = resp.text

            soup = BeautifulSoup(html, "html.parser")
            anchors = soup.find_all("a", class_="teaser-link")

            urls: List[str] = []
            for a in anchors:
                href = a.get("href")
                if not href:
                    continue
                full_url = urljoin(self.base_url, href)
                urls.append(full_url)

            seen = set()
            unique_urls: List[str] = []
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    unique_urls.append(u)

            logger.info("Found %d article URLs at %s", len(unique_urls), self.base_url)
            logger.debug("Article URLs: %s", unique_urls)
            return unique_urls

        except Exception:
            logger.exception("Failed to fetch or parse article URLs from %s", self.base_url)
            return []

    def parse_article(self, html: Optional[str] = None) -> str:
        """
        Backwards-compatible wrapper: returns article text (string).
        Internally delegates to parse_article_to_object so the ObjectModel is created there.
        """
        obj = self.parse_article_to_object(self.base_url, html=html)
        return obj.text or ""

    def parse_article_to_object(
        self,
        url: str,
        html: Optional[str] = None,
        title: Optional[str] = None,
        teaser: Optional[str] = None,
    ) -> ObjectModel:
        """
        Parse an article and return a fully populated ObjectModel.

        If a content_hash can be computed from titel+teaser and is present in
        self.known_hashes, an ObjectModel with that content_hash is returned
        early so the caller can skip further processing/persistence.
        """
        try:
            if html is None:
                logger.debug("Fetching article URL for parsing: %s", url)
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                html = resp.text

            soup = BeautifulSoup(html, "html.parser")

            # Helper to extract title/teaser if not provided
            if not title:
                title_el = soup.find(
                    lambda tag: tag.name == "span"
                    and tag.get("class")
                    and any("headline" in c for c in tag.get("class"))
                )
                title = title_el.get_text(" ", strip=True) if title_el else None

            if not teaser:
                teaser_el = soup.find(
                    lambda tag: tag.name == "p"
                    and tag.get("class")
                    and any("typo-r-subline-detail" in c for c in tag.get("class"))
                )
                teaser = teaser_el.get_text(" ", strip=True) if teaser_el else None

            # If we can compute a content_hash from titel (+ teaser) and it's known, skip early
            if title:
                try:
                    h = hashlib.sha256()
                    h.update(title.encode("utf-8"))
                    h.update(b"\n")
                    teaser_bytes = teaser.encode("utf-8") if teaser else b""
                    h.update(teaser_bytes)
                    computed_hash = h.hexdigest()
                    if computed_hash in (self.known_hashes or set()):
                        logger.info("Skipping already-known article %s (content_hash=%s)", url, computed_hash)
                        # return minimal ObjectModel that carries the content_hash so caller can detect skip
                        return ObjectModel(
                            id=url,
                            html=html,
                            text="",
                            titel=title or "",
                            teaser=teaser or "",
                            parsed_date=datetime.utcnow(),
                            content_hash=computed_hash,
                        )
                except Exception:
                    logger.exception("Failed to compute early content_hash for %s", url)

            # Remove title and teaser nodes so they don't appear in fallbacks
            for el in soup.find_all(lambda tag: tag.name == "span" and tag.get("class") and "headline" in tag.get("class")):
                el.decompose()
            for el in soup.find_all(lambda tag: tag.name == "p" and tag.get("class") and "typo-r-subline-detail" in tag.get("class")):
                el.decompose()

            # 1) JSON-LD extraction
            text: str = ""
            for script in soup.find_all("script", type="application/ld+json"):
                raw = script.string or script.get_text()
                if not raw or not raw.strip():
                    continue
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    try:
                        cleaned = raw.strip()
                        data = json.loads(cleaned)
                    except Exception:
                        continue

                def _find_article_body(obj):
                    if isinstance(obj, list):
                        for item in obj:
                            res = _find_article_body(item)
                            if res:
                                return res
                    elif isinstance(obj, dict):
                        t = obj.get("@type") or obj.get("type")
                        if isinstance(t, str) and "NewsArticle" in t:
                            body = obj.get("articleBody")
                            if body:
                                return body
                        for v in obj.values():
                            if isinstance(v, (dict, list)):
                                res = _find_article_body(v)
                                if res:
                                    return res
                    return None

                article_body = _find_article_body(data)
                if article_body:
                    text = BeautifulSoup(article_body, "html.parser").get_text(separator=" ").strip()
                    text = re.sub(r"\s+", " ", text).strip()
                    text = re.sub(r"^([^\s])\s+([^\s])", r"\1\2", text, count=1)
                    logger.info("Extracted article body from JSON-LD (normalized to flowing text)")
                    break

            # 2) Fallback: semantic containers
            if not text:
                candidates = []
                article_tag = soup.find("article")
                if article_tag:
                    candidates.append(article_tag)

                selectors = [
                    "div.article__body",
                    "div.article-body",
                    "div#article",
                    "div.content",
                    "div.entry-content",
                    "main",
                ]
                for sel in selectors:
                    n = soup.select_one(sel)
                    if n:
                        candidates.append(n)

                for node in candidates:
                    ps = node.find_all("p")
                    if ps:
                        texts = [p.get_text(" ", strip=True) for p in ps if p.get_text(strip=True)]
                        if texts:
                            text = " ".join(texts)
                            text = re.sub(r"\s+", " ", text).strip()
                            text = re.sub(r"^([^\s])\s+([^\s])", r"\1\2", text, count=1)
                            break

            # 3) Final fallback: all paragraphs on page
            if not text:
                ps = soup.find_all("p")
                texts = [p.get_text(" ", strip=True) for p in ps if p.get_text(strip=True)]
                if texts:
                    text = " ".join(texts)
                    text = re.sub(r"\s+", " ", text).strip()
                    text = re.sub(r"^([^\s])\s+([^\s])", r"\1\2", text, count=1)

            if not text:
                logger.warning("No article text found for %s", url)
                text = ""

            parsed_date = datetime.utcnow()

            # Extrahiere published_date und autor aus dem HTML
            published_date_iso, author = _extract_meta_from_soup(soup)

            obj = ObjectModel(
                id=url,
                html=html,
                text=text,
                titel=title or "",
                teaser=teaser or "",
                parsed_date=parsed_date,
                published_date=published_date_iso,
                autor=author,
            )

            logger.info("Created ObjectModel for %s (text length: %d)", url, len(text))
            return obj

        except Exception:
            logger.exception("Failed to parse article content from %s", url)
            # return an empty/placeholder ObjectModel on error
            return ObjectModel(id=url, html=html, text="", titel=title or "", teaser=teaser or "", parsed_date=datetime.utcnow())
