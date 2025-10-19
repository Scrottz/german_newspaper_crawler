# python
from __future__ import annotations
from typing import List, Optional, Set, Tuple, Callable, Any
from urllib.parse import urljoin
import json
import re
from datetime import datetime
import hashlib

from bs4 import BeautifulSoup

from lib.common.logging import get_logger
from lib.common.object_model import ObjectModel
from lib.common.web_requests import get_html

logger = get_logger(__name__)


def _extract_meta_from_soup(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract published_date (ISO string), author (string) and category (string)
    from a BeautifulSoup-parsed article page.

    Search order:
    1) JSON-LD / structured data
    2) meta tags (name="author", property="article:author")
    3) TAZ-specific author containers (author-pic-name-wrapper, author-name-wrapper,
       img alt/title, span.tyop-name-detail-bold, text pattern 'von <author>')
    4) generic fallbacks (rel=author, itemprop, .byline, .author)

    Returns (published_iso, author, category).
    """
    published_iso: Optional[str] = None
    author: Optional[str] = None
    category: Optional[str] = None

    # published_date from <time datetime="...">
    time_tag = soup.find("time")
    if time_tag:
        dt_attr = (time_tag.get("datetime") or "").strip()
        if dt_attr:
            try:
                dt = datetime.fromisoformat(dt_attr)
                published_iso = dt.isoformat()
                logger.debug("Extracted published_date from time tag: %s", published_iso)
            except Exception:
                m = re.search(r"(\d{4}-\d{2}-\d{2})", dt_attr)
                if m:
                    published_iso = m.group(1)
                    logger.debug("Fallback extracted published_date: %s", published_iso)

    # category detection (TAZ styling heuristics)
    def _has_category_classes(c):
        if not c:
            return False
        if isinstance(c, str):
            classes = set(c.split())
        else:
            classes = set(c)
        needed = {"is-flex", "headline", "typo-r-topline-detail"}
        return needed.issubset(classes)

    category_tag = soup.find("span", class_=_has_category_classes)
    if category_tag:
        cat_txt = category_tag.get_text(" ", strip=True)
        if cat_txt:
            category = re.sub(r"\s+", " ", cat_txt).strip()
            logger.debug("Extracted category: %s", category)

    # --- JSON-LD / structured data author extraction ---
    def _find_author_in_json(obj):
        if isinstance(obj, list):
            for it in obj:
                res = _find_author_in_json(it)
                if res:
                    return res
        elif isinstance(obj, dict):
            for k in ("author", "creator"):
                if k in obj:
                    v = obj[k]
                    if isinstance(v, str) and v.strip():
                        return v.strip()
                    if isinstance(v, dict):
                        name = v.get("name") or v.get("givenName") or v.get("familyName")
                        if isinstance(name, str) and name.strip():
                            return name.strip()
                    if isinstance(v, list):
                        for entry in v:
                            if isinstance(entry, dict):
                                name = entry.get("name")
                                if isinstance(name, str) and name.strip():
                                    return name.strip()
                            elif isinstance(entry, str) and entry.strip():
                                return entry.strip()
            for val in obj.values():
                if isinstance(val, (dict, list)):
                    res = _find_author_in_json(val)
                    if res:
                        return res
        return None

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text()
        if not raw or not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            try:
                data = json.loads(raw.strip())
            except Exception:
                data = None
        if data is None:
            continue
        author_found = _find_author_in_json(data)
        if author_found:
            author = author_found
            logger.debug("Author extracted from JSON-LD: %s", author)
            break

    # --- meta tag fallbacks ---
    if not author:
        m = soup.find("meta", attrs={"name": "author"})
        if m and m.get("content"):
            author = m["content"].strip()
            logger.debug("Author extracted from meta name=author: %s", author)
    if not author:
        m = soup.find("meta", attrs={"property": "article:author"})
        if m and m.get("content"):
            author = m["content"].strip()
            logger.debug("Author extracted from meta property=article:author: %s", author)

    # --- TAZ-specific container heuristics ---
    if not author:
        author_container = soup.find(
            lambda tag: tag.name in ("div", "section")
            and tag.get("class")
            and any("author" in c for c in tag.get("class"))
        )
        if author_container:
            logger.debug("Found author container, applying TAZ heuristics")
            img = author_container.find("img")
            if img:
                alt = (img.get("alt") or img.get("title") or "").strip()
                if alt:
                    author = re.sub(r"\s+", " ", alt).strip()
                    logger.debug("Author from image alt/title: %s", author)

            if not author:
                for span in author_container.find_all("span"):
                    classes = span.get("class") or []
                    cls_str = " ".join(classes) if isinstance(classes, (list, tuple)) else str(classes)
                    if "typo-name-detail-bold" in cls_str:
                        txt = (span.get_text(" ", strip=True) or "").strip()
                        if txt and not re.search(r"Kolumne|Ernsthaft\?", txt, re.IGNORECASE):
                            author = re.sub(r"\s+", " ", txt).strip()
                            logger.debug("Author from strong span candidate: %s", author)
                            break

            if not author:
                texts = author_container.find_all(text=True)
                for i, t in enumerate(texts):
                    if isinstance(t, str) and re.search(r"\bvon\b", t, re.IGNORECASE):
                        parent = t.parent
                        next_candidate = parent.find_next(["a", "span"])
                        if next_candidate:
                            candidate = (next_candidate.get_text(" ", strip=True) or "").strip()
                            if candidate:
                                author = re.sub(r"\s+", " ", candidate).strip()
                                logger.debug("Author found after 'von' token: %s", author)
                                break

            if not author:
                a_tag = author_container.find("a", href=True)
                if a_tag:
                    atxt = (a_tag.get_text(" ", strip=True) or "").strip()
                    if atxt and not re.search(r"Kolumne|Ernsthaft\?", atxt, re.IGNORECASE):
                        author = re.sub(r"\s+", " ", atxt).strip()
                        logger.debug("Author from first <a> in container: %s", author)

    # --- generic fallbacks ---
    if not author:
        rels = soup.select("[rel=author], [itemprop~=author], .byline, .author, .autor")
        for el in rels:
            txt = (el.get_text(" ", strip=True) or "").strip()
            if txt and len(txt) < 100:
                author = re.sub(r"\s+", " ", txt).strip()
                logger.debug("Author from generic selector: %s", author)
                break

    # final cleanup: remove leading 'von' / 'by'
    if isinstance(author, str):
        author = re.sub(r"^\s*(von|by|von:)\s+", "", author, flags=re.IGNORECASE).strip()
        if not author:
            author = None

    return published_iso, author, category


class TAZ:
    """
    TAZ crawler helper.

    Provides methods to collect article URLs and to parse article text.
    Accepts an optional `known_hashes` set so already-seen articles can be skipped.
    A `fetcher` callable can be injected for HTTP access (default: lib.common.web_requests.get_html).
    """
    def __init__(
        self,
        base_url: str,
        known_hashes: Optional[Set[str]] = None,
        fetcher: Optional[Callable[..., str]] = None,
    ) -> None:
        self.base_url = base_url
        # allow injection via constructor or later via setattr(instance, "known_hashes", set)
        self.known_hashes: Set[str] = known_hashes or set()
        # fetcher should return HTML string given a URL; allow flexible signature
        self.fetcher: Callable[..., str] = fetcher or get_html
        logger.debug("TAZ initialized for base_url=%s known_hashes=%d", self.base_url, len(self.known_hashes))

    def fetch_article_urls(self, html: Optional[str] = None) -> List[str]:
        """
        Fetch the base page and return a list of absolute article URLs found as 'a.teaser-link'.
        Uses injected fetcher to retrieve HTML.
        """
        try:
            if html is None:
                logger.debug("Fetching base URL: %s", self.base_url)
                html = self.fetcher(self.base_url)

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
        Delegates to parse_article_to_object.
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

        The content_hash is computed solely from the URL (SHA256). If the hash is
        already present in self.known_hashes the method returns a minimal ObjectModel
        without performing the HTTP request or full parsing.
        """
        try:
            # Compute URL-based content_hash (only source of truth)
            try:
                h = hashlib.sha256()
                h.update(url.encode("utf-8"))
                url_hash = h.hexdigest()
                logger.debug("Computed url_hash=%s for %s", url_hash, url)
            except Exception:
                logger.exception("Failed to compute URL hash for %s", url)
                url_hash = None

            if url_hash and url_hash in (self.known_hashes or set()):
                logger.info("Skipping already-known article %s (url_hash=%s)", url, url_hash)
                return ObjectModel(
                    id=url,
                    html="",
                    text="",
                    titel=title or "",
                    teaser=teaser or "",
                    parsed_date=datetime.utcnow(),
                    content_hash=url_hash,
                )

            # proceed to fetch and parse
            if html is None:
                logger.debug("Fetching article URL for parsing: %s", url)
                html = self.fetcher(url)

            soup = BeautifulSoup(html, "html.parser")

            # Helper to extract title/teaser if not provided
            if not title:
                title_el = soup.find(
                    lambda tag: tag.name == "span"
                    and tag.get("class")
                    and any("headline" in c for c in tag.get("class"))
                )
                title = title_el.get_text(" ", strip=True) if title_el else None
                logger.debug("Extracted title: %s", title)

            if not teaser:
                teaser_el = soup.find(
                    lambda tag: tag.name == "p"
                    and tag.get("class")
                    and any("typo-r-subline-detail" in c for c in tag.get("class"))
                )
                teaser = teaser_el.get_text(" ", strip=True) if teaser_el else None
                logger.debug("Extracted teaser: %s", teaser)

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
                        data = None
                if data is None:
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
                            logger.debug("Extracted article from semantic container: selector/node")
                            break

            # 3) Final fallback: all paragraphs on page
            if not text:
                ps = soup.find_all("p")
                texts = [p.get_text(" ", strip=True) for p in ps if p.get_text(strip=True)]
                if texts:
                    text = " ".join(texts)
                    text = re.sub(r"\s+", " ", text).strip()
                    text = re.sub(r"^([^\s])\s+([^\s])", r"\1\2", text, count=1)
                    logger.debug("Extracted article from all paragraphs fallback")

            if not text:
                logger.warning("No article text found for %s", url)
                text = ""

            parsed_date = datetime.utcnow()

            # Extract published_date, author and category
            published_date_iso, author, category = _extract_meta_from_soup(soup)
            logger.debug("Meta extracted: published=%s author=%s category=%s", published_date_iso, author, category)

            obj = ObjectModel(
                id=url,
                html=html,
                text=text,
                titel=title or "",
                teaser=teaser or "",
                parsed_date=parsed_date,
                published_date=published_date_iso,
                autor=author,
                category=category,
                content_hash=url_hash,
            )
            logger.info("Created ObjectModel for %s (text length: %d)", url, len(text))
            return obj

        except Exception:
            logger.exception("Failed to parse article content from %s", url)
            try:
                h2 = hashlib.sha256()
                h2.update(url.encode("utf-8"))
                err_hash = h2.hexdigest()
            except Exception:
                err_hash = None
            return ObjectModel(id=url, html=html or "", text="", titel=title or "", teaser=teaser or "", parsed_date=datetime.utcnow(), content_hash=err_hash)
