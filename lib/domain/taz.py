# python
# File: `lib/domain/taz.py`
from __future__ import annotations
from typing import List, Optional, Set, Tuple, Callable, Any
from urllib.parse import urljoin
import json
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import re

from lib.common.logging import get_logger
from lib.common.object_model import ObjectModel
from lib.common.web_requests import fetch_url

logger = get_logger(__name__)


def _extract_meta_from_soup(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Extract metadata from article soup.

    Returns tuple: (published_iso, author, category, teaser)
    - published_iso: from <time datetime="..."> or meta[property="article:published_time"]
    - author: heuristics looking for a.teaser-link > span.typo-name-detail-bold or meta[name="author"]
    - category: robust extraction:
        * prefer the element that contains the visible category label (often a span with
          class containing 'typo-r-head-detail') found inside the same <h2> that contains
          a span with 'typo-r-topline-detail'
        * fallback to the text of the 'typo-r-topline-detail' span itself
    - teaser: paragraph with class containing 'typo-r-subline-detail' or meta[name="description"]
    """
    published_iso: Optional[str] = None
    author: Optional[str] = None
    category: Optional[str] = None
    teaser: Optional[str] = None

    # published_date from <time datetime="...">
    time_tag = soup.find("time")
    if time_tag:
        dt_attr = (time_tag.get("datetime") or "").strip()
        if dt_attr:
            published_iso = dt_attr

    if not published_iso:
        meta_time = soup.find("meta", {"property": "article:published_time"}) or soup.find("meta", {"name": "pubdate"})
        if meta_time and meta_time.get("content"):
            published_iso = meta_time.get("content").strip()

    # author from a.teaser-link > span.typo-name-detail-bold or meta[name=author]
    a_tag = soup.find("a", class_=lambda c: c and "teaser-link" in c)
    if a_tag:
        span = a_tag.find("span", class_=lambda c: c and "typo-name-detail-bold" in c)
        if span:
            txt = span.get_text(" ", strip=True)
            if txt:
                author = " ".join(txt.split())

    if not author:
        meta_author = soup.find("meta", {"name": "author"})
        if meta_author and meta_author.get("content"):
            author = meta_author.get("content").strip()

    # category: robust detection for structures like:
    # <h2 ...><span class="headline typo-r-topline-detail">CATEGORY</span><span class="is-hidden">: </span><span class="is-flex headline typo-r-head-detail ...">Actual Category</span></h2>
    def _class_contains(fragment: str):
        def checker(c):
            if not c:
                return False
            if isinstance(c, str):
                return fragment in c
            if isinstance(c, (list, tuple)):
                return any(fragment in cl for cl in c if isinstance(cl, str))
            return False
        return checker

    # find the 'topline' marker span (either class string or list)
    cat_marker = soup.find(lambda tag: tag.name in ("span", "div") and _class_contains("typo-r-topline-detail")(tag.get("class")))
    candidate_text = None
    if cat_marker:
        # Try to find the visible category inside the same parent h2 (preferred)
        parent_h2 = cat_marker.find_parent("h2")
        if parent_h2:
            head_span = parent_h2.find(lambda tag: tag.name == "span" and _class_contains("typo-r-head-detail")(tag.get("class")))
            if not head_span:
                # fallback to variant class names that might contain 'typo-r-head'
                head_span = parent_h2.find(lambda tag: tag.name == "span" and _class_contains("typo-r-head")(tag.get("class")))
            if head_span:
                candidate_text = head_span.get_text(" ", strip=True)

        # If not found in parent h2, look for a next sibling span with the head class
        if candidate_text is None:
            next_span = cat_marker.find_next_sibling(lambda tag: tag.name == "span" and _class_contains("typo-r-head-detail")(tag.get("class")))
            if not next_span:
                next_span = cat_marker.find_next_sibling(lambda tag: tag.name == "span" and _class_contains("typo-r-head")(tag.get("class")))
            if next_span:
                candidate_text = next_span.get_text(" ", strip=True)

        # If still nothing, fall back to the marker's own text
        if candidate_text is None:
            mtxt = cat_marker.get_text(" ", strip=True)
            if mtxt:
                candidate_text = mtxt

    if candidate_text:
        category = " ".join(candidate_text.split())

    # teaser: look for paragraph with class containing 'typo-r-subline-detail'
    p_tag = soup.find("p", class_=lambda c: isinstance(c, str) and "typo-r-subline-detail" in c)
    if not p_tag:
        # sometimes classes are a list
        p_tag = soup.find(lambda tag: tag.name == "p" and isinstance(tag.get("class"), list) and any("typo-r-subline-detail" in cl for cl in tag.get("class")))
    if p_tag:
        t = p_tag.get_text(" ", strip=True)
        if t:
            teaser = " ".join(t.split())

    if not teaser:
        # fallback to meta description
        meta_desc = soup.find("meta", {"name": "description"})
        if meta_desc and meta_desc.get("content"):
            teaser = meta_desc.get("content").strip()
    return published_iso, author, category, teaser


class TAZ:
    """
    TAZ crawler helper using BeautifulSoup exclusively.
    """

    def __init__(
        self,
        base_url: str,
        known_hashes: Optional[Set[str]] = None,
        fetcher: Optional[Callable[..., str]] = None,
    ) -> None:
        # Initialize instance state
        self.base_url = base_url
        self.known_hashes: Set[str] = known_hashes or set()
        # fetcher should be a callable(url) -> str returning HTML; default is a no-op returning empty string
        self.fetcher: Callable[..., str] = fetcher or (lambda u, **kw: "")
        logger.debug("TAZ initialized for base_url=%s known_hashes=%d", self.base_url, len(self.known_hashes))

    def fetch_article_urls(self, html: Optional[str] = None) -> List[str]:
        """
        Fetch the listing page (if html not provided) and extract article URLs.
        Uses several selectors and a fallback fetcher if the injected fetcher returns empty.
        Returns a list of absolute http(s) URLs.
        """
        try:
            # ensure we have HTML (try injected fetcher first)
            if html is None:
                try:
                    html = self.fetcher(self.base_url)
                except TypeError:
                    html = self.fetcher()  # some test fetchers ignore args
                except Exception:
                    logger.exception("fetch_article_urls: injected fetcher failed for %s", self.base_url)
                    html = ""

            # fallback to common fetch utility if injected fetcher returned falsy result
            if not html:
                try:
                    html = fetch_url(self.base_url)
                except Exception:
                    logger.exception("fetch_article_urls: fallback fetch_url failed for %s", self.base_url)
                    html = ""

            soup = BeautifulSoup(html or "", "html.parser")

            # Try several strategies to find article anchors
            selectors = [
                "a.teaser-link",
                "a.headline-link",
                "a.article__link",
                "a[href*='/artikel/']",
                "a[href^='/']",
                "a[href^='http']",
            ]

            anchors = []
            for sel in selectors:
                for a in soup.select(sel):
                    href = a.get("href")
                    if href:
                        anchors.append(href)

            # As a last resort, collect anchors inside article lists
            if not anchors:
                for a in soup.find_all("a", href=True):
                    anchors.append(a["href"])

            # Deduplicate preserving order and make absolute
            urls: List[str] = []
            seen = set()
            for href in anchors:
                # ignore javascript/mailto/#
                if not isinstance(href, str):
                    continue
                href = href.strip()
                if not href or href.startswith("javascript:") or href.startswith("mailto:") or href == "#":
                    continue
                abs_url = urljoin(self.base_url, href)
                if abs_url not in seen:
                    seen.add(abs_url)
                    urls.append(abs_url)

            logger.info("Found %d article URLs at %s", len(urls), self.base_url)
            logger.debug("Article URLs: %s", urls)
            return urls

        except Exception:
            logger.exception("Failed to fetch or parse article URLs from %s", self.base_url)
            return []

    def parse_article(self, html: Optional[str] = None) -> str:
        """
        Convenience wrapper that returns article text only.
        """
        obj = self.parse_article_to_object(self.base_url, html=html)
        return obj.text or ""

    def _extract_body_text(self, soup: BeautifulSoup) -> str:
        """
        Extract article body text using common article container selectors.
        """
        # common article containers
        candidates = [
            "article",
            "div.article__body",
            "div.article-body",
            "div.article__content",
            "div#article",
            "div[itemprop='articleBody']",
            "main",
            "div[class*='article']",
        ]
        paragraphs: List[str] = []

        for sel in candidates:
            tag = soup.select_one(sel)
            if not tag:
                # more tolerant search: find any div/article with many <p> children
                continue
            ps = tag.find_all("p")
            for p in ps:
                txt = p.get_text(" ", strip=True)
                if txt:
                    paragraphs.append(" ".join(txt.split()))
            if paragraphs:
                return "\n\n".join(paragraphs)

        # fallback: collect all top-level paragraphs
        all_ps = soup.find_all("p")
        for p in all_ps:
            txt = p.get_text(" ", strip=True)
            if txt:
                paragraphs.append(" ".join(txt.split()))
        return "\n\n".join(paragraphs)

    def parse_article_to_object(
        self,
        url: str,
        html: Optional[str] = None,
        title: Optional[str] = None,
        teaser: Optional[str] = None,
    ) -> ObjectModel:
        """
        Parse a single article URL/html into an ObjectModel.
        Minimal, DOM-only heuristics; relies on `_extract_meta_from_soup` for meta.
        """
        try:
            if html is None:
                try:
                    html = self.fetcher(url)
                except TypeError:
                    html = self.fetcher()
                except Exception:
                    logger.exception("parse_article_to_object: injected fetcher failed for %s", url)
                    html = ""

            if not html:
                try:
                    html = fetch_url(url)
                except Exception:
                    logger.exception("parse_article_to_object: fallback fetch_url failed for %s", url)
                    html = ""

            soup = BeautifulSoup(html or "", "html.parser")

            # Title: prefer <h1>, else meta og:title
            title_text = title
            if not title_text:
                h1 = soup.find("h1")
                if h1:
                    title_text = h1.get_text(" ", strip=True)
                else:
                    meta_title = soup.find("meta", {"property": "og:title"}) or soup.find("meta", {"name": "title"})
                    if meta_title and meta_title.get("content"):
                        title_text = meta_title.get("content").strip()

            # Extract meta: published_iso (string), author, category, teaser
            published_iso, author, category_extracted, teaser_extracted = _extract_meta_from_soup(soup)
            if teaser is None:
                teaser = teaser_extracted

            # Body text
            body_text = self._extract_body_text(soup)

            # Parse published_iso into datetime if present
            published_dt: Optional[datetime] = None
            if published_iso:
                try:
                    iso = published_iso.strip()
                    if iso.endswith("Z"):
                        iso = iso[:-1] + "+00:00"
                    published_dt = datetime.fromisoformat(iso)
                except Exception:
                    # try common fallback patterns
                    try:
                        published_dt = datetime.strptime(published_iso, "%Y-%m-%dT%H:%M:%S")
                    except Exception:
                        logger.debug("parse_article_to_object: could not parse published_iso=%r for url=%s", published_iso, url)
                        published_dt = None

            obj = ObjectModel(
                _id=None,
                url=url,
                titel=title_text,
                teaser=teaser,
                autor=author,
                category=category_extracted,
                published_date=published_dt,
                parsed_date=datetime.utcnow(),
                html=html,
                text=body_text,
                ai_keywords=None,
                pos_taggs=None,
                content_hash=None,
            )

            logger.info("Parsed article %s titel=%s autor=%s category=%s teaser_present=%s text_len=%d",
                        url,
                        obj.titel,
                        obj.autor,
                        obj.category,
                        bool(obj.teaser),
                        len(obj.text or ""))

            return obj

        except Exception:
            logger.exception("parse_article_to_object: failed to parse article %s", url)
            # Return a minimal ObjectModel to avoid breaking callers
            fallback_text = re.sub(r"<[^>]+>", " ", html or "")
            return ObjectModel(url=url, html=html, text=fallback_text)


def get_article_urls(domain_cfg: dict) -> List[str]:
    """
    Adapter for the crawler framework. Expects a domain_cfg dict with one of:
    - 'base_url' or 'url' or 'name' providing the site root.
    Optional keys:
    - 'fetcher': callable(url) -> str (HTML) to inject http client for tests.
    - 'known_hashes': iterable of known content hashes to avoid duplicates.
    Returns a list of article URLs (possibly empty).
    """
    base_url = domain_cfg.get("base_url") or domain_cfg.get("url") or domain_cfg.get("name")
    if not base_url:
        logger.debug("get_article_urls: missing base_url/url in domain_cfg for %s", domain_cfg.get("name"))
        return []

    fetcher_candidate = domain_cfg.get("fetcher")
    fetcher = fetcher_candidate if callable(fetcher_candidate) else None

    known = domain_cfg.get("known_hashes")
    known_set = set(known) if isinstance(known, (list, set)) else None

    taz = TAZ(base_url=base_url, known_hashes=known_set, fetcher=fetcher)
    try:
        return taz.fetch_article_urls()
    except Exception:
        logger.exception("get_article_urls: failed to fetch article URLs for %s", base_url)
        return []


def parse_article(url: str, html: Optional[str] = None) -> ObjectModel:
    """
    Module-level parse_article wrapper expected by the crawler.
    Returns an ObjectModel for the given url/html so that process_domain_generic
    can use author/published_date/category produced by the domain parser.
    """
    taz = TAZ(base_url=url, fetcher=fetch_url)
    return taz.parse_article_to_object(url, html=html)
