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

    - published_iso: from <time datetime="..."> or meta[property="article:published_time"] or meta[name="pubdate"]
    - author: looks for the structure described:
        <div class="typo-name-detail pr-xsmall  author-name-wrapper"> ... <a class="teaser-link"> <span class="typo-name-detail-bold">AUTHOR</span>
      falls back to meta[name="author"]
    - category: looks for <span class="headline typo-r-topline-detail">CATEGORY</span>
      und versucht, die sichtbare Kategorie in der gleichen H2/Elternstruktur zu finden
    - teaser: paragraph with class containing 'typo-r-subline-detail' (auch wenn mehrere Klassen vorhanden)
      falls nicht vorhanden, fallback meta[name="description"]
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

    # author extraction: prefer the specific TAZ structure
    # <div class="typo-name-detail pr-xsmall  author-name-wrapper"> ... <a class="teaser-link"> <span class="typo-name-detail-bold">AUTHOR</span>
    try:
        author_wrapper = soup.find("div", class_=lambda c: isinstance(c, str) and "author-name-wrapper" in c or (
                    isinstance(c, list) and any("author-name-wrapper" in cl for cl in c)))
        if author_wrapper:
            # find anchor with class teaser-link inside
            a_tag = author_wrapper.find("a", class_=lambda c: isinstance(c, str) and "teaser-link" in c or (
                        isinstance(c, list) and any("teaser-link" in cl for cl in c)))
            if a_tag:
                span_bold = a_tag.find("span", class_=lambda c: isinstance(c, str) and "typo-name-detail-bold" in c or (
                            isinstance(c, list) and any("typo-name-detail-bold" in cl for cl in c)))
                if span_bold:
                    txt = span_bold.get_text(" ", strip=True)
                    if txt:
                        author = " ".join(txt.split())
    except Exception:
        logger.exception("_extract_meta_from_soup: author extraction failed")

    if not author:
        meta_author = soup.find("meta", {"name": "author"})
        if meta_author and meta_author.get("content"):
            author = meta_author.get("content").strip()

    # category extraction:
    # find <span class="headline typo-r-topline-detail">CATEGORY</span>
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

    cat_marker = soup.find(
        lambda tag: tag.name in ("span", "div") and _class_contains("typo-r-topline-detail")(tag.get("class")))
    candidate_text = None
    if cat_marker:
        # try to get visible category in same parent h2
        parent_h2 = cat_marker.find_parent("h2")
        if parent_h2:
            head_span = parent_h2.find(
                lambda tag: tag.name == "span" and _class_contains("typo-r-head-detail")(tag.get("class")))
            if head_span:
                t = head_span.get_text(" ", strip=True)
                if t:
                    candidate_text = t

        # fallback to next sibling span with head class
        if candidate_text is None:
            next_span = cat_marker.find_next_sibling(
                lambda tag: tag.name == "span" and _class_contains("typo-r-head-detail")(tag.get("class")))
            if next_span:
                t = next_span.get_text(" ", strip=True)
                if t:
                    candidate_text = t

        # fallback to marker's own text (the CATEGORY label)
        if candidate_text is None:
            mtxt = cat_marker.get_text(" ", strip=True)
            if mtxt:
                candidate_text = mtxt

    if candidate_text:
        category = " ".join(candidate_text.split())

    # teaser extraction: paragraph with class containing 'typo-r-subline-detail'
    try:
        p_tag = soup.find("p", class_=lambda c: isinstance(c, str) and "typo-r-subline-detail" in c or (
                    isinstance(c, list) and any("typo-r-subline-detail" in cl for cl in c)))
        if not p_tag:
            # sometimes classes are a list or different structure; search more generally
            p_tag = soup.find(lambda tag: tag.name == "p" and (isinstance(tag.get("class"), (list, tuple)) and any(
                "typo-r-subline-detail" in cl for cl in tag.get("class"))))
        if p_tag:
            t = p_tag.get_text(" ", strip=True)
            if t:
                teaser = " ".join(t.split())
    except Exception:
        logger.exception("_extract_meta_from_soup: teaser extraction failed")

    if not teaser:
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
            fetcher: Optional[Callable[[str], str]] = None,
    ) -> None:
        self.base_url = base_url
        self.known_hashes = set(known_hashes) if known_hashes is not None else set()
        self.fetcher = fetcher if callable(fetcher) else fetch_url

    def fetch_article_urls(self, html: Optional[str] = None) -> List[str]:
        """
        Fetch the listing page (if html not provided) and extract article URLs.
        Skip URLs whose url-hash or anchor-text-hash is already present in self.known_hashes
        to avoid downloading the article page later.
        Returns a list of absolute http(s) URLs.
        """
        try:
            # ensure we have HTML (try injected fetcher first)
            if html is None:
                try:
                    html = self.fetcher(self.base_url)
                except TypeError:
                    html = self.fetcher()
                except Exception:
                    logger.exception("fetch_article_urls: fetcher failed for %s", self.base_url)
                    html = ""

            if not html:
                # fetcher may return falsy; try secondary fetch_url if our injected fetcher is not working
                try:
                    html = fetch_url(self.base_url)
                except Exception:
                    logger.exception("fetch_article_urls: fallback fetch_url failed for %s", self.base_url)
                    html = ""

            soup = BeautifulSoup(html or "", "html.parser")

            # selectors likely containing article links
            selectors = [
                "a.teaser-link",
                "a.headline-link",
                "a.article__link",
                "a[href*='/artikel/']",
                "a[href^='/']",
                "a[href^='http']",
            ]

            anchors: List[Tuple[str, str]] = []
            for sel in selectors:
                for a in soup.select(sel):
                    href = a.get("href")
                    if href:
                        anchors.append((href, a.get_text(" ", strip=True) or ""))

            if not anchors:
                for a in soup.find_all("a", href=True):
                    anchors.append((a["href"], a.get_text(" ", strip=True) or ""))

            urls: List[str] = []
            seen = set()
            for href, anchor_text in anchors:
                if not isinstance(href, str):
                    continue
                href = href.strip()
                if not href or href.startswith("javascript:") or href.startswith("mailto:") or href == "#":
                    continue
                abs_url = urljoin(self.base_url, href)
                if abs_url in seen:
                    continue

                # compute url-hash and optional anchor-text-hash and compare to known_hashes
                skip = False
                try:
                    h_url = hashlib.sha256()
                    h_url.update(abs_url.encode("utf-8"))
                    url_hash = h_url.hexdigest()
                    if self.known_hashes and url_hash in self.known_hashes:
                        logger.debug("Skipping known url (url_hash) %s", abs_url)
                        skip = True
                except Exception:
                    logger.debug("Failed to compute url_hash for %s", abs_url)

                if not skip and anchor_text:
                    try:
                        h_txt = hashlib.sha256()
                        h_txt.update(anchor_text.strip().encode("utf-8"))
                        text_hash = h_txt.hexdigest()
                        if self.known_hashes and text_hash in self.known_hashes:
                            logger.debug("Skipping known url by anchor text (text_hash) %s", abs_url)
                            skip = True
                    except Exception:
                        logger.debug("Failed to compute text_hash for anchor %s", anchor_text[:50])

                if skip:
                    continue

                seen.add(abs_url)
                urls.append(abs_url)

            logger.info("Found %d article URLs at %s (after skipping known)", len(urls), self.base_url)
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
        candidates = [
            "div.article__body",
            "div.article__content",
            "div.lead-body",
            "div.article-content",
            "section.article-body",
            "div.story-body",
            "div#content",
            "article",
            "div.teaser-body",
        ]
        paragraphs: List[str] = []

        for sel in candidates:
            container = soup.select_one(sel)
            if container:
                # gather paragraphs inside container
                for p in container.find_all("p"):
                    txt = p.get_text(" ", strip=True)
                    if txt:
                        paragraphs.append(" ".join(txt.split()))
                if paragraphs:
                    break

        # fallback: collect all top-level paragraphs if nothing found
        if not paragraphs:
            all_ps = soup.find_all("p")
            for p in all_ps:
                txt = p.get_text(" ", strip=True)
                if txt:
                    paragraphs.append(" ".join(txt.split()))
        return "\n\n".join(paragraphs)

    # python
    def parse_article_to_object(
            self,
            url: str,
            html: Optional[str] = None,
    ) -> ObjectModel:
        """
        Parse an article URL (or provided HTML) and return an ObjectModel.
        Sets `parsed_date` to current UTC time when parsing yields non-empty text.
        """
        try:
            # fetch html if not provided
            if html is None:
                try:
                    html = self.fetcher(url)
                except Exception:
                    logger.exception("parse_article_to_object: fetch failed for %s", url)
                    html = ""

            soup = BeautifulSoup(html or "", "html.parser")

            # extract metadata
            published_iso, author, category, teaser = _extract_meta_from_soup(soup)

            # try to parse published date (lenient)
            published_dt = None
            if published_iso:
                try:
                    published_dt = datetime.fromisoformat(published_iso)
                except Exception:
                    try:
                        # fallback simple ISO-ish cleanup
                        cleaned = published_iso.strip()
                        published_dt = datetime.fromisoformat(cleaned)
                    except Exception:
                        logger.debug("parse_article_to_object: could not parse published_iso=%r for %s", published_iso,
                                     url)
                        published_dt = None

            # extract body text
            body_text = self._extract_body_text(soup)

            # construct ObjectModel with extracted fields
            obj = ObjectModel(
                url=url,
                titel=None,
                teaser=teaser,
                autor=author,
                category=category,
                published_date=published_dt,
                html=html,
                text=body_text,
            )

            # set parsed_date when parsing produced actual text
            if obj.text:
                try:
                    obj.parsed_date = datetime.utcnow()
                except Exception:
                    logger.exception("parse_article_to_object: failed to set parsed_date for %s", url)

            return obj

        except Exception:
            logger.exception("parse_article_to_object: unexpected failure for %s", url)
            # return minimal ObjectModel on failure
            return ObjectModel(url=url, html=html or "", text="")


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
