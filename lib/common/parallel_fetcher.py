# python
# File: `lib/common/parallel_fetcher.py`

from __future__ import annotations
from typing import List, Dict, Optional, Callable, Any, Iterable
import re
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from lib.common.logging import get_logger
from lib.common.web_requests import fetch_url
from lib.common.object_model import ObjectModel

logger = get_logger(__name__)


def _coerce_to_objectmodel(result: Any, url: str, html: Optional[str]) -> Optional[ObjectModel]:
    """
    Coerce a fetcher result into an ObjectModel.
    Accepts ObjectModel, dict or raw HTML text.
    """
    if isinstance(result, ObjectModel):
        return result
    if isinstance(result, dict):
        try:
            return ObjectModel.from_dict(result)
        except Exception:
            logger.exception("Failed to convert dict to ObjectModel for %s", url)
            return None
    content = html if html is not None else (result if isinstance(result, str) else "")
    text = re.sub(r"<[^>]+>", " ", content or "")
    try:
        return ObjectModel(url=url, html=content, text=text)
    except Exception:
        logger.exception("Failed to build ObjectModel for %s", url)
        return None


def fetch_urls_in_parallel(
    urls: Iterable[str],
    fetcher: Callable[..., str] = fetch_url,
    max_workers: int = 8,
    show_progress: bool = True,
    timeout: int = 15,
    headers: Optional[Dict[str, str]] = None,
) -> List[ObjectModel]:
    """
    Fetch a list of URLs in parallel and return a list of ObjectModel.
    If show_progress is True, display a tqdm progressbar independent of logger level.
    """
    urls_list = list(urls or [])
    if not urls_list:
        return []

    results: List[ObjectModel] = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = [exe.submit(fetcher, url, timeout=timeout, headers=headers) for url in urls_list]
        future_to_url = {f: url for f, url in zip(futures, urls_list)}

        iterator = as_completed(future_to_url)
        tq = None
        try:
            if show_progress:
                tq = tqdm(total=len(future_to_url), desc="Downloading", unit="url", leave=True, file=sys.stderr, disable=False, dynamic_ncols=True, mininterval=0.1)

            for fut in iterator:
                url = future_to_url.get(fut)
                try:
                    html = fut.result()
                except Exception:
                    logger.exception("fetch_urls_in_parallel: fetch failed for %s", url)
                    html = ""
                obj = _coerce_to_objectmodel(html, url, html)
                if obj is not None:
                    results.append(obj)
                if tq is not None:
                    tq.update(1)
        finally:
            if tq is not None:
                tq.close()

    return results


def process_urls_parallel(
    urls: Iterable[str],
    parse_fn: Optional[Callable[[str, Optional[str]], ObjectModel]] = None,
    fetcher: Callable[..., str] = fetch_url,
    max_workers: int = 8,
    show_progress: bool = True,
    timeout: int = 15,
    headers: Optional[Dict[str, str]] = None,
) -> List[ObjectModel]:
    """
    Fetch and optionally parse URLs in parallel.
    - If parse_fn is provided it is called as parse_fn(url, html) or parse_fn(url) (flexible).
    - show_progress controls tqdm display.
    """
    urls_list = list(urls or [])
    if not urls_list:
        return []

    results: List[ObjectModel] = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = [exe.submit(fetcher, url, timeout=timeout, headers=headers) for url in urls_list]
        future_to_url = {f: url for f, url in zip(futures, urls_list)}

        iterator = as_completed(future_to_url)
        tq = None
        try:
            if show_progress:
                tq = tqdm(total=len(future_to_url), desc="Downloading", unit="url", leave=True, file=sys.stderr, disable=False, dynamic_ncols=True, mininterval=0.1)

            for fut in iterator:
                url = future_to_url.get(fut)
                try:
                    html = fut.result()
                except Exception:
                    logger.exception("process_urls_parallel: fetch failed for %s", url)
                    html = ""

                obj: Optional[ObjectModel] = None
                if parse_fn is not None:
                    try:
                        try:
                            obj = parse_fn(url, html)
                        except TypeError:
                            obj = parse_fn(url)
                    except Exception:
                        logger.exception("process_urls_parallel: parse_fn failed for %s", url)
                        obj = _coerce_to_objectmodel(html, url, html)
                else:
                    obj = _coerce_to_objectmodel(html, url, html)

                if obj is not None:
                    results.append(obj)

                if tq is not None:
                    tq.update(1)
        finally:
            if tq is not None:
                tq.close()

    return results
