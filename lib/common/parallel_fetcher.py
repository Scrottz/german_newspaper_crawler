# python
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, List, Dict, Optional, Any
from urllib.parse import urlsplit, unquote
import os
import re
import threading
import requests
from requests import Session
from lib.common.logging import get_logger
from tqdm import tqdm

logger = get_logger(__name__)

# Erlaube direkte Download-Extensions (ersetze/erweitere bei Bedarf)
_ALLOWED_EXTENSIONS = {".pdf", ".zip", ".jpg", ".jpeg", ".png"}


def _sanitize_filename(name: str) -> str:
    name = unquote(name)
    # entferne unsichere Zeichen
    name = re.sub(r"[^\w\-\._() ]+", "_", name)
    return name[:200]


def _choose_filename_from_response(url: str, resp: Optional[requests.Response]) -> str:
    # 1) Content-Disposition
    if resp is not None:
        cd = resp.headers.get("content-disposition", "")
        m = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)', cd, flags=re.IGNORECASE)
        if m:
            return _sanitize_filename(m.group(1))

    # 2) from URL path
    path = urlsplit(url).path or ""
    base = os.path.basename(path)
    if base:
        return _sanitize_filename(base)

    # 3) fallback
    return _sanitize_filename("downloaded_file")


_filename_lock = threading.Lock()


def _ensure_unique_path(dest_dir: str, name: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    base, ext = os.path.splitext(name)
    attempt = 0
    with _filename_lock:
        candidate = name
        while os.path.exists(os.path.join(dest_dir, candidate)):
            attempt += 1
            candidate = f"{base}_{attempt}{ext}"
        return os.path.join(dest_dir, candidate)


def _default_filter(url: str) -> bool:
    """
    Gibt True zurück, wenn die URL heruntergeladen werden soll.
    Verwirft direkte Dateien mit Extensions aus `_ALLOWED_EXTENSIONS`.
    """
    path = urlsplit(url).path or ""
    ext = os.path.splitext(path)[1].lower()
    if ext and ext in _ALLOWED_EXTENSIONS:
        return True  # erlauben, falls diese Extensions gewünscht sind
    # keine Extension -> wahrscheinlich eine HTML-Seite -> erlauben
    return True


def _download_worker(session: Session, url: str, dest_dir: str, timeout: int = 30) -> Dict[str, Any]:
    try:
        resp = session.get(url, stream=True, timeout=timeout)
        resp.raise_for_status()

        filename = _choose_filename_from_response(url, resp)
        out_path = _ensure_unique_path(dest_dir, filename)

        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return {"url": url, "success": True, "path": out_path, "status_code": resp.status_code}
    except Exception as e:
        logger.debug("Download failed for %s: %s", url, e)
        return {"url": url, "success": False, "error": str(e)}


def download_urls(urls: Iterable[str],
                  dest_dir: str,
                  max_workers: int = 6,
                  show_progress: bool = False,
                  filter_fn: Optional[Callable[[str], bool]] = None,
                  session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """
    Lädt die gegebenen URLs parallel in dest_dir herunter.
    - filter_fn(url) -> bool kann genutzt werden, um URLs zu überspringen.
    - Gibt eine Liste von Ergebnis-Dictionaries zurück.
    """
    filter_fn = filter_fn or _default_filter
    urls = [u for u in urls if u and filter_fn(u)]
    if not urls:
        return []

    sess_provided = session is not None
    session = session or requests.Session()

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as exe:
        futures = {exe.submit(_download_worker, session, u, dest_dir): u for u in urls}
        progress = tqdm(total=len(futures), desc="downloads", unit="file") if show_progress else None
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            if progress:
                progress.update(1)
        if progress:
            progress.close()

    if not sess_provided:
        try:
            session.close()
        except Exception:
            logger.debug("Session close failed")

    return results
