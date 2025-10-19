from __future__ import annotations
import os
import hashlib
import logging
import threading
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import tempfile
from urllib.parse import urlparse, unquote
import requests
from tqdm import tqdm
import sys

from lib.common.logging import get_logger

logger = get_logger(__name__)

# Lock used to protect mutation of known_hashes when provided
_known_hashes_lock = threading.Lock()


def _sha256_hex(s: str) -> str:
    h = hashlib.sha256()
    h.update(s.encode("utf-8"))
    return h.hexdigest()


def _compute_file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_filename_from_url(url: str, url_hash: str) -> str:
    """
    Erzeuge einen stabilen, kurzen Dateinamen basierend auf der URL und deren Hash.
    Bevorzugt wird der letzte Pfadbestandteil der URL; falls leer/ungültig, wird der Hash verwendet.
    """
    try:
        p = urlparse(url)
        name = os.path.basename(unquote(p.path)) or ""
        name = name.strip()
        # remove query fragments etc. and unsafe chars
        if name:
            # limit length and replace spaces
            name = name.split("?")[0].split("#")[0]
            # sanitize
            name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)
            if 1 <= len(name) <= 200:
                return f"_{url_hash[:12]}_{name}"
    except Exception:
        pass
    return f"_{url_hash}"


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        logger.exception("Failed to create directory %s", path)


def _download_single(
    url: str,
    dest_dir: str,
    timeout: int = 30,
    known_hashes: Optional[Set[str]] = None,
    progress_position: Optional[int] = None,
    show_progress: bool = True,
) -> Dict[str, Any]:
    """
    Lädt eine URL herunter und speichert sie in dest_dir.
    - Wenn known_hashes übergeben und der URL-Hash bereits vorhanden ist, wird übersprungen.
    - Fügt neue URL-/Content-Hashes thread-sicher zu known_hashes hinzu (falls gesetzt).
    - Wenn show_progress True, wird eine per-file tqdm-Bar (in Bytes) angezeigt.
    Rückgabe: dict mit keys: url, success(bool), path(optional), url_hash, content_hash(optional), error(optional), skipped(bool)
    """
    result: Dict[str, Any] = {"url": url, "success": False, "path": None, "url_hash": None, "content_hash": None, "error": None, "skipped": False}
    url_hash = None
    pbar = None
    try:
        url_hash = _sha256_hex(url)
        result["url_hash"] = url_hash

        # if caller provided known_hashes, check early by url_hash
        if known_hashes is not None:
            with _known_hashes_lock:
                if url_hash in known_hashes:
                    result["skipped"] = True
                    result["success"] = True
                    logger.debug("Skipping download for known URL hash %s: %s", url_hash, url)
                    return result

        # stream download to temp file
        resp = requests.get(url, stream=True, timeout=timeout)
        resp.raise_for_status()

        # prepare progress bar if requested
        if show_progress:
            # try to get content-length
            total_bytes = None
            try:
                cl = resp.headers.get("Content-Length")
                if cl is not None:
                    total_bytes = int(cl)
            except Exception:
                total_bytes = None

            desc = _safe_filename_from_url(url, url_hash)
            try:
                pbar = tqdm(
                    total=total_bytes,
                    desc=desc,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=progress_position,
                    leave=True,
                    file=sys.stderr,
                )
            except Exception:
                pbar = None

        _ensure_dir(dest_dir)
        # write to temp file first
        fd, tmp_path = tempfile.mkstemp(prefix="dl_", dir=dest_dir)
        os.close(fd)
        try:
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
                        if pbar:
                            try:
                                pbar.update(len(chunk))
                            except Exception:
                                pass

            # compute content hash of the downloaded file
            content_hash = _compute_file_sha256(tmp_path)
            result["content_hash"] = content_hash

            # decide final filename
            fname = _safe_filename_from_url(url, url_hash)
            final_path = os.path.join(dest_dir, fname)
            # avoid overwriting by appending short content hash if collision
            if os.path.exists(final_path):
                existing_hash = _compute_file_sha256(final_path)
                if existing_hash != content_hash:
                    final_path = f"{final_path}_{content_hash[:12]}"

            # move temp file to final location
            shutil.move(tmp_path, final_path)
            result["path"] = final_path
            result["success"] = True

            # update known_hashes with url_hash and content_hash if provided
            if known_hashes is not None:
                with _known_hashes_lock:
                    known_hashes.add(url_hash)
                    if result["content_hash"]:
                        known_hashes.add(result["content_hash"])

            logger.info("Downloaded %s -> %s (url_hash=%s)", url, final_path, url_hash)
            return result
        finally:
            # cleanup temp if still present
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            if pbar:
                try:
                    pbar.close()
                except Exception:
                    pass

    except requests.RequestException as e:
        logger.debug("Request error downloading %s: %s", url, e)
        result["error"] = str(e)
    except Exception as e:
        logger.exception("Unexpected error downloading %s", url)
        result["error"] = str(e)

    if pbar:
        try:
            pbar.close()
        except Exception:
            pass

    return result


def download_urls(urls: List[str], dest_dir: str, max_workers: int = 6, show_progress: bool = True, known_hashes: Optional[Set[str]] = None) -> List[Dict[str, Any]]:
    """
    Paralleler Download von URLs in das Verzeichnis dest_dir.
    - known_hashes: optionales Set\[str\] (url- oder content-hashes), das genutzt wird, um bereits bekannte Inhalte/URLs zu überspringen; das Set wird bei neuen Downloads aktualisiert (thread-sicher).
    - show_progress: wenn True, wird pro Datei eine tqdm-Progressbar angezeigt (bytes) sowie eine Gesamtbar für Dateien.
    - Rückgabe: Liste von Ergebnis-Dicts (Reihenfolge nicht garantiert).
    """
    results: List[Dict[str, Any]] = []
    if not urls:
        return results

    _ensure_dir(dest_dir)

    overall_pbar = None
    if show_progress:
        try:
            overall_pbar = tqdm(total=len(urls), desc="files", unit="file", position=0, leave=True, file=sys.stderr)
        except Exception:
            overall_pbar = None

    # use ThreadPoolExecutor for IO-bound downloads
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as exe:
        # pass an index/position to each worker so tqdm bars get distinct positions
        futures = {
            exe.submit(_download_single, url, dest_dir, 30, known_hashes, (idx + 1) if show_progress else None, show_progress): (idx, url)
            for idx, url in enumerate(urls)
        }

        try:
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    # should not happen often since _download_single catches exceptions, but be defensive
                    logger.exception("Download future raised unexpected exception")
                    _, url = futures.get(fut, (None, None))
                    res = {"url": url, "success": False, "error": str(e)}
                results.append(res)
                # update overall files progress
                if overall_pbar:
                    try:
                        overall_pbar.update(1)
                    except Exception:
                        pass
        finally:
            if overall_pbar:
                try:
                    overall_pbar.close()
                except Exception:
                    pass

    return results
