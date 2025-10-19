# python
# File: `lib/common/pos_tagging.py`
from __future__ import annotations
from typing import List, Tuple, Optional, Dict, Any, Iterable
import re
import logging

from lib.common.logging import get_logger

logger = get_logger(__name__)

# Configurable thresholds
MAX_POS_TAG_ENTRIES = 50000        # max tokens to store in the ObjectModel
MAX_POS_TAG_TOKENS = 200_000       # if estimated tokens exceed this, skip tagging entirely
CHUNK_DEFAULT = 200_000            # preferred chunk size for spaCy processing (chars)
SKIP_EXTENSIONS = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".mp4", ".zip", ".gz")
PDF_MAGIC = "%PDF"

# Cached spaCy nlp objects by model name
_NLP_CACHE: Dict[str, Any] = {}

try:
    import spacy  # type: ignore
except Exception:
    spacy = None  # spaCy optional


def _get_nlp(model_name: str = "de_core_news_sm", disable: Optional[Iterable[str]] = None):
    """
    Load and cache a spaCy model. Disable heavy components by default (parser, ner).
    Returns None if spaCy or the model cannot be loaded.
    """
    if spacy is None:
        logger.warning("spaCy not available; POS tagging will fallback to whitespace tokenization")
        return None
    if model_name in _NLP_CACHE:
        return _NLP_CACHE[model_name]
    try:
        disable_list = list(disable) if disable is not None else ["parser", "ner"]
        nlp = spacy.load(model_name, disable=disable_list)
        _NLP_CACHE[model_name] = nlp
        logger.info("Loaded spaCy model %s (disable=%s)", model_name, disable_list)
        return nlp
    except Exception:
        logger.exception("Failed to load spaCy model %s", model_name)
        return None


def _chunk_text_by_paragraphs(text: str, max_len: int) -> List[str]:
    """
    Split text on paragraph boundaries and group into chunks <= max_len.
    Very long paragraphs are sliced.
    """
    if not text:
        return []
    paras = re.split(r"\n{2,}", text)
    chunks: List[str] = []
    current = ""
    for p in paras:
        p = p.strip()
        if not p:
            continue
        if len(p) > max_len:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(p), max_len):
                part = p[i : i + max_len]
                if part:
                    chunks.append(part)
            continue
        if not current:
            current = p
        elif len(current) + 2 + len(p) <= max_len:
            current = current + "\n\n" + p
        else:
            chunks.append(current)
            current = p
    if current:
        chunks.append(current)
    return chunks


def _estimate_token_count(text: str) -> int:
    """
    Cheap token estimate using whitespace split. Fast and conservative.
    """
    if not text:
        return 0
    try:
        return len(text.split())
    except Exception:
        # fallback: approximate by chars/5
        return max(0, int(len(text) / 5))


def pos_tag_text(
    text: str,
    model_name: str = "de_core_news_sm",
    preferred_chunk_chars: int = CHUNK_DEFAULT,
) -> List[Tuple[int, str, str, str, str]]:
    """
    POS-tag a text in chunks. Returns list of tuples:
      (id, token, lemma, tag, pos)

    If spaCy is unavailable, returns whitespace-tokenized fallback tuples.
    """
    if not text:
        return []

    nlp = _get_nlp(model_name)
    if nlp is None:
        # whitespace fallback
        tokens = []
        for i, w in enumerate(text.split()):
            if i >= MAX_POS_TAG_ENTRIES:
                break
            tokens.append((i, w, "", "", ""))
        logger.info("pos_tag_text: spaCy not available, returned %d whitespace tokens", len(tokens))
        return tokens

    # determine safe chunk size from model max_length and preferred chunk size
    model_max = getattr(nlp, "max_length", 1_000_000)
    # choose slightly smaller than model_max to be conservative
    chunk_size = min(preferred_chunk_chars, max(10_000, int(model_max * 0.8)))
    if chunk_size <= 0:
        chunk_size = preferred_chunk_chars

    if len(text) > model_max:
        logger.warning(
            "pos_tag_text: input length %d exceeds spaCy model max_length %d; chunking into %d-char chunks",
            len(text),
            model_max,
            chunk_size,
        )

    chunks = _chunk_text_by_paragraphs(text, chunk_size)

    entries: List[Tuple[int, str, str, str, str]] = []
    next_id = 0

    try:
        # Use nlp.pipe to process chunks sequentially (n_process=1 to avoid multiprocessing memory spikes)
        disable = [name for name in ("parser", "ner") if name in getattr(nlp, "pipe_names", [])]
        for doc in nlp.pipe(chunks, batch_size=8, n_process=1, disable=disable):
            for token in doc:
                if len(entries) >= MAX_POS_TAG_ENTRIES:
                    logger.warning("pos_tag_text: reached MAX_POS_TAG_ENTRIES=%d, truncating output", MAX_POS_TAG_ENTRIES)
                    return entries
                lemma = getattr(token, "lemma_", "") or ""
                tag = getattr(token, "tag_", "") or ""
                pos = getattr(token, "pos_", "") or ""
                entries.append((next_id, token.text, lemma, tag, pos))
                next_id += 1
    except Exception:
        logger.exception("pos_tag_text: spaCy failed during chunked processing, falling back to whitespace for remaining text")
        # Fallback: if something failed, fill remaining from whitespace split (best-effort)
        if len(entries) < MAX_POS_TAG_ENTRIES:
            remaining = text.split()
            start = next_id
            for i, w in enumerate(remaining):
                if len(entries) >= MAX_POS_TAG_ENTRIES:
                    break
                entries.append((start + i, w, "", "", ""))
    logger.info("pos_tag_text: produced %d tokens", len(entries))
    return entries


def pos_tag_object(obj: Any, model_name: str = "de_core_news_sm") -> None:
    """
    Annotate an ObjectModel-like object with POS tags unless skipping conditions apply.
    - Skips binary/PDF URLs or responses.
    - Skips if estimated token count > MAX_POS_TAG_TOKENS.
    - Stores up to MAX_POS_TAG_ENTRIES tokens in obj.pos_taggs.
    """
    try:
        if obj is None:
            return

        url = (getattr(obj, "url", "") or "").lower()
        html = getattr(obj, "html", "") or ""
        text = getattr(obj, "text", "") or ""

        # Skip binary URLs or PDF magic in HTML
        if any(url.endswith(ext) for ext in SKIP_EXTENSIONS) or (html and html.lstrip().startswith(PDF_MAGIC)):
            logger.info("pos_tag_object: skipping POS tagging for binary/PDF content: %s", url or "<no-url>")
            return

        if not text:
            logger.debug("pos_tag_object: no text available to POS tag for %s", url or "<no-url>")
            return

        # Estimate tokens and possibly skip tagging for huge texts
        estimated = _estimate_token_count(text)
        if estimated > MAX_POS_TAG_TOKENS:
            logger.info(
                "pos_tag_object: skipping POS tagging for %s — estimated tokens %d > limit %d",
                url or "<no-url>",
                estimated,
                MAX_POS_TAG_TOKENS,
            )
            return

        entries = pos_tag_text(text, model_name=model_name)
        if not entries:
            logger.debug("pos_tag_object: pos_tag_text returned no entries for %s", url or "<no-url>")
            return

        # clamp stored entries
        if len(entries) > MAX_POS_TAG_ENTRIES:
            logger.warning(
                "pos_tag_object: pos tag count %d exceeds storage cap %d; truncating for %s",
                len(entries),
                MAX_POS_TAG_ENTRIES,
                url or "<no-url>",
            )
            entries = entries[:MAX_POS_TAG_ENTRIES]

        # Attach to object; prefer attribute name 'pos_taggs' to match ObjectModel
        try:
            setattr(obj, "pos_taggs", entries)
            logger.info("pos_tag_object: stored %d pos tags for %s", len(entries), url or "<no-url>")
        except Exception:
            logger.exception("pos_tag_object: failed to set pos_taggs on object %s", url or "<no-url>")
    except Exception:
        logger.exception("pos_tag_object: unexpected error for _id=%s", getattr(obj, "_id", None))
        return
