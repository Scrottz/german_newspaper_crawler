from __future__ import annotations
from typing import List, Tuple, Dict, Optional, Set
from lib.common.logging import get_logger
from lib.common.object_model import ObjectModel

logger = get_logger(__name__)

_nlp = None
_loaded_model_name: Optional[str] = None


def init_tagger(model_name: str = "de_core_news_sm") -> None:
    """
    Try to load the given spaCy model and remember it globally.
    On error a warning is logged and the tagger remains disabled.
    """
    global _nlp, _loaded_model_name
    if _nlp is not None and _loaded_model_name == model_name:
        return
    try:
        import spacy  # type: ignore
        _nlp = spacy.load(model_name)
        _loaded_model_name = model_name
        logger.info("POS tagger loaded: %s", model_name)
    except Exception:
        _nlp = None
        _loaded_model_name = None
        logger.warning(
            "spaCy model '%s' could not be loaded. Falling back to simple tokenization. "
            "Install with: pip install spacy && python -m spacy download %s",
            model_name,
            model_name,
        )


def tag_text(text: str) -> List[Tuple[str, str]]:
    """
    Tag the given text and return a list of (token, pos).
    If spaCy is not available a simple whitespace tokenization with POS 'X' is returned.
    The entire text is processed (no token limit).
    """
    if not text:
        return []

    if _nlp is None:
        # No model loaded — fallback
        logger.debug("No POS tagger loaded, using simple whitespace tokenization")
        tokens = text.split()
        return [(t, "X") for t in tokens]

    try:
        doc = _nlp(text)
        return [(token.text, token.pos_) for token in doc]
    except Exception:
        logger.exception("Error during POS tagging with spaCy, falling back to simple tokenization")
        tokens = text.split()
        return [(t, "X") for t in tokens]


def tag_object(obj: ObjectModel, max_unique_tokens: int = 200) -> ObjectModel:
    """
    Attach a `pos_taggs` mapping to the given ObjectModel.
    The structure is: token -> universal POS tag (string).
    Only the first `max_unique_tokens` unique tokens are stored.
    """
    text = (obj.text or "").strip()
    if not text:
        # fallback: try to strip tags from html minimally (very small heuristic)
        import re
        html = obj.html or ""
        # remove tags, collapse whitespace
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text).strip()

    tagged = tag_text(text)
    seen: Set[str] = set()
    pos_map: Dict[str, str] = {}
    for tok, pos in tagged:
        key = tok.strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        pos_map[key] = pos
        if len(pos_map) >= max_unique_tokens:
            break

    obj.pos_taggs = pos_map
    logger.debug("POS tagging for id=%s: %d tokens stored", obj.id, len(pos_map))
    return obj
