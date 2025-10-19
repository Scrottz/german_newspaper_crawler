from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Any, Dict, List, Tuple
import threading
import hashlib

from lib.common.logging import get_logger

logger = get_logger(__name__)

_next_internal_id: int = 0
_next_internal_id_lock = threading.Lock()


def _get_next_internal_id() -> int:
    global _next_internal_id
    with _next_internal_id_lock:
        val = _next_internal_id
        _next_internal_id += 1
    return val


def _ensure_next_internal_id_at_least(min_exclusive: int) -> None:
    global _next_internal_id
    with _next_internal_id_lock:
        if _next_internal_id <= min_exclusive:
            _next_internal_id = min_exclusive + 1


def _maybe_parse_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        try:
            return int(val)
        except ValueError:
            return None
    return None


@dataclass(init=False)
class ObjectModel:
    """
    Object model for articles.

    - `ai_keywords`: List[str]
    - `pos_taggs`: List[Tuple[int, str, str, str, str]] with entries:
        (id: int, token: str, lemma: str, tags: str, pos: str)

    Constructor remains backwards-compatible with legacy fields (`id`, old pos_taggs dict/list).
    """
    _id: Optional[int] = None
    url: Optional[str] = None
    titel: Optional[str] = None
    teaser: Optional[str] = None
    autor: Optional[str] = None
    category: Optional[str] = None
    published_date: Optional[datetime] = None
    parsed_date: Optional[datetime] = None
    html: Optional[str] = None
    text: Optional[str] = None
    ai_keywords: List[str] = field(default_factory=list)
    pos_taggs: List[Tuple[int, str, str, str, str]] = field(default_factory=list)
    content_hash: Optional[str] = None

    def __init__(
        self,
        _id: Optional[int] = None,
        url: Optional[str] = None,
        titel: Optional[str] = None,
        teaser: Optional[str] = None,
        autor: Optional[str] = None,
        category: Optional[str] = None,
        published_date: Optional[datetime] = None,
        parsed_date: Optional[datetime] = None,
        html: Optional[str] = None,
        text: Optional[str] = None,
        ai_keywords: Optional[List[str]] = None,
        pos_taggs: Optional[Any] = None,
        content_hash: Optional[str] = None,
    ) -> None:
        # Resolve legacy `id` alias (note: legacy callers may have passed 'id' via kwargs)
        resolved_internal_id: Optional[int] = None
        resolved_url: Optional[str] = None

        # Backwards compatibility if someone passed 'id' in kwargs (handled by callers like from_dict)
        # Keep logic robust: do not rely on builtin `id`
        # If callers passed a numeric _id already, prefer that via _id param below.
        # (No direct 'id' parameter here to avoid shadowing builtin.)

        if _id is not None:
            resolved_internal_id = _id
        if url is not None:
            resolved_url = url

        self._id = resolved_internal_id
        self.url = resolved_url
        self.titel = titel
        self.teaser = teaser
        self.autor = autor
        self.category = category
        self.published_date = published_date
        self.parsed_date = parsed_date
        self.html = html
        self.text = text

        # Normalize ai_keywords: prefer explicit list, otherwise empty list
        if isinstance(ai_keywords, list):
            self.ai_keywords = [str(k) for k in ai_keywords]
        else:
            self.ai_keywords = []

        # Normalize pos_taggs into the new list-of-tuples format
        self.pos_taggs = self._normalize_pos_taggs(pos_taggs)
        self.content_hash = content_hash

        # finalize (assign internal id if missing, compute content_hash fallback, logging)
        self.__post_init__()

    @staticmethod
    def _normalize_pos_taggs(val: Any) -> List[Tuple[int, str, str, str, str]]:
        if val is None:
            return []
        entries: List[Tuple[int, str, str, str, str]] = []

        if isinstance(val, dict):
            for i, (tok, pos) in enumerate(val.items()):
                entries.append((i, tok or "", "", "", pos or ""))
            return entries

        if isinstance(val, list):
            next_id = 0
            for i, item in enumerate(val):
                if isinstance(item, dict):
                    tid = item.get("id", item.get("tid"))
                    tid = tid if isinstance(tid, int) else next_id
                    token = item.get("wort") or item.get("text") or item.get("token") or ""
                    lemma = item.get("lemma") or ""
                    pos = item.get("pos") or ""
                    tags = item.get("tags") or item.get("tag") or ""
                    entries.append((int(tid), token, lemma, tags, pos))
                    next_id = int(tid) + 1
                    continue

                if isinstance(item, (list, tuple)):
                    if len(item) == 5:
                        try:
                            tid = int(item[0])
                        except Exception:
                            tid = next_id
                        token = str(item[1]) if item[1] is not None else ""
                        lemma = str(item[2]) if item[2] is not None else ""
                        tags = str(item[3]) if item[3] is not None else ""
                        pos = str(item[4]) if item[4] is not None else ""
                        entries.append((tid, token, lemma, tags, pos))
                        next_id = tid + 1
                        continue
                    if len(item) == 4:
                        token = str(item[0]) if item[0] is not None else ""
                        lemma = str(item[1]) if item[1] is not None else ""
                        tags = str(item[2]) if item[2] is not None else ""
                        pos = str(item[3]) if item[3] is not None else ""
                        entries.append((next_id, token, lemma, tags, pos))
                        next_id += 1
                        continue
                    flattened = [str(x) if x is not None else "" for x in item]
                    while len(flattened) < 4:
                        flattened.append("")
                    token, lemma, tags, pos = flattened[0], flattened[1], flattened[2], flattened[3]
                    entries.append((next_id, token, lemma, tags, pos))
                    next_id += 1
                    continue

                token = str(item)
                entries.append((next_id, token, "", "", ""))
                next_id += 1

            try:
                entries.sort(key=lambda x: int(x[0]))
            except Exception:
                pass
            return entries

        logger.warning("ObjectModel._normalize_pos_taggs: unexpected type %s, returning empty list", type(val))
        return []

    def __post_init__(self) -> None:
        if self._id is None:
            self._id = _get_next_internal_id()
        else:
            _ensure_next_internal_id_at_least(self._id)

        if self.content_hash is None and isinstance(self.url, str) and self.url.startswith(("http://", "https://")):
            try:
                h = hashlib.sha256()
                h.update(self.url.encode("utf-8"))
                self.content_hash = h.hexdigest()
                logger.debug("Computed content_hash from url=%s: %s", self.url, self.content_hash)
            except Exception:
                logger.exception("Failed to compute content_hash from url=%s", self.url)
                self.content_hash = None
        elif self.content_hash is None:
            data = (self.text or self.html or "").strip()
            if data:
                try:
                    h = hashlib.sha256()
                    h.update(data.encode("utf-8"))
                    self.content_hash = h.hexdigest()
                    logger.debug("Computed content_hash from content for _id=%s: %s", self._id, self.content_hash)
                except Exception:
                    logger.exception("Failed to compute fallback content_hash for _id=%s", self._id)
                    self.content_hash = None

        logger.info(
            "ObjectModel created: _id=%s url=%s autor=%s category=%s published_date=%s parsed_date=%s titel=%s teaser_present=%s pos_taggs_count=%d ai_keywords_count=%d content_hash=%s",
            self._id,
            self.url,
            self.autor,
            self.category,
            self.published_date.isoformat() if isinstance(self.published_date, datetime) else self.published_date,
            self.parsed_date.isoformat() if isinstance(self.parsed_date, datetime) else self.parsed_date,
            self.titel,
            bool(self.teaser),
            len(self.pos_taggs) if isinstance(self.pos_taggs, list) else 0,
            len(self.ai_keywords) if isinstance(self.ai_keywords, list) else 0,
            self.content_hash,
        )

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        data["_id"] = getattr(self, "_id", None)
        data["url"] = getattr(self, "url", None)
        data["html"] = getattr(self, "html", None)
        data["text"] = getattr(self, "text", None)
        data["titel"] = getattr(self, "titel", None)
        data["teaser"] = getattr(self, "teaser", None)
        data["content_hash"] = getattr(self, "content_hash", None)
        data["autor"] = getattr(self, "autor", None)
        data["ai_keywords"] = getattr(self, "ai_keywords", None)

        pt = getattr(self, "pos_taggs", None)
        if isinstance(pt, list):
            try:
                data["pos_taggs"] = [[int(t[0]), t[1], t[2], t[3], t[4]] for t in pt]
            except Exception:
                serialized = []
                for i, t in enumerate(pt):
                    try:
                        serialized.append([int(t[0]), str(t[1]), str(t[2]), str(t[3]), str(t[4])])
                    except Exception:
                        serialized.append([i, str(t), "", "", ""])
                data["pos_taggs"] = serialized
        else:
            data["pos_taggs"] = []

        def _serialize_date(val):
            if val is None:
                return None
            if isinstance(val, (datetime, date)):
                return val.isoformat()
            if isinstance(val, str):
                return val
            try:
                iso = getattr(val, "isoformat", None)
                if callable(iso):
                    return iso()
            except Exception:
                pass
            return str(val)

        data["parsed_date"] = _serialize_date(getattr(self, "parsed_date", None))
        data["published_date"] = _serialize_date(getattr(self, "published_date", None))

        return data

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ObjectModel":
        logger.debug("Deserializing ObjectModel from data keys=%s", list(data.keys()))

        def _parse_date(value: Any, field_name: str) -> Optional[datetime]:
            if isinstance(value, str):
                try:
                    parsed = datetime.fromisoformat(value)
                    logger.debug("Parsed %s: %s", field_name, parsed.isoformat())
                    return parsed
                except ValueError:
                    logger.warning("Failed to parse date field %s: %r", field_name, value)
                    return None
            if isinstance(value, datetime):
                return value
            if value is None:
                return None
            logger.warning("Unexpected type for date field %s: %r", field_name, value)
            return None

        pd = _parse_date(data.get("published_date"), "published_date")
        parsed = _parse_date(data.get("parsed_date"), "parsed_date")

        existing_internal = _maybe_parse_int(data.get("_id"))

        existing_id_numeric = None
        id_field = data.get("id")
        if id_field is not None:
            maybe_num = _maybe_parse_int(id_field)
            if maybe_num is not None:
                existing_id_numeric = maybe_num

        max_seen = None
        if existing_internal is not None:
            max_seen = existing_internal
        if existing_id_numeric is not None:
            if max_seen is None or existing_id_numeric > max_seen:
                max_seen = existing_id_numeric
        if max_seen is not None:
            _ensure_next_internal_id_at_least(max_seen)

        # ai_keywords: prefer new list field; accept legacy ai_summary (string) or None
        ak = data.get("ai_keywords")
        if ak is None:
            ai_summary_legacy = data.get("ai_summary")
            if isinstance(ai_summary_legacy, str) and ai_summary_legacy:
                normalized_ai_keywords: List[str] = [ai_summary_legacy]
            else:
                normalized_ai_keywords = []
        elif isinstance(ak, list):
            normalized_ai_keywords = [str(x) for x in ak]
        else:
            normalized_ai_keywords = [str(ak)]

        pos_taggs_val = data.get("pos_taggs", [])
        normalized_pos_taggs = ObjectModel._normalize_pos_taggs(pos_taggs_val)

        url_value = data.get("url")
        if not url_value and isinstance(id_field, str) and id_field.startswith(("http://", "https://")):
            url_value = id_field

        obj = ObjectModel(
            _id=existing_internal,
            url=url_value,
            autor=data.get("autor"),
            category=data.get("category"),
            published_date=pd,
            parsed_date=parsed,
            html=data.get("html"),
            text=data.get("text"),
            ai_keywords=normalized_ai_keywords,
            pos_taggs=normalized_pos_taggs,
            titel=data.get("titel"),
            teaser=data.get("teaser"),
            content_hash=data.get("content_hash"),
        )

        logger.debug("Deserialized ObjectModel _id=%s url=%s autor=%s category=%s", obj._id, obj.url, obj.autor, obj.category)
        return obj
