# python
from __future__ import annotations
from dataclasses import dataclass, asdict, field
from datetime import datetime, date
from typing import Optional, Any, Dict
import threading
import hashlib

from lib.common.logging import get_logger

logger = get_logger(__name__)

# Module-level counter for internal IDs and a lock for thread-safety
_next_internal_id: int = 0
_next_internal_id_lock = threading.Lock()


def _get_next_internal_id() -> int:
    global _next_internal_id
    with _next_internal_id_lock:
        val = _next_internal_id
        _next_internal_id += 1
    return val


def _ensure_next_internal_id_at_least(min_exclusive: int) -> None:
    """
    Ensure the next internal id will be at least min_exclusive (i.e. next id > min_exclusive).
    This is used when loading existing data to continue counting.
    """
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
    Simple object model for articles.

    Backwards-compatible constructor:
    - accepts `_id` and `url` as explicit kwargs
    - also accepts legacy `id` kwarg:
        * numeric -> treated as internal `_id`
        * string starting with http(s) -> treated as `url`
        * other string -> treated as `url` (fallback)
    """
    # internal numeric id (auto-assigned)
    _id: Optional[int] = None

    # url field replaces the previous public `id` that held the URL
    url: Optional[str] = None

    titel: Optional[str] = None
    teaser: Optional[str] = None
    autor: Optional[str] = None
    category: Optional[str] = None
    published_date: Optional[datetime] = None
    parsed_date: Optional[datetime] = None
    html: Optional[str] = None
    text: Optional[str] = None
    ai_summary: Optional[str] = None
    pos_taggs: Dict[str, str] = field(default_factory=dict)
    content_hash: Optional[str] = None

    def __init__(
        self,
        _id: Optional[int] = None,
        url: Optional[str] = None,
        id: Any = None,  # backward-compatible alias: numeric -> _id, str-url -> url
        titel: Optional[str] = None,
        teaser: Optional[str] = None,
        autor: Optional[str] = None,
        category: Optional[str] = None,
        published_date: Optional[datetime] = None,
        parsed_date: Optional[datetime] = None,
        html: Optional[str] = None,
        text: Optional[str] = None,
        ai_summary: Optional[str] = None,
        pos_taggs: Optional[Dict[str, str]] = None,
        content_hash: Optional[str] = None,
    ) -> None:
        # Resolve legacy `id` alias
        resolved_internal_id: Optional[int] = None
        resolved_url: Optional[str] = None

        if id is not None:
            maybe_num = _maybe_parse_int(id)
            if maybe_num is not None:
                resolved_internal_id = maybe_num
            elif isinstance(id, str) and id.startswith(("http://", "https://")):
                resolved_url = id
            elif isinstance(id, str):
                # fallback: preserve string as url if no other candidate
                resolved_url = id

        # explicit kwargs override alias resolution
        if _id is not None:
            resolved_internal_id = _id
        if url is not None:
            resolved_url = url

        # assign resolved values
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
        self.ai_summary = ai_summary
        self.pos_taggs = pos_taggs if isinstance(pos_taggs, dict) else {}
        self.content_hash = content_hash

        # finalize (assign internal id if missing, compute content_hash fallback, logging)
        self.__post_init__()

    def __post_init__(self) -> None:
        """Assign internal \_id if missing and compute content_hash from url when available."""
        # assign internal id if not provided
        if self._id is None:
            self._id = _get_next_internal_id()
        else:
            # if _id was provided on construction, ensure counter continues beyond it
            _ensure_next_internal_id_at_least(self._id)

        # compute content_hash from the URL string (url) if available and looks like a URL
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
            # fallback: compute from text/html if available
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
            "ObjectModel created: _id=%s url=%s autor=%s category=%s published_date=%s parsed_date=%s titel=%s teaser_present=%s pos_taggs_count=%d content_hash=%s",
            self._id,
            self.url,
            self.autor,
            self.category,
            self.published_date.isoformat() if isinstance(self.published_date, datetime) else self.published_date,
            self.parsed_date.isoformat() if isinstance(self.parsed_date, datetime) else self.parsed_date,
            self.titel,
            bool(self.teaser),
            len(self.pos_taggs) if isinstance(self.pos_taggs, dict) else 0,
            self.content_hash,
        )


def to_dict(obj) -> Dict[str, Any]:
    """
    Robuste Serialisierung eines ObjectModel-Objekts zu einem dict.
    published_date und parsed_date werden als ISO-Strings ausgegeben
    falls sie datetime/date sind; Strings bleiben unverändert; None bleibt None.
    Gibt auch das interne `_id` und `url` aus.
    """
    data: Dict[str, Any] = {}
    data["_id"] = getattr(obj, "_id", None)
    data["url"] = getattr(obj, "url", None)
    data["html"] = getattr(obj, "html", None)
    data["text"] = getattr(obj, "text", None)
    data["titel"] = getattr(obj, "titel", None)
    data["teaser"] = getattr(obj, "teaser", None)
    data["content_hash"] = getattr(obj, "content_hash", None)
    data["autor"] = getattr(obj, "autor", None)
    data["pos_taggs"] = getattr(obj, "pos_taggs", None)

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

    data["parsed_date"] = _serialize_date(getattr(obj, "parsed_date", None))
    data["published_date"] = _serialize_date(getattr(obj, "published_date", None))

    return data


def from_dict(data: Dict[str, Any]) -> ObjectModel:
    """Create an ObjectModel from a dict; accepts ISO strings for date fields.
    Updates the internal id counter based on existing `_id` or numeric `id` values so
    subsequent new objects continue counting from the maximum seen.
    """
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
        logger.warning("Unexpected type for date field %s: %r", field_name, type(value))
        return None

    pd = _parse_date(data.get("published_date"), "published_date")
    parsed = _parse_date(data.get("parsed_date"), "parsed_date")

    # Update internal id counter from existing data so it continues incrementing across runs
    existing_internal = _maybe_parse_int(data.get("_id"))

    # Backwards compatibility: older records may have used `id` as numeric or as URL.
    existing_id_numeric = None
    id_field = data.get("id")
    if id_field is not None:
        # If `id` is numeric string or int -> consider for internal counter
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

    pos_taggs_val = data.get("pos_taggs", {})
    if pos_taggs_val is None:
        pos_taggs_val = {}
    if not isinstance(pos_taggs_val, dict):
        logger.warning("pos_taggs has unexpected type %s, forcing empty dict", type(pos_taggs_val))
        pos_taggs_val = {}

    # Determine url: prefer explicit "url" field; fallback to old "id" if it looks like a URL
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
        ai_summary=data.get("ai_summary"),
        titel=data.get("titel"),
        teaser=data.get("teaser"),
        pos_taggs=pos_taggs_val,
        content_hash=data.get("content_hash"),
    )

    logger.debug("Deserialized ObjectModel _id=%s url=%s autor=%s category=%s", obj._id, obj.url, obj.autor, obj.category)
    return obj
