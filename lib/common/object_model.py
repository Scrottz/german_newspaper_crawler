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


@dataclass
class ObjectModel:
    """
    Simple object model for articles.
    Fields: _id (internal int), id (external identifier, may be numeric string),
    autor, category, published_date, parsed_date, html, text, ai_summary,
    titel, teaser, pos_taggs, content_hash
    """
    # internal numeric id (auto-assigned)
    _id: Optional[int] = None

    # public id (kept for compatibility). If missing or looks like a URL it will be set to the numeric _id.
    id: Optional[str] = None

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

    def __post_init__(self) -> None:
        """Assign internal _id if missing and compute content_hash if possible."""
        # assign internal id if not provided
        if self._id is None:
            self._id = _get_next_internal_id()
        else:
            # if _id was provided on construction, ensure counter continues beyond it
            _ensure_next_internal_id_at_least(self._id)

        # If id is missing or looks like a URL, prefer a simple incremental id (string)
        if not self.id or (isinstance(self.id, str) and self.id.startswith(("http://", "https://"))):
            self.id = str(self._id)

        # compute content hash from titel and teaser when not provided
        if self.content_hash is None and self.titel:
            try:
                h = hashlib.sha256()
                # use titel and teaser separated to avoid accidental collisions
                h.update(self.titel.encode("utf-8"))
                h.update(b"\n")
                teaser_bytes = self.teaser.encode("utf-8") if self.teaser else b""
                h.update(teaser_bytes)
                self.content_hash = h.hexdigest()
                logger.debug("Computed content_hash for titel=%s: %s", self.titel, self.content_hash)
            except Exception:
                logger.exception("Failed to compute content_hash for titel=%s", self.titel)
                self.content_hash = None

        logger.info(
            "ObjectModel created: _id=%s id=%s autor=%s category=%s published_date=%s parsed_date=%s titel=%s teaser_present=%s pos_taggs_count=%d content_hash=%s",
            self._id,
            self.id,
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
    Gibt auch das interne `_id` aus.
    """
    data: Dict[str, Any] = {}
    data["_id"] = getattr(obj, "_id", None)
    data["id"] = getattr(obj, "id", None)
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
    existing_id_numeric = _maybe_parse_int(data.get("id"))
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

    obj = ObjectModel(
        _id=existing_internal,
        id=data.get("id"),
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

    logger.debug("Deserialized ObjectModel _id=%s id=%s autor=%s category=%s", obj._id, obj.id, obj.autor, obj.category)
    return obj
