# python
from typing import Any, Dict, Optional
from pymongo.database import Database
from pymongo import ReturnDocument


def get_next_id(db: Database, counter_name: str, seq_field: str = "seq") -> int:
    """
    Return the next incremental ID (atomic).

    Uses a counter document in the `counters` collection with the form:
      { "_id": "<counter_name>", "seq": <number> }

    Parameters:
    - db: pymongo Database instance
    - counter_name: name of the counter to increment
    - seq_field: field name used to store the sequence value (default: "seq")

    Raises:
    - RuntimeError if the counter document could not be retrieved/created.
    """
    # Atomically increment the counter document and return the new sequence value.
    res = db.counters.find_one_and_update(
        {"_id": counter_name},
        {"$inc": {seq_field: 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    if res is None:
        # This should not happen when upsert=True, but fail loudly if it does.
        raise RuntimeError(f"Failed to generate next ID for counter '{counter_name}'")
    return int(res.get(seq_field, 0))


def insert_with_sequence(db: Database, collection_name: str, doc: Dict[str, Any],
                         counter_name: Optional[str] = None, seq_field: str = "seq") -> Any:
    """
    Insert `doc` into `collection_name` and set a numeric `_id` using a sequence.

    Behavior:
    - If `counter_name` is omitted, the default counter name is "<collection_name>_id".
    - The function obtains the next sequence value and assigns it to `doc['_id']`
      before inserting the document.

    Parameters:
    - db: pymongo Database instance
    - collection_name: target collection name for insertion
    - doc: document to insert (modified in-place by setting `_id`)
    - counter_name: optional counter name; defaults to "<collection_name>_id"
    - seq_field: name of the sequence field in the counter document (default: "seq")

    Returns:
    - The result of `insert_one` (InsertOneResult).
    """
    if counter_name is None:
        counter_name = f"{collection_name}_id"

    next_id = get_next_id(db, counter_name, seq_field=seq_field)
    # Set the numeric _id (will overwrite any existing _id in the provided doc)
    doc["_id"] = next_id
    return db[collection_name].insert_one(doc)
