"""
The uplink's own PocketBase collections, created or completed at startup.
Superuser-only collections have null rules; "" makes a collection publicly readable.
"""

import logging
from typing import Any, Dict, List

from src.pocketbase_store import PocketbaseStore

logger = logging.getLogger(__name__)


def text(name: str) -> Dict[str, Any]:
    return {"name": name, "type": "text"}


def autodate(name: str, on_update: bool) -> Dict[str, Any]:
    return {"name": name, "type": "autodate", "onCreate": True, "onUpdate": on_update}


COLLECTIONS: List[Dict[str, Any]] = [
    {
        # One record; empty fields fall back to the environment
        "name": "sap_settings",
        "fields": [text("sap_url"), text("access_token"), text("event_id"), text("course_area"), autodate("updated", True)],
        "public_read": False,
    },
]


def ensure_collections(store: PocketbaseStore) -> None:
    for spec in COLLECTIONS:
        read = "" if spec["public_read"] else None
        rules = {"listRule": read, "viewRule": read, "createRule": None, "updateRule": None, "deleteRule": None}
        existing = store.get_collection(spec["name"])
        if existing is None:
            logger.info(f"Creating collection {spec['name']}")
            store.save_collection(
                None,
                {"name": spec["name"], "type": "base", "fields": spec["fields"], "indexes": spec.get("indexes", []), **rules},
            )
            continue
        names = {field["name"] for field in existing.get("fields", [])}
        missing = [field for field in spec["fields"] if field["name"] not in names]
        indexes = list(existing.get("indexes") or [])
        indexes += [sql for sql in spec.get("indexes", []) if sql not in indexes]
        for field in missing:
            logger.info(f"Adding field {spec['name']}.{field['name']}")
        store.save_collection(spec["name"], {"fields": existing.get("fields", []) + missing, "indexes": indexes, **rules})
