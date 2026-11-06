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


def field(name: str, type_: str) -> Dict[str, Any]:
    return {"name": name, "type": type_}


def autodate(name: str, on_update: bool) -> Dict[str, Any]:
    return {"name": name, "type": "autodate", "onCreate": True, "onUpdate": on_update}


COLLECTIONS: List[Dict[str, Any]] = [
    {
        # One record; empty fields fall back to the environment
        "name": "sap_settings",
        "fields": [text("sap_url"), text("access_token"), text("event_id"), text("course_area"), autodate("updated", True)],
        "public_read": False,
    },
    {
        # One record, overwritten every few seconds
        "name": "sap_uplink_status",
        "fields": [
            field("started_at", "date"),
            field("heartbeat_at", "date"),
            field("configured", "bool"),
            text("problem"),
            text("sap_url"),
            text("event_id"),
            text("course_area"),
            text("realtime"),
            text("cursor"),
            field("last_sent_at", "date"),
            field("last_accepted_at", "date"),
            field("last_error_at", "date"),
            text("last_error"),
            field("current_races", "json"),
            # No race list: SAP adds the wind to every tracked race the login may update
            field("all_races", "bool"),
            field("tracked_races", "json"),
            field("stats", "json"),
        ],
        "public_read": True,
    },
    {
        # Per race and source: the range of fixes SAP accepted
        "name": "sap_race_uploads",
        "fields": [
            text("regatta_name"),
            text("race_name"),
            text("source_id"),
            field("first_fix", "date"),
            field("last_fix", "date"),
            field("fixes_accepted", "number"),
            field("last_accepted_at", "date"),
            autodate("updated", True),
        ],
        "indexes": [
            "CREATE UNIQUE INDEX `idx_sap_race_uploads_race_source` ON `sap_race_uploads` "
            "(`regatta_name`, `race_name`, `source_id`)"
        ],
        "public_read": True,
    },
    {
        # Requested backfills; the uplink runs pending ones in order
        "name": "sap_backfills",
        "fields": [
            text("regatta_name"),
            text("race_name"),
            {"name": "range_from", "type": "date", "required": True},
            {"name": "range_until", "type": "date", "required": True},
            {"name": "status", "type": "select", "required": True, "maxSelect": 1, "values": ["pending", "running", "done", "failed"]},
            text("message"),
            field("fixes_sent", "number"),
            field("fixes_accepted", "number"),
            field("started_at", "date"),
            field("finished_at", "date"),
            autodate("created", False),
        ],
        "public_read": True,
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
