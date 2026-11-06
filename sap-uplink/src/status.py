"""
The uplink's status record (sap_uplink_status) and, per race and source, the
range of fixes SAP accepted (sap_race_uploads), for the health page.

Both are written to PocketBase at most every few seconds; a failed write is
retried with the next one and never holds up forwarding. The status is also
written by a thread of its own while the forwarding loop waits or blocks, so
the heartbeat doesn't depend on the poll interval or a slow SAP.
"""

import hashlib
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple

from src.convert import format_pocketbase_ts, parse_pocketbase_ts
from src.pocketbase_store import STATUS_ID, PocketbaseStore

logger = logging.getLogger(__name__)

STATUS_COLLECTION = "sap_uplink_status"
RACE_UPLOADS_COLLECTION = "sap_race_uploads"
WRITE_INTERVAL_SECONDS = 5


def now_ts() -> str:
    return format_pocketbase_ts(datetime.now(timezone.utc))


def ms_ts(ms: int) -> str:
    return format_pocketbase_ts(datetime.fromtimestamp(ms / 1000, timezone.utc))


class UplinkStatus:
    def __init__(self, store: Optional[PocketbaseStore], extra: Callable[[], Dict[str, Any]] = dict):
        """extra() adds fields read at write time (current races, realtime state, stats)."""
        self.store = store
        self.extra = extra
        self.fields: Dict[str, Any] = {"started_at": now_ts(), "configured": False}
        self._written_at = 0.0
        self._write_failed = False
        # fields is updated by the forwarding loop and written by the heartbeat thread
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self.stop_event = threading.Event()

    def start(self) -> None:
        """Write the status every few seconds, whatever the forwarding loop is doing."""
        threading.Thread(target=self._heartbeat, name="status-heartbeat", daemon=True).start()

    def _heartbeat(self) -> None:
        while not self.stop_event.wait(WRITE_INTERVAL_SECONDS):
            self.tick()

    def update(self, **fields: Any) -> None:
        with self._lock:
            self.fields.update(fields)

    def sent(self, accepted: bool) -> None:
        sent_at = now_ts()
        self.update(last_sent_at=sent_at, **({"last_accepted_at": sent_at} if accepted else {}))

    def failed(self, error: str) -> None:
        self.update(last_error=error[:2000], last_error_at=now_ts())

    def tick(self, force: bool = False) -> None:
        if self.store is None:
            return
        with self._write_lock:
            # The loop and the heartbeat thread both tick: one write per interval
            if not force and time.monotonic() - self._written_at < WRITE_INTERVAL_SECONDS:
                return
            self._written_at = time.monotonic()
            self._write()

    def _write(self) -> None:
        with self._lock:
            fields = dict(self.fields)
        try:
            self.store.upsert(STATUS_COLLECTION, STATUS_ID, {**fields, **self.extra(), "heartbeat_at": now_ts()})
        except Exception as e:
            if not self._write_failed:
                logger.warning(f"Could not write the uplink status to PocketBase: {e}")
            self._write_failed = True
        else:
            self._write_failed = False


def upload_record_id(regatta: str, race: str, source_id: str) -> str:
    # A stable id per race and source, so updates need no lookup by name
    return hashlib.sha1(f"{regatta}\0{race}\0{source_id}".encode()).hexdigest()[:15]


class RaceUploads:
    """Accumulates accepted fixes per race and source and writes the ranges to PocketBase."""

    def __init__(self, store: Optional[PocketbaseStore]):
        self.store = store
        self._lock = threading.Lock()
        # The live loop and the backfill thread both flush; one at a time
        self._flush_lock = threading.Lock()
        # Key -> {first_ms, last_ms, count, accepted_at}; pending is what isn't written yet
        self._pending: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._saved: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._written_at = 0.0

    def record(self, source_id: str, race: Dict[str, str], first_ms: int, last_ms: int, count: int) -> None:
        key = (race.get("regattaName") or "", race.get("raceName") or "", source_id)
        with self._lock:
            entry = self._pending.setdefault(key, {"first_ms": first_ms, "last_ms": last_ms, "count": 0})
            entry["first_ms"] = min(entry["first_ms"], first_ms)
            entry["last_ms"] = max(entry["last_ms"], last_ms)
            entry["count"] += count
            entry["accepted_at"] = now_ts()

    def flush(self, force: bool = False) -> None:
        if self.store is None:
            return
        with self._flush_lock:
            self._flush(force)

    def _flush(self, force: bool) -> None:
        with self._lock:
            if not self._pending or (not force and time.monotonic() - self._written_at < WRITE_INTERVAL_SECONDS):
                return
            self._written_at = time.monotonic()
            pending, self._pending = self._pending, {}
        for key, entry in pending.items():
            try:
                self._write(key, entry)
            except Exception as e:
                logger.warning(f"Could not save accepted fixes of {' / '.join(key)}, retrying: {e}")
                with self._lock:
                    self._merge_back(key, entry)

    def _write(self, key: Tuple[str, str, str], entry: Dict[str, Any]) -> None:
        record_id = upload_record_id(*key)
        saved = self._saved.get(key)
        if saved is None:
            saved = self._load(record_id)
        merged = {
            "first_ms": min(entry["first_ms"], saved["first_ms"]) if saved else entry["first_ms"],
            "last_ms": max(entry["last_ms"], saved["last_ms"]) if saved else entry["last_ms"],
            "count": entry["count"] + (saved["count"] if saved else 0),
        }
        regatta, race, source_id = key
        self.store.upsert(
            RACE_UPLOADS_COLLECTION,
            record_id,
            {
                "regatta_name": regatta,
                "race_name": race,
                "source_id": source_id,
                "first_fix": ms_ts(merged["first_ms"]),
                "last_fix": ms_ts(merged["last_ms"]),
                "fixes_accepted": merged["count"],
                "last_accepted_at": entry["accepted_at"],
            },
        )
        self._saved[key] = merged

    def _load(self, record_id: str) -> Optional[Dict[str, Any]]:
        record = self.store.get(RACE_UPLOADS_COLLECTION, record_id)
        if not record or not record.get("first_fix") or not record.get("last_fix"):
            return None
        return {
            "first_ms": int(parse_pocketbase_ts(record["first_fix"]).timestamp() * 1000),
            "last_ms": int(parse_pocketbase_ts(record["last_fix"]).timestamp() * 1000),
            "count": int(record.get("fixes_accepted") or 0),
        }

    def _merge_back(self, key: Tuple[str, str, str], entry: Dict[str, Any]) -> None:
        current = self._pending.get(key)
        if current is None:
            self._pending[key] = entry
            return
        current["first_ms"] = min(current["first_ms"], entry["first_ms"])
        current["last_ms"] = max(current["last_ms"], entry["last_ms"])
        current["count"] += entry["count"]
