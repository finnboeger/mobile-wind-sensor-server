"""
The uplink's status record in PocketBase (sap_uplink_status), for the health
page. Written at most every few seconds, and by a thread of its own while the
forwarding loop waits or blocks, so the heartbeat doesn't depend on the poll
interval or a slow SAP. A failed write is retried with the next one and never
holds up forwarding.
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from src.convert import format_pocketbase_ts
from src.pocketbase_store import STATUS_ID, PocketbaseStore

logger = logging.getLogger(__name__)

STATUS_COLLECTION = "sap_uplink_status"
WRITE_INTERVAL_SECONDS = 5


def now_ts() -> str:
    return format_pocketbase_ts(datetime.now(timezone.utc))


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
