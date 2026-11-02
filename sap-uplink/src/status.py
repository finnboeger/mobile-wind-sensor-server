"""
The uplink's status record in PocketBase (sap_uplink_status), for the health
page. Written at most every few seconds; a failed write is retried with the
next one and never holds up forwarding.
"""

import logging
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

    def update(self, **fields: Any) -> None:
        self.fields.update(fields)

    def sent(self, accepted: bool) -> None:
        self.fields["last_sent_at"] = now_ts()
        if accepted:
            self.fields["last_accepted_at"] = self.fields["last_sent_at"]

    def failed(self, error: str) -> None:
        self.update(last_error=error[:2000], last_error_at=now_ts())

    def tick(self, force: bool = False) -> None:
        if self.store is None or (not force and time.monotonic() - self._written_at < WRITE_INTERVAL_SECONDS):
            return
        self._written_at = time.monotonic()
        try:
            self.store.upsert(STATUS_COLLECTION, STATUS_ID, {**self.fields, **self.extra(), "heartbeat_at": now_ts()})
        except Exception as e:
            if not self._write_failed:
                logger.warning(f"Could not write the uplink status to PocketBase: {e}")
            self._write_failed = True
        else:
            self._write_failed = False
