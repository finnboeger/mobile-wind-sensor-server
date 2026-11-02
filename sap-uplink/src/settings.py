"""
SAP settings that can change while the service runs: server, access token and
event. Values saved in PocketBase (sap_settings) win; empty ones fall back
to the environment.
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from src.pocketbase_store import SETTINGS_ID, PocketbaseStore

logger = logging.getLogger(__name__)

SETTINGS_COLLECTION = "sap_settings"


@dataclass(frozen=True)
class SapSettings:
    sap_url: str = ""
    access_token: str = ""
    event_id: str = ""
    course_area: str = ""

    def merged_over(self, defaults: "SapSettings") -> "SapSettings":
        return SapSettings(
            sap_url=self.sap_url or defaults.sap_url,
            access_token=self.access_token or defaults.access_token,
            event_id=self.event_id or defaults.event_id,
            course_area=self.course_area or defaults.course_area,
        )

    def problem(self) -> Optional[str]:
        """Why the uplink can't send with these settings, or None."""
        if not self.sap_url:
            return "No SAP server set"
        if not self.sap_url.startswith(("http://", "https://")):
            return f"SAP server must start with http:// or https://, not '{self.sap_url}'"
        if self.sap_url.endswith("/sailingserver"):
            return "SAP server must not include the /sailingserver path"
        if not self.access_token:
            return "No access token set"
        if self.course_area and not self.event_id:
            return "A course area needs an event"
        return None


def normalized(url: str) -> str:
    return url.strip().rstrip("/")


class SettingsSource:
    def __init__(self, store: Optional[PocketbaseStore], defaults: SapSettings, refresh_seconds: float = 10):
        self.store = store
        self.defaults = defaults
        self.refresh_seconds = refresh_seconds
        self._current: Optional[SapSettings] = None
        self._fetched_at = 0.0

    def get(self, force: bool = False) -> SapSettings:
        """Current settings; re-read every refresh_seconds, the last known ones if PocketBase is down."""
        if not force and self._current is not None and time.monotonic() - self._fetched_at < self.refresh_seconds:
            return self._current
        self._fetched_at = time.monotonic()
        try:
            saved = self._load()
        except Exception as e:
            if self._current is None:
                logger.warning(f"Could not read SAP settings from PocketBase, using the environment: {e}")
                self._current = self.defaults
            else:
                logger.warning(f"Could not read SAP settings from PocketBase, keeping the last ones: {e}")
            return self._current
        self._current = saved.merged_over(self.defaults)
        return self._current

    def _load(self) -> SapSettings:
        if self.store is None:
            return SapSettings()
        record = self.store.get(SETTINGS_COLLECTION, SETTINGS_ID) or {}
        return SapSettings(
            sap_url=normalized(record.get("sap_url") or ""),
            access_token=(record.get("access_token") or "").strip(),
            event_id=(record.get("event_id") or "").strip(),
            course_area=(record.get("course_area") or "").strip(),
        )
