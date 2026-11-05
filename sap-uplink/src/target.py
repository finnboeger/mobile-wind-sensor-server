"""
Where the wind goes: the SAP client and the race selection for the current
settings. Rebuilt whenever the settings change.
"""

import logging
import time
from typing import Dict, List, Optional

from src.config import UplinkConfig
from src.race_finder import RaceFinder
from src.sap_client import SapClient, SapError, SapRejectedError
from src.settings import SapSettings, SettingsSource

logger = logging.getLogger(__name__)

# How often to look the event up again after SAP said it doesn't exist (it may
# just not be published yet)
REJECTED_RECHECK_SECONDS = 60


class SapTarget:
    def __init__(self, config: UplinkConfig, settings_source: SettingsSource, client_factory=SapClient):
        self.config = config
        self.settings_source = settings_source
        self.client_factory = client_factory
        self.settings: Optional[SapSettings] = None
        self.sap: Optional[SapClient] = None
        self.finder: Optional[RaceFinder] = None
        self.problem: Optional[str] = "Not configured yet"
        # When to check a rejected event again (monotonic), None if it wasn't rejected
        self._recheck_at: Optional[float] = None

    def refresh(self) -> Optional[str]:
        """Pick up changed settings; returns why nothing can be sent, or None."""
        settings = self.settings_source.get()
        recheck = self._recheck_at is not None and time.monotonic() >= self._recheck_at
        if settings != self.settings or recheck:
            self._build(settings)
        return self.problem

    def current_races(self) -> Optional[List[Dict[str, str]]]:
        """Races to send live wind to (None: every tracked race the login may update)."""
        if self.config.races:
            return self.config.races
        if self.finder:
            return self.finder.current_races()
        return None

    def backfill_races(self, race: Optional[Dict[str, str]] = None) -> Optional[List[Dict[str, str]]]:
        """
        Races for a backfill: the given one, or every race that may have been
        running back then. SAP only keeps fixes inside each race's tracking interval.
        """
        if race:
            return [race]
        if self.config.races:
            return self.config.races
        if self.finder:
            return self.finder.all_tracked_races()
        return None

    def _build(self, settings: SapSettings) -> None:
        self.settings = settings
        self.sap = self.finder = None
        self._recheck_at = None
        self.problem = settings.problem()
        if not self.problem and not (self.config.races or settings.event_id or self.config.all_races):
            self.problem = "No event set (or SAP_RACES / SAP_ALL_RACES)"
        if self.problem:
            logger.warning(f"Not sending to SAP: {self.problem}")
            return

        self.sap = self.client_factory(
            settings.sap_url,
            bearer_token=settings.access_token,
            timeout_seconds=self.config.request_timeout_seconds,
        )
        if self.config.races:
            races = ", ".join(f"{r['regattaName']} / {r['raceName']}" for r in self.config.races)
            logger.info(f"Sending to {settings.sap_url}, races: {races}")
        elif settings.event_id:
            self.finder = RaceFinder(
                self.sap,
                settings.event_id,
                settings.course_area or None,
                refresh_seconds=self.config.race_refresh_seconds,
            )
            try:
                self.finder.check()
            except SapRejectedError as e:
                self.problem = str(e)
                self._recheck_at = time.monotonic() + REJECTED_RECHECK_SECONDS
                logger.error(f"Not sending to SAP: {e} (checking again in {REJECTED_RECHECK_SECONDS}s)")
                return
            except SapError as e:
                # SAP unreachable right now: the lookup is retried with every batch
                logger.warning(f"Could not check event {settings.event_id} yet: {e}")
            area = f"course area '{settings.course_area}'" if settings.course_area else "all course areas"
            logger.info(f"Sending to {settings.sap_url}, races: current races of event {settings.event_id}, {area}")
        else:
            logger.warning(f"Sending to {settings.sap_url}, races: every tracked race this login may update (SAP_ALL_RACES=true)")
