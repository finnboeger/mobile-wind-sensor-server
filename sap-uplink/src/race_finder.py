"""
Finds the races that should currently receive wind, from SAP's race states.

A race qualifies when it is linked to a tracked race (tracking was started)
and hasn't finished, or finished less than SAP_FINISHED_GRACE_SECONDS ago so
the last minutes still get in. SAP itself only stores fixes that fall inside
a race's tracking interval.

Several qualifying races on one course area (e.g. a race officer forgot to
finish one) all get the wind: they sail in the same wind anyway.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.sap_client import SapClient

logger = logging.getLogger(__name__)


def _parse_time(value: Any) -> Optional[datetime]:
    """Race state times come as epoch millis or ISO strings depending on the server version."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def race_identifier(race_state: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """{regattaName, raceName} of the tracked race behind a race state, or None if not tracked."""
    if not race_state.get("trackedRaceLinked"):
        return None
    race_name = race_state.get("trackedRaceName")
    tracked_race_id = race_state.get("trackedRaceId") or ""
    # trackedRaceId is "<regatta>/<race>"; cut the known race name off the end so
    # slashes inside the regatta name don't matter
    suffix = f"/{race_name}"
    if not race_name or not tracked_race_id.endswith(suffix):
        return None
    return {"regattaName": tracked_race_id[: -len(suffix)], "raceName": race_name}


def select_races(
    race_states: List[Dict[str, Any]],
    now: datetime,
    finished_grace_seconds: float,
    include_finished: bool = False,
) -> List[Dict[str, str]]:
    races: List[Dict[str, str]] = []
    for race_state in race_states:
        race = race_identifier(race_state)
        if race is None or race in races:
            continue
        state = race_state.get("raceState") or {}
        if not include_finished and state.get("lastStatus") == "FINISHED":
            end = _parse_time(state.get("endTime"))
            if end is None or (now - end).total_seconds() > finished_grace_seconds:
                continue
        races.append(race)
    return races


class RaceFinder:
    def __init__(
        self,
        sap: SapClient,
        event_id: str,
        course_area: Optional[str],
        refresh_seconds: float = 30,
        finished_grace_seconds: float = 600,
    ):
        self.sap = sap
        self.event_id = event_id
        self.course_area = course_area
        self.refresh_seconds = refresh_seconds
        self.finished_grace_seconds = finished_grace_seconds
        self._races: Optional[List[Dict[str, str]]] = None
        self._fetched_at = 0.0

    def current_races(self) -> List[Dict[str, str]]:
        """Races to send to now. Cached; on lookup errors the last known races are kept."""
        if self._races is not None and time.monotonic() - self._fetched_at < self.refresh_seconds:
            return self._races
        try:
            states = self.sap.get_race_states(self.event_id, self.course_area)
        except Exception as e:
            if self._races is None:
                raise
            logger.warning(f"Could not refresh race states, keeping {self._describe(self._races)}: {e}")
            self._fetched_at = time.monotonic()
            return self._races

        races = select_races(states, datetime.now(timezone.utc), self.finished_grace_seconds)
        if races != self._races:
            logger.info(f"Current races: {self._describe(races)}")
        self._races = races
        self._fetched_at = time.monotonic()
        return races

    def all_tracked_races(self) -> List[Dict[str, str]]:
        """Every tracked race of the event/course area, finished or not (for backfill)."""
        states = self.sap.get_race_states(self.event_id, self.course_area)
        return select_races(states, datetime.now(timezone.utc), self.finished_grace_seconds, include_finished=True)

    @staticmethod
    def _describe(races: List[Dict[str, str]]) -> str:
        if not races:
            return "none"
        return ", ".join(f"{r['regattaName']} / {r['raceName']}" for r in races)
