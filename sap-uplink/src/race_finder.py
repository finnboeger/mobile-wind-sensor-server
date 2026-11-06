"""
Finds the races that should currently receive wind, from SAP's race states.

A race qualifies when it is linked to a tracked race (tracking was started)
and hasn't finished, or was first seen finished less than
FINISHED_GRACE_SECONDS ago. SAP itself only stores fixes that fall inside a
race's tracking interval.

Several qualifying races on one course area (e.g. a race officer forgot to
finish one) all get the wind: they sail in the same wind anyway.
"""

import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.sap_client import SapClient

logger = logging.getLogger(__name__)

# SAP keeps fixes up to 3 minutes after a race ends (IS_LIVE_GRACE_PERIOD_IN_MILLIS)
# and ignores later ones itself. Keep sending to a finished race a while longer so
# measurements that reach PocketBase late (e.g. after a mobile data gap) still
# get to it instead of being dropped.
FINISHED_GRACE_SECONDS = 600


def is_finished(race_state: Dict[str, Any]) -> bool:
    return (race_state.get("raceState") or {}).get("lastStatus") == "FINISHED"


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
    keep_finished: Callable[[Dict[str, str]], bool] = lambda race: False,
) -> List[Dict[str, str]]:
    """Tracked races; finished ones only where keep_finished(race) says so."""
    races: List[Dict[str, str]] = []
    for race_state in race_states:
        race = race_identifier(race_state)
        if race is None or race in races:
            continue
        if is_finished(race_state) and not keep_finished(race):
            continue
        races.append(race)
    return races


def tracked_races(race_states: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tracked: List[Dict[str, Any]] = []
    for race_state in race_states:
        race = race_identifier(race_state)
        if race is not None and not any(t["regattaName"] == race["regattaName"] and t["raceName"] == race["raceName"] for t in tracked):
            tracked.append({**race, "finished": is_finished(race_state)})
    return tracked


class RaceFinder:
    def __init__(
        self,
        sap: SapClient,
        event_id: str,
        course_area: Optional[str],
        refresh_seconds: float = 30,
    ):
        self.sap = sap
        self.event_id = event_id
        self.course_area = course_area
        self.refresh_seconds = refresh_seconds
        self._races: Optional[List[Dict[str, str]]] = None
        # Every tracked race of the last lookup, with "finished", for the status
        self.tracked: List[Dict[str, Any]] = []
        # (regattaName, raceName) -> when the race was first seen finished (monotonic)
        self._finished_since: Dict[Tuple[str, str], float] = {}
        self._fetched_at = 0.0

    def check(self) -> None:
        """
        Look the event up once at startup, so a wrong SAP_EVENT_ID fails right
        away (SapRejectedError) and a course area without races is reported.
        """
        states = self.sap.get_race_states(self.event_id)
        if self.course_area:
            areas = sorted({s["courseAreaName"] for s in states if s.get("courseAreaName")})
            if self.course_area not in areas:
                logger.warning(
                    f"Course area '{self.course_area}' has no races in event {self.event_id} "
                    f"(course areas with races: {', '.join(areas) or 'none'})"
                )

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

        races = select_races(states, self._recently_finished(states))
        self.tracked = tracked_races(states)
        if races != self._races:
            logger.info(f"Current races: {self._describe(races)}")
        self._races = races
        self._fetched_at = time.monotonic()
        return races

    @property
    def last_races(self) -> Optional[List[Dict[str, str]]]:
        return self._races

    def all_tracked_races(self) -> List[Dict[str, str]]:
        """Every tracked race of the event/course area, finished or not (for backfill)."""
        states = self.sap.get_race_states(self.event_id, self.course_area)
        return select_races(states, keep_finished=lambda race: True)

    def _recently_finished(self, race_states: List[Dict[str, Any]]) -> Callable[[Dict[str, str]], bool]:
        """Note when races are first seen finished; returns which ones are still in their grace time."""
        now = time.monotonic()
        finished = set()
        for race_state in race_states:
            race = race_identifier(race_state)
            if race is not None and is_finished(race_state):
                key = (race["regattaName"], race["raceName"])
                finished.add(key)
                self._finished_since.setdefault(key, now)
        # A race that is no longer finished (e.g. the finish was undone) starts over
        for key in set(self._finished_since) - finished:
            del self._finished_since[key]
        return lambda race: now - self._finished_since[(race["regattaName"], race["raceName"])] < FINISHED_GRACE_SECONDS

    @staticmethod
    def _describe(races: List[Dict[str, str]]) -> str:
        if not races:
            return "none"
        return ", ".join(f"{r['regattaName']} / {r['raceName']}" for r in races)
