import unittest
from datetime import datetime, timedelta, timezone

from src.race_finder import RaceFinder, race_identifier, select_races
from src.sap_client import SapError

NOW = datetime(2026, 10, 31, 22, 0, tzinfo=timezone.utc)


def race_state(race, status="UNSCHEDULED", linked=True, end=None, regatta="Windbot Test"):
    # Shape as returned by /events/{id}/racestates on dev.sapsailing.com
    tracked_name = f"{regatta} {race} Default"
    return {
        "raceName": race,
        "fleetName": "Default",
        "trackedRaceLinked": linked,
        "trackedRaceName": tracked_name if linked else None,
        "trackedRaceId": f"{regatta}/{tracked_name}" if linked else None,
        "raceState": {
            "startTime": None,
            "endTime": int(end.timestamp() * 1000) if end else None,
            "lastStatus": status,
        },
        "courseAreaName": "Default",
        "leaderboardName": regatta,
    }


def ident(race, regatta="Windbot Test"):
    return {"regattaName": regatta, "raceName": f"{regatta} {race} Default"}


class SelectRacesTest(unittest.TestCase):
    def test_picks_tracked_unfinished_races(self):
        states = [
            race_state("R1", "RUNNING"),
            race_state("R2", "UNSCHEDULED", linked=False),
            race_state("R3", "FINISHED", end=NOW - timedelta(hours=1)),
        ]
        self.assertEqual(select_races(states, NOW, 600), [ident("R1")])

    def test_keeps_just_finished_race_during_grace(self):
        states = [race_state("R1", "FINISHED", end=NOW - timedelta(minutes=5))]
        self.assertEqual(select_races(states, NOW, 600), [ident("R1")])

    def test_several_active_races_all_get_wind(self):
        states = [race_state("R1", "RUNNING"), race_state("R2", "SCHEDULED")]
        self.assertEqual(select_races(states, NOW, 600), [ident("R1"), ident("R2")])

    def test_include_finished_for_backfill(self):
        states = [race_state("R1", "FINISHED", end=NOW - timedelta(days=1))]
        self.assertEqual(select_races(states, NOW, 600, include_finished=True), [ident("R1")])

    def test_regatta_name_with_slash(self):
        self.assertEqual(race_identifier(race_state("R1", regatta="KW 2027 / J70")), ident("R1", "KW 2027 / J70"))

    def test_iso_end_time(self):
        state = race_state("R1", "FINISHED")
        state["raceState"]["endTime"] = (NOW - timedelta(minutes=1)).isoformat()
        self.assertEqual(select_races([state], NOW, 600), [ident("R1")])


class FakeSap:
    def __init__(self):
        self.states = []
        self.fail = False
        self.calls = []

    def get_race_states(self, event_id, course_area):
        self.calls.append((event_id, course_area))
        if self.fail:
            raise SapError("down")
        return self.states


class RaceFinderTest(unittest.TestCase):
    def test_filters_by_course_area_and_caches(self):
        sap = FakeSap()
        sap.states = [race_state("R1", "RUNNING")]
        finder = RaceFinder(sap, "event-id", "Alpha", refresh_seconds=60)
        self.assertEqual(finder.current_races(), [ident("R1")])
        finder.current_races()
        self.assertEqual(sap.calls, [("event-id", "Alpha")])

    def test_keeps_last_races_when_lookup_fails(self):
        sap = FakeSap()
        sap.states = [race_state("R1", "RUNNING")]
        finder = RaceFinder(sap, "event-id", None, refresh_seconds=0)
        finder.current_races()
        sap.fail = True
        self.assertEqual(finder.current_races(), [ident("R1")])

    def test_first_lookup_failure_raises(self):
        sap = FakeSap()
        sap.fail = True
        with self.assertRaises(SapError):
            RaceFinder(sap, "event-id", None).current_races()


if __name__ == "__main__":
    unittest.main()
