import unittest
from unittest import mock

from src.race_finder import FINISHED_GRACE_SECONDS, RaceFinder, race_identifier, select_races
from src.sap_client import SapError


def race_state(race, status="UNSCHEDULED", linked=True, regatta="Windbot Test"):
    # Shape as written by SAP's RaceStateSerializer: times are TimePoint.toString(),
    # i.e. Java's Date.toString() in the server's time zone plus the milliseconds
    tracked_name = f"{regatta} {race} Default"
    return {
        "raceName": race,
        "fleetName": "Default",
        "trackedRaceLinked": linked,
        "trackedRaceName": tracked_name if linked else None,
        "trackedRaceId": f"{regatta}/{tracked_name}" if linked else None,
        "raceState": {
            "startTime": "Sat Oct 31 21:00:00 UTC 2026",
            "endTime": "Sat Oct 31 21:55:00 UTC 2026 (+123ms)" if status == "FINISHED" else None,
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
            race_state("R3", "FINISHED"),
        ]
        self.assertEqual(select_races(states), [ident("R1")])

    def test_several_active_races_all_get_wind(self):
        states = [race_state("R1", "RUNNING"), race_state("R2", "SCHEDULED")]
        self.assertEqual(select_races(states), [ident("R1"), ident("R2")])

    def test_include_finished_for_backfill(self):
        states = [race_state("R1", "FINISHED")]
        self.assertEqual(select_races(states, keep_finished=lambda race: True), [ident("R1")])

    def test_regatta_name_with_slash(self):
        self.assertEqual(race_identifier(race_state("R1", regatta="KW 2027 / J70")), ident("R1", "KW 2027 / J70"))


class FakeSap:
    def __init__(self):
        self.states = []
        self.fail = False
        self.calls = []

    def get_race_states(self, event_id, course_area=None):
        self.calls.append((event_id, course_area))
        if self.fail:
            raise SapError("down")
        return self.states


class RaceFinderTest(unittest.TestCase):
    def test_keeps_finished_race_for_grace_time_from_first_seen(self):
        sap = FakeSap()
        sap.states = [race_state("R1", "RUNNING")]
        finder = RaceFinder(sap, "event-id", None, refresh_seconds=0)
        with mock.patch("src.race_finder.time.monotonic") as clock:
            clock.return_value = 1000.0
            finder.current_races()
            sap.states = [race_state("R1", "FINISHED")]
            clock.return_value = 1030.0
            self.assertEqual(finder.current_races(), [ident("R1")])
            clock.return_value = 1030.0 + FINISHED_GRACE_SECONDS - 1
            self.assertEqual(finder.current_races(), [ident("R1")])
            clock.return_value = 1030.0 + FINISHED_GRACE_SECONDS
            self.assertEqual(finder.current_races(), [])

            # A reopened race that finishes again gets a new grace time
            sap.states = [race_state("R1", "RUNNING")]
            finder.current_races()
            sap.states = [race_state("R1", "FINISHED")]
            self.assertEqual(finder.current_races(), [ident("R1")])

    def test_check_warns_about_course_area_without_races(self):
        sap = FakeSap()
        sap.states = [race_state("R1", "RUNNING")]
        with self.assertLogs("src.race_finder", "WARNING") as logs:
            RaceFinder(sap, "event-id", "Alpha").check()
        self.assertIn("course areas with races: Default", logs.output[0])
        self.assertEqual(sap.calls, [("event-id", None)])

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
