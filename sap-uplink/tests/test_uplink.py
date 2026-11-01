import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.convert import format_pocketbase_ts
from src.sap_client import SapError, SapRejectedError
from src.uplink import CursorStore, Uplink

T0 = datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)


def ts(seconds):
    return format_pocketbase_ts(T0 + timedelta(seconds=seconds))


def measurement(seconds, source_id="sensor_1", **overrides):
    record = {
        "id": f"{source_id}-{seconds}",
        "source_id": source_id,
        "ts": ts(seconds),
        "created_at": ts(seconds),
        "gps_lat": 54.3,
        "gps_lng": 10.1,
        "true_wind_dir_deg": 270.0,
        "true_wind_speed_mps": 5.0,
    }
    record.update(overrides)
    return record


class FakeReader:
    """Mimics PocketBase's filter/sort/paging on an in-memory list."""

    def __init__(self):
        self.records = []

    def list_new(self, after, limit=200):
        matches = sorted(
            (r for r in self.records if r["created_at"] > after),
            key=lambda r: r["created_at"],
        )
        return matches[:limit]


class FakeSap:
    def __init__(self):
        self.calls = []
        self.races = []
        self.fail_with = None
        self.fail_for = set()  # sources that get a SapError

    def put_wind(self, wind_source_id, fixes, races=None):
        if self.fail_with:
            raise self.fail_with
        if wind_source_id in self.fail_for:
            raise SapError("down")
        self.calls.append((wind_source_id, fixes))
        self.races.append(races)
        return [{"regattaNameAndRaceName": {"regattaName": "R", "raceName": "1"}, "answer": [True] * len(fixes)}]

    @property
    def timepoints(self):
        return [fix["timepoint"] for _, fixes in self.calls for fix in fixes]


def timepoint(seconds):
    return int((T0 + timedelta(seconds=seconds)).timestamp() * 1000)


class UplinkTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_file = os.path.join(self.tmp.name, "cursor.json")
        self.reader = FakeReader()
        self.sap = FakeSap()

    def tearDown(self):
        self.tmp.cleanup()

    def make_uplink(self, races_provider=lambda: None, **overrides):
        config = SimpleNamespace(
            exclude_sources=set(),
            start_from=T0,
            max_batch_size=3,
            poll_seconds=5,
            state_file=self.state_file,
        )
        for key, value in overrides.items():
            setattr(config, key, value)
        uplink = Uplink(config, self.reader, self.sap, CursorStore(self.state_file), races_provider)
        uplink.init_cursor()
        return uplink

    def test_forwards_new_records_in_batches_and_saves_cursor(self):
        self.reader.records = [measurement(s) for s in range(-5, 8)]  # before start_from are ignored
        uplink = self.make_uplink()

        while uplink.poll_once():
            pass

        self.assertEqual(self.sap.timepoints, [timepoint(s) for s in range(1, 8)])
        self.assertEqual([len(f) for _, f in self.sap.calls], [3, 3, 1])
        self.assertEqual({w for w, _ in self.sap.calls}, {"sensor_1"})
        self.assertEqual(CursorStore(self.state_file).load(), ts(7))
        self.assertEqual(uplink.stats["fixes_accepted"], 7)

    def test_late_measurement_is_sent(self):
        self.reader.records = [measurement(s) for s in (1, 2, 5)]
        uplink = self.make_uplink()
        uplink.forward_new()

        # Measured at 3, but only reached PocketBase after 5
        self.reader.records.append(measurement(3, created_at=ts(6)))
        uplink.forward_new()

        self.assertEqual(self.sap.timepoints, [timepoint(s) for s in (1, 2, 5, 3)])
        self.assertEqual(uplink.cursor, ts(6))

    def test_keeps_cursor_while_sap_is_down(self):
        self.reader.records = [measurement(s) for s in range(1, 3)]
        uplink = self.make_uplink()

        self.sap.fail_with = SapError("down")
        with self.assertRaises(SapError):
            uplink.forward_new()
        self.assertEqual(uplink.cursor, ts(0))

        self.sap.fail_with = None
        uplink.forward_new()
        self.assertEqual(self.sap.timepoints, [timepoint(1), timepoint(2)])

    def test_rejected_batch_is_skipped_not_retried_forever(self):
        self.reader.records = [measurement(1)]
        uplink = self.make_uplink()
        self.sap.fail_with = SapRejectedError("bad")
        uplink.forward_new()
        self.assertEqual(uplink.cursor, ts(1))
        self.assertEqual(uplink.stats["batches_rejected"], 1)

    def test_records_without_position_advance_cursor_without_a_request(self):
        self.reader.records = [measurement(1, gps_lat=None), measurement(2, gps_lat=None)]
        uplink = self.make_uplink()
        uplink.forward_new()
        self.assertEqual(self.sap.calls, [])
        self.assertEqual(uplink.cursor, ts(2))

    def test_sends_each_source_under_its_own_id(self):
        self.reader.records = [
            measurement(1),
            measurement(1, source_id="sensor_2", created_at=ts(2)),
            measurement(2, created_at=ts(3)),
        ]
        uplink = self.make_uplink()
        uplink.forward_new()
        self.assertEqual([(w, len(f)) for w, f in self.sap.calls], [("sensor_1", 2), ("sensor_2", 1)])
        self.assertEqual(uplink.cursor, ts(3))

    def test_excluded_sources_are_skipped(self):
        self.reader.records = [measurement(1), measurement(1, source_id="bench", created_at=ts(2))]
        uplink = self.make_uplink(exclude_sources={"bench"})
        uplink.forward_new()
        self.assertEqual([w for w, _ in self.sap.calls], ["sensor_1"])
        self.assertEqual(uplink.cursor, ts(2))

    def test_failure_backs_off_and_retries_the_batch(self):
        self.reader.records = [measurement(1), measurement(1, source_id="sensor_2", created_at=ts(2))]
        uplink = self.make_uplink()

        # sensor_2 fails after sensor_1 went through: the cursor stays and the uplink backs off
        self.sap.fail_for = {"sensor_2"}
        self.assertFalse(uplink.poll_once())
        self.assertEqual(uplink.cursor, ts(0))
        self.assertEqual(uplink.failures, 1)
        self.assertFalse(uplink.poll_once())  # still backing off
        self.assertEqual([w for w, _ in self.sap.calls], ["sensor_1"])

        # Once the backoff has passed the batch is sent again; SAP ignores the repeated fix
        self.sap.fail_for = set()
        uplink.retry_at = 0
        uplink.poll_once()
        self.assertEqual([w for w, _ in self.sap.calls], ["sensor_1", "sensor_1", "sensor_2"])
        self.assertEqual(uplink.cursor, ts(2))
        self.assertEqual(uplink.failures, 0)

    def test_backoff_is_capped(self):
        uplink = self.make_uplink()
        self.assertEqual(uplink.backoff_seconds(1), 10)
        self.assertEqual(uplink.backoff_seconds(5000), 300)

    def test_sends_to_current_races(self):
        self.reader.records = [measurement(1)]
        races = [{"regattaName": "Windbot Test", "raceName": "Windbot Test R1 Default"}]
        uplink = self.make_uplink(races_provider=lambda: races)
        uplink.forward_new()
        self.assertEqual(self.sap.races, [races])

    def test_no_current_race_drops_fixes_and_moves_on(self):
        self.reader.records = [measurement(1)]
        uplink = self.make_uplink(races_provider=lambda: [])
        uplink.forward_new()
        self.assertEqual(self.sap.calls, [])
        self.assertEqual(uplink.cursor, ts(1))


if __name__ == "__main__":
    unittest.main()
