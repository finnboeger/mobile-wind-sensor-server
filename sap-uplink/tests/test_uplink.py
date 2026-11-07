import os
import tempfile
import unittest
import unittest.mock
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.convert import format_pocketbase_ts
from src.sap_client import SapError, SapRejectedError
from src.settings import SapSettings
from src.status import RaceUploads
from src.steadiness import SteadinessConfig
from src.target import SapTarget
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

    def iter_range(self, after, until, page_size=200):
        matches = sorted((r for r in self.records if after < r["ts"] <= until), key=lambda r: r["ts"])
        for i in range(0, len(matches), page_size):
            yield matches[i : i + page_size]


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

    def test_backfill_sends_range_without_moving_cursor(self):
        self.reader.records = [measurement(s) for s in range(-10, 0)]
        self.reader.records.append(measurement(-3, source_id="bench"))
        uplink = self.make_uplink(exclude_sources={"bench"})
        uplink.backfill(T0 - timedelta(seconds=5), T0)
        self.assertEqual(self.sap.timepoints, [timepoint(s) for s in range(-4, 0)])
        self.assertEqual(uplink.cursor, ts(0))

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

    def test_follows_settings_and_waits_while_unconfigured(self):
        self.reader.records = [measurement(1)]
        settings = SimpleNamespace(value=SapSettings())
        clients = []

        def client(url, bearer_token, timeout_seconds):
            clients.append((url, bearer_token))
            return self.sap

        config = SimpleNamespace(races=[{"regattaName": "R", "raceName": "1"}], all_races=False, request_timeout_seconds=1)
        target = SapTarget(config, SimpleNamespace(get=lambda: settings.value), client_factory=client)
        uplink = self.make_uplink()
        uplink.target = target

        self.assertFalse(uplink.poll_once())
        self.assertEqual((self.sap.calls, uplink.cursor), ([], ts(0)))
        self.assertEqual(uplink.status.fields["problem"], "No SAP server set")

        settings.value = SapSettings(sap_url="https://a.sapsailing.com", access_token="t1")
        uplink.retry_at = 0
        uplink.poll_once()
        self.reader.records.append(measurement(2))
        settings.value = SapSettings(sap_url="https://b.sapsailing.com", access_token="t2")
        uplink.poll_once()

        self.assertEqual(clients, [("https://a.sapsailing.com", "t1"), ("https://b.sapsailing.com", "t2")])
        self.assertEqual(self.sap.timepoints, [timepoint(1), timepoint(2)])
        self.assertTrue(uplink.status.fields["configured"])

    def test_reports_accepted_range_per_race(self):
        self.reader.records = [measurement(s) for s in (1, 2, 3)]
        uploads = RaceUploads(None)
        uplink = self.make_uplink()
        uplink.uploads = uploads
        uplink.forward_new()
        self.assertEqual(
            uploads._pending[("R", "1", "sensor_1")],
            {"first_ms": timepoint(1), "last_ms": timepoint(3), "count": 3, "accepted_at": unittest.mock.ANY},
        )


if __name__ == "__main__":
    unittest.main()


class SteadinessTest(unittest.TestCase):
    """Measurements taken while accelerating or turning are not sent."""

    setUp = UplinkTest.setUp
    tearDown = UplinkTest.tearDown

    def make_uplink(self, **overrides):
        overrides.setdefault("max_batch_size", 50)
        overrides.setdefault("steadiness", SteadinessConfig())
        return UplinkTest.make_uplink(self, **overrides)

    def boat(self, seconds, speed=3.0, heading=90.0):
        return measurement(seconds, sensor_speed_mps=speed, sensor_heading_deg=heading)

    def at(self, seconds):
        return unittest.mock.patch("src.uplink.time.time", return_value=(T0 + timedelta(seconds=seconds)).timestamp())

    def test_drops_measurements_around_acceleration_and_turns(self):
        self.reader.records = (
            [self.boat(s) for s in range(1, 21)]
            + [self.boat(21, speed=4.0)]  # +1 m/s in 1 s: event at 20.5
            + [self.boat(s, speed=4.0) for s in range(22, 41)]
            + [self.boat(41, speed=4.0, heading=110.0)]  # 20°/s: event at 40.5
            + [self.boat(s, speed=4.0, heading=110.0) for s in range(42, 61)]
        )
        uplink = self.make_uplink()
        with self.at(1000):
            while uplink.forward_new():
                pass

        dropped = set(range(16, 26)) | set(range(36, 46))
        self.assertEqual(self.sap.timepoints, [timepoint(s) for s in range(1, 61) if s not in dropped])
        self.assertEqual(uplink.stats["records_unsteady"], len(dropped))
        self.assertEqual(uplink.cursor, ts(60))

    def test_holds_back_recent_measurements_until_later_ones_arrive(self):
        self.reader.records = [self.boat(s) for s in range(1, 11)]
        uplink = self.make_uplink()
        with self.at(11):
            uplink.forward_new()
        # 6..10 can't be judged without what follows them
        self.assertEqual(self.sap.timepoints, [timepoint(s) for s in range(1, 6)])
        self.assertEqual(CursorStore(self.state_file).load(), ts(5))
        self.assertGreater(uplink.hold_until, 0)

        self.reader.records += [self.boat(11, speed=5.0)] + [self.boat(s, speed=5.0) for s in range(12, 16)]
        with self.at(16):
            uplink.forward_new()
        # The event at 10.5 also reaches back into the records sent before
        self.assertEqual(self.sap.timepoints, [timepoint(s) for s in range(1, 6)])
        self.assertEqual(uplink.cursor, ts(10))

        # Nothing newer arrives: sent once they waited long enough
        with self.at(40):
            uplink.forward_new()
        self.assertEqual(uplink.cursor, ts(15))
        self.assertEqual(uplink.stats["records_unsteady"], 10)

    def test_backfill_drops_unsteady_measurements(self):
        self.reader.records = [self.boat(s) for s in range(1, 11)] + [self.boat(s, speed=6.0) for s in range(11, 31)]
        uplink = self.make_uplink(max_batch_size=7)
        uplink.backfill(T0, T0 + timedelta(seconds=30))
        dropped = set(range(6, 16))
        self.assertEqual(self.sap.timepoints, [timepoint(s) for s in range(1, 31) if s not in dropped])

    def test_can_be_turned_off(self):
        self.reader.records = [self.boat(1), self.boat(2, speed=9.0)]
        uplink = self.make_uplink(steadiness=SteadinessConfig(enabled=False))
        uplink.forward_new()
        self.assertEqual(self.sap.timepoints, [timepoint(1), timepoint(2)])
