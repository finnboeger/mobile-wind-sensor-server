import unittest
from datetime import datetime, timezone

from src.convert import format_pocketbase_ts, parse_pocketbase_ts, to_wind_fix


def record(**overrides):
    base = {
        "id": "abc",
        "ts": "2018-09-15 05:38:00.000Z",
        "gps_lat": 35.299495,
        "gps_lng": 139.493689,
        "true_wind_dir_deg": 300.0,
        "true_wind_speed_mps": 8.951,
    }
    base.update(overrides)
    return base


class ToWindFixTest(unittest.TestCase):
    def test_matches_sap_example(self):
        # SAP's docs: 17.4 kn from 300 deg is sent as direction 120 at 1536989880000
        fix = to_wind_fix(record())
        self.assertEqual(
            fix,
            {
                "position": {"latitude_deg": 35.299495, "longitude_deg": 139.493689},
                "timepoint": 1536989880000,
                "direction": 120.0,
                "speedinknots": 17.4,
            },
        )

    def test_direction_wraps(self):
        self.assertEqual(to_wind_fix(record(true_wind_dir_deg=90))["direction"], 270)
        self.assertEqual(to_wind_fix(record(true_wind_dir_deg=180))["direction"], 0)
        self.assertEqual(to_wind_fix(record(true_wind_dir_deg=359.5))["direction"], 179.5)

    def test_skips_records_without_position_or_wind(self):
        self.assertIsNone(to_wind_fix(record(gps_lat=None)))
        self.assertIsNone(to_wind_fix(record(gps_lng=None)))
        self.assertIsNone(to_wind_fix(record(gps_lat=0, gps_lng=0)))
        self.assertIsNone(to_wind_fix(record(true_wind_dir_deg=None)))
        self.assertIsNone(to_wind_fix(record(true_wind_speed_mps=float("nan"))))
        self.assertIsNone(to_wind_fix(record(gps_lat=95)))

    def test_zero_wind_is_sent(self):
        fix = to_wind_fix(record(true_wind_dir_deg=0, true_wind_speed_mps=0))
        self.assertEqual((fix["direction"], fix["speedinknots"]), (180, 0))


class TimestampTest(unittest.TestCase):
    def test_round_trip(self):
        ts = datetime(2026, 10, 28, 17, 3, 29, 123000, tzinfo=timezone.utc)
        text = format_pocketbase_ts(ts)
        self.assertEqual(text, "2026-10-28 17:03:29.123Z")
        self.assertEqual(parse_pocketbase_ts(text), ts)

    def test_parses_iso_with_offset(self):
        self.assertEqual(
            parse_pocketbase_ts("2026-10-28T17:03:29+00:00"),
            datetime(2026, 10, 28, 17, 3, 29, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()
