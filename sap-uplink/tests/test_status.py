import unittest

from src.status import RaceUploads, upload_record_id

RACE = {"regattaName": "Windbot Test", "raceName": "R1"}


class FakeStore:
    def __init__(self):
        self.records = {}

    def get(self, collection, record_id):
        return self.records.get((collection, record_id))

    def upsert(self, collection, record_id, data):
        self.records[(collection, record_id)] = {**self.records.get((collection, record_id), {}), **data}


class RaceUploadsTest(unittest.TestCase):
    def test_extends_the_saved_range_per_race_and_source(self):
        store = FakeStore()
        record_id = upload_record_id("Windbot Test", "R1", "sensor_1")
        # Saved by an earlier run of the service
        store.records[("sap_race_uploads", record_id)] = {
            "first_fix": "2026-10-28 12:00:10.000Z",
            "last_fix": "2026-10-28 12:00:20.000Z",
            "fixes_accepted": 11,
        }
        uploads = RaceUploads(store)
        t0 = 1793188800000  # 2026-10-28 12:00:00Z
        uploads.record("sensor_1", RACE, t0 + 30_000, t0 + 40_000, 2)
        uploads.record("sensor_1", RACE, t0 + 5_000, t0 + 6_000, 2)
        uploads.record("sensor_2", RACE, t0, t0, 1)
        uploads.flush(force=True)

        saved = store.records[("sap_race_uploads", record_id)]
        self.assertEqual(
            (saved["first_fix"], saved["last_fix"], saved["fixes_accepted"]),
            ("2026-10-28 12:00:05.000Z", "2026-10-28 12:00:40.000Z", 15),
        )
        other = store.records[("sap_race_uploads", upload_record_id("Windbot Test", "R1", "sensor_2"))]
        self.assertEqual(other["source_id"], "sensor_2")


if __name__ == "__main__":
    unittest.main()
