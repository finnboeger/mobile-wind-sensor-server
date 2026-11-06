import unittest
from types import SimpleNamespace
from unittest import mock

from src.backfill_jobs import BackfillWorker
from src.status import RaceUploads
from tests.test_uplink import FakeReader, FakeSap, measurement, ts


class FakeStore:
    def __init__(self, job):
        self.job = job

    def update(self, collection, record_id, data):
        self.job.update(data)


class BackfillWorkerTest(unittest.TestCase):
    def test_sends_the_range_to_the_requested_race_and_reports_the_result(self):
        job = {"id": "b1", "regatta_name": "R", "race_name": "1", "range_from": ts(0), "range_until": ts(2), "status": "pending"}
        reader, sap = FakeReader(), FakeSap()
        reader.records = [measurement(s) for s in (1, 2, 3)]
        target = SimpleNamespace(refresh=lambda: None, sap=sap, backfill_races=lambda race: [race])
        config = SimpleNamespace(exclude_sources=set(), max_batch_size=200, pocketbase_url="", request_timeout_seconds=1, defaults=None)

        with mock.patch("src.backfill_jobs.SapTarget", return_value=target), mock.patch(
            "src.backfill_jobs.PocketbaseReader", return_value=reader
        ):
            BackfillWorker(config, FakeStore(job), RaceUploads(None)).run_job(job)

        self.assertEqual(sap.races, [[{"regattaName": "R", "raceName": "1"}]])
        self.assertEqual((job["status"], job["fixes_sent"], job["fixes_accepted"]), ("done", 2, 2))

    def test_keeps_running_when_pocketbase_fails_mid_job(self):
        jobs = [{"id": "b1", "race_name": "", "range_from": ts(0), "range_until": ts(2), "status": "pending"}]
        worker = None

        class DownStore:
            def list(self, collection, filter_query, sort="", per_page=200):
                if filter_query == 'status = "running"':
                    return []
                if not jobs:
                    worker.stop_event.set()
                return [jobs.pop()] if jobs else []

            def update(self, collection, record_id, data):
                raise RuntimeError("PocketBase down")

        worker = BackfillWorker(SimpleNamespace(), DownStore(), RaceUploads(None))
        with mock.patch("src.backfill_jobs.POLL_SECONDS", 0):
            worker.run()
        self.assertEqual(jobs, [])

    def test_a_bad_range_fails_the_job(self):
        job = {"id": "b1", "race_name": "", "range_from": "", "range_until": ts(2), "status": "pending"}
        BackfillWorker(SimpleNamespace(), FakeStore(job), RaceUploads(None)).run_job(job)
        self.assertEqual(job["status"], "failed")


if __name__ == "__main__":
    unittest.main()
