import threading
import unittest
from unittest import mock

from src.status import UplinkStatus


class FakeStore:
    def __init__(self):
        self.writes = []
        self.written = threading.Event()

    def upsert(self, collection, record_id, data):
        self.writes.append(data)
        self.written.set()


class HeartbeatTest(unittest.TestCase):
    def test_writes_without_the_forwarding_loop(self):
        store = FakeStore()
        status = UplinkStatus(store)
        status.update(cursor="c1")
        with mock.patch("src.status.WRITE_INTERVAL_SECONDS", 0.01):
            status.start()
            self.assertTrue(store.written.wait(2))
            status.stop_event.set()
        self.assertEqual(store.writes[0]["cursor"], "c1")
        self.assertIn("heartbeat_at", store.writes[0])

    def test_one_write_per_interval(self):
        store = FakeStore()
        status = UplinkStatus(store)
        status.tick()
        status.tick()
        self.assertEqual(len(store.writes), 1)


if __name__ == "__main__":
    unittest.main()
