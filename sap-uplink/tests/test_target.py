import unittest
from types import SimpleNamespace
from unittest import mock

from src.sap_client import SapRejectedError
from src.settings import SapSettings
from src.target import REJECTED_RECHECK_SECONDS, SapTarget

SETTINGS = SapSettings(sap_url="https://a.sapsailing.com", access_token="t", event_id="e1")


class EventSap:
    def __init__(self):
        self.exists = False

    def get_race_states(self, event_id, course_area=None):
        if not self.exists:
            raise SapRejectedError(f"event {event_id} not found")
        return []


class SapTargetTest(unittest.TestCase):
    def test_checks_a_rejected_event_again_later(self):
        sap = EventSap()
        config = SimpleNamespace(races=None, all_races=False, request_timeout_seconds=1, race_refresh_seconds=30)
        target = SapTarget(config, SimpleNamespace(get=lambda: SETTINGS), client_factory=lambda *a, **k: sap)
        clock = [1000.0]
        with mock.patch("src.target.time.monotonic", side_effect=lambda: clock[0]):
            self.assertIn("not found", target.refresh())
            # Published meanwhile, but not checked before the delay is up
            sap.exists = True
            self.assertIn("not found", target.refresh())
            clock[0] += REJECTED_RECHECK_SECONDS
            self.assertIsNone(target.refresh())
            self.assertIsNotNone(target.finder)


if __name__ == "__main__":
    unittest.main()
