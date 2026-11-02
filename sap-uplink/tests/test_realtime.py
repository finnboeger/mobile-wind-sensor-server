import os
import queue
import tempfile
import unittest
from types import SimpleNamespace
from contextlib import contextmanager
from unittest import mock

from src.realtime import RealtimeListener
from src.uplink import CursorStore, Uplink
from tests.test_uplink import T0, FakeReader, FakeSap, measurement, timepoint


def sse(event, data):
    return [f"event:{event}", f"data:{data}", ""]


class StopLoop(Exception):
    pass


class ScriptedQueue:
    """Hands out wake-ups one by one, running a setup step before each; ends the loop when empty."""

    def __init__(self, steps):
        self.steps = list(steps)

    def get(self, timeout=None):
        if not self.steps:
            raise StopLoop
        setup, item = self.steps.pop(0)
        setup()
        return item

    def get_nowait(self):
        raise queue.Empty


class RealtimeTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_file = os.path.join(tmp.name, "cursor.json")
        self.reader = FakeReader()
        self.sap = FakeSap()

    def make_uplink(self):
        config = SimpleNamespace(exclude_sources=set(), start_from=T0, max_batch_size=3, poll_seconds=3600)
        uplink = Uplink(config, self.reader, self.sap, CursorStore(self.state_file))
        uplink.init_cursor()
        return uplink

    def test_wake_up_forwards_right_away(self):
        uplink = self.make_uplink()

        def new_measurement():
            self.reader.records = [measurement(1)]

        # poll_seconds is an hour, so only the wake-up can have sent it
        with self.assertRaises(StopLoop):
            uplink.run_forever(ScriptedQueue([(new_measurement, None)]))
        self.assertEqual(self.sap.timepoints, [timepoint(1)])

    def test_dropped_connection_resubscribes_and_catches_up(self):
        wakeups = queue.Queue()
        listener = RealtimeListener("http://pocketbase:8090", {"bench"}, lambda: wakeups.put("wake"))
        streams = [
            # first connection announces one record, then drops
            sse("PB_CONNECT", '{"clientId":"a"}')
            + sse("measurements/*", '{"action":"create","record":{"source_id":"sensor_1"}}')
            + sse("measurements/*", '{"action":"create","record":{"source_id":"bench"}}'),
            sse("PB_CONNECT", '{"clientId":"b"}'),
        ]

        def lines(stream, last):
            yield from stream
            if last:
                listener.stop()  # end after the second connection

        @contextmanager
        def get(url, **kwargs):
            stream = streams.pop(0)
            yield mock.Mock(iter_lines=mock.Mock(return_value=lines(stream, last=not streams)))

        listener.session = mock.Mock(get=get)
        listener.stop_event.wait = lambda seconds: None
        listener.run()

        client_ids = [c.kwargs["json"]["clientId"] for c in listener.session.post.call_args_list]
        self.assertEqual(client_ids, ["a", "b"])
        self.assertEqual(listener.session.post.call_args.kwargs["json"]["subscriptions"], ["measurements/*"])
        # connect, sensor_1's record (not the excluded bench), reconnect
        self.assertEqual(wakeups.qsize(), 3)

        # what arrived while disconnected is sent from the cursor on the catch-up wake-up
        uplink = self.make_uplink()

        def arrived_while_disconnected():
            self.reader.records = [measurement(1), measurement(2)]

        with self.assertRaises(StopLoop):
            uplink.run_forever(ScriptedQueue([(arrived_while_disconnected, None)]))
        self.assertEqual(self.sap.timepoints, [timepoint(1), timepoint(2)])


if __name__ == "__main__":
    unittest.main()
