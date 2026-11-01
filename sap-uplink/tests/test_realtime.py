import os
import queue
import tempfile
import unittest
from types import SimpleNamespace
from contextlib import contextmanager
from unittest import mock

from src.realtime import RealtimeListener
from src.uplink import ALL_SOURCES, CursorStore, Uplink
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


class RealtimeTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_file = os.path.join(tmp.name, "cursor.json")
        self.reader = FakeReader()
        self.sap = FakeSap()

    def make_uplink(self, sources):
        config = SimpleNamespace(sources=sources, start_from=T0, batch_size=3, poll_seconds=3600)
        uplink = Uplink(config, self.reader, self.sap, CursorStore(self.state_file))
        uplink.init_cursors()
        return uplink

    def test_event_forwards_only_that_source_right_away(self):
        uplink = self.make_uplink({"sensor_1": "windbot1", "sensor_2": "windbot2"})

        def new_measurements():
            self.reader.records = [measurement(1), measurement(1, source_id="sensor_2")]

        with self.assertRaises(StopLoop):
            uplink.run_forever(ScriptedQueue([(new_measurements, "sensor_2")]))

        self.assertEqual([w for w, _ in self.sap.calls], ["windbot2"])
        self.assertEqual(uplink.cursors["sensor_1"], measurement(0)["created_at"])

    def test_dropped_connection_resubscribes_and_catches_up(self):
        wakeups = queue.Queue()
        listener = RealtimeListener("http://pocketbase:8090", ["sensor_1"], wakeups.put, lambda: wakeups.put(ALL_SOURCES))
        streams = [
            # first connection announces one record, then drops
            sse("PB_CONNECT", '{"clientId":"a"}')
            + sse("measurements/*", '{"action":"create","record":{"source_id":"sensor_1"}}')
            + sse("measurements/*", '{"action":"create","record":{"source_id":"other"}}'),
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
        self.assertEqual(list(wakeups.queue), [ALL_SOURCES, "sensor_1", ALL_SOURCES])

        # what arrived while disconnected is sent from the cursor on the catch-up wake-up
        uplink = self.make_uplink({"sensor_1": "windbot1"})

        def arrived_while_disconnected():
            self.reader.records = [measurement(1), measurement(2)]

        with self.assertRaises(StopLoop):
            uplink.run_forever(ScriptedQueue([(arrived_while_disconnected, wakeups.queue[2])]))
        self.assertEqual(self.sap.timepoints, [timepoint(1), timepoint(2)])


if __name__ == "__main__":
    unittest.main()
