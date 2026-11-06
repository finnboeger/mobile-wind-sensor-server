"""
PocketBase realtime subscription, used only as a wake-up signal.

PocketBase realtime is Server-Sent Events on /api/realtime: the server first
sends a PB_CONNECT event with a clientId, the client then POSTs its topics to
/api/realtime, and from then on every matching record change arrives as an
event named after the topic with {"action": ..., "record": {...}} as data.

The listener doesn't hand records on. For a new measurement of a source that
isn't excluded it only wakes the main loop, which reads through its cursor as
usual. A dropped connection therefore loses nothing: after reconnecting it
wakes the main loop once more to catch up.
"""

import json
import logging
import threading
from typing import Callable, Iterable, Iterator, Optional, Tuple

import requests

from src.pocketbase_reader import MEASUREMENTS_COLLECTION

logger = logging.getLogger(__name__)

TOPIC = f"{MEASUREMENTS_COLLECTION}/*"
MAX_RECONNECT_SECONDS = 60
# PocketBase drops realtime clients that received nothing for 5 minutes; a read
# timeout a bit above that also catches connections that died silently
READ_TIMEOUT_SECONDS = 330


def iter_sse(lines: Iterable[str]) -> Iterator[Tuple[str, str]]:
    """Yield (event, data) pairs from the lines of an SSE stream."""
    event, data = "message", []
    for line in lines:
        if line == "":
            if data:
                yield event, "\n".join(data)
            event, data = "message", []
        elif line.startswith(":"):
            continue
        else:
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "event":
                event = value
            elif field == "data":
                data.append(value)


class RealtimeListener:
    def __init__(
        self,
        pocketbase_url: str,
        exclude_sources: Iterable[str],
        on_wake: Callable[[], None],
        connect_timeout_seconds: float = 30,
    ):
        """
        on_wake() is called for every new measurement of a source that isn't
        excluded, and after every (re)subscribe so the caller can catch up. It
        runs on the listener thread and must only hand the work over.
        """
        self.url = f"{pocketbase_url}/api/realtime"
        self.exclude_sources = set(exclude_sources)
        self.on_wake = on_wake
        self.connect_timeout_seconds = connect_timeout_seconds
        self.session = requests.Session()
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.connected = False

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, name="pocketbase-realtime", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        failures = 0
        while not self.stop_event.is_set():
            try:
                subscribed = self.listen_once()
                self.connected = False
            except Exception as e:
                self.connected = False
                logger.info(f"Realtime connection lost: {e}")
                subscribed = False
            else:
                logger.info("Realtime connection closed by PocketBase")
            if self.stop_event.is_set():
                return
            # A connection that got as far as subscribing resets the backoff
            failures = 0 if subscribed else failures + 1
            delay = min(2 ** failures, MAX_RECONNECT_SECONDS) if failures else 1
            logger.info(f"Reconnecting to realtime in {delay}s")
            self.stop_event.wait(delay)

    def listen_once(self) -> bool:
        """Read one SSE connection until it ends. Returns whether it got subscribed."""
        subscribed = False
        with self.session.get(
            self.url,
            stream=True,
            timeout=(self.connect_timeout_seconds, READ_TIMEOUT_SECONDS),
            headers={"Accept": "text/event-stream"},
        ) as response:
            response.raise_for_status()
            # chunk_size=1: the default buffers 512 bytes, which would hold back
            # a single small event until the next one arrives
            for event, data in iter_sse(response.iter_lines(chunk_size=1, decode_unicode=True)):
                if self.stop_event.is_set():
                    break
                if event == "PB_CONNECT":
                    self._subscribe(json.loads(data)["clientId"])
                    subscribed = self.connected = True
                    logger.info(f"Subscribed to PocketBase realtime ({TOPIC})")
                    self.on_wake()
                elif event == TOPIC:
                    self._handle(data)
        return subscribed

    def _subscribe(self, client_id: str) -> None:
        response = self.session.post(
            self.url,
            json={"clientId": client_id, "subscriptions": [TOPIC]},
            timeout=self.connect_timeout_seconds,
        )
        response.raise_for_status()

    def _handle(self, data: str) -> None:
        try:
            message = json.loads(data)
        except ValueError:
            logger.debug(f"Ignoring realtime event that isn't JSON: {data[:200]}")
            return
        if message.get("action") != "create":
            return
        source_id = (message.get("record") or {}).get("source_id")
        if source_id and source_id not in self.exclude_sources:
            logger.debug(f"{source_id}: new measurement announced")
            self.on_wake()
