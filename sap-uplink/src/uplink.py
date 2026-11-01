"""
Forwards measurements from PocketBase to SAP Sailing's putWind API.

Per source the service keeps a cursor: when the newest measurement it has
handed to SAP reached PocketBase (its `created_at` time, not its `ts`), so
measurements that arrive late are still picked up. It is saved to a small JSON
file so a restart resumes where it stopped, and it only moves forward once SAP
has answered a batch, so nothing is lost while SAP is unreachable.
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from src.config import UplinkConfig
from src.convert import format_pocketbase_ts, to_wind_fix
from src.pocketbase_reader import PocketbaseReader
from src.sap_client import SapClient, SapRejectedError

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 300

# Returns the races to send to: a list ({regattaName, raceName}), or None for
# "no list" (SAP picks every tracked race the login may update)
RacesProvider = Callable[[], Optional[List[Dict[str, str]]]]


class CursorStore:
    def __init__(self, path: str):
        self.path = path

    def load(self) -> Dict[str, str]:
        try:
            with open(self.path) as f:
                data = json.load(f)
            return {k: v for k, v in data.items() if isinstance(v, str)}
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as e:
            logger.warning(f"Could not read cursor file {self.path}, starting fresh: {e}")
            return {}

    def save(self, cursors: Dict[str, str]) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(cursors, f, indent=2)
        os.replace(tmp_path, self.path)


class Uplink:
    def __init__(
        self,
        config: UplinkConfig,
        reader: PocketbaseReader,
        sap: SapClient,
        store: CursorStore,
        races_provider: RacesProvider = lambda: None,
    ):
        self.config = config
        self.reader = reader
        self.sap = sap
        self.store = store
        self.races_provider = races_provider
        self.cursors: Dict[str, str] = {}
        # Per source: failed rounds in a row, and when to try again (monotonic)
        self.failures: Dict[str, int] = {}
        self.retry_at: Dict[str, float] = {}
        self.stats = {"fixes_sent": 0, "fixes_accepted": 0, "records_skipped": 0, "batches_rejected": 0}

    # ------------------------------------------------------------------ setup

    def init_cursors(self) -> None:
        saved = self.store.load()
        start = self.config.start_from or datetime.now(timezone.utc)
        for source_id in self.config.sources:
            if source_id in saved:
                self.cursors[source_id] = saved[source_id]
                logger.info(f"{source_id}: resuming after {saved[source_id]}")
            else:
                self.cursors[source_id] = format_pocketbase_ts(start)
                logger.info(f"{source_id}: no saved cursor, starting after {self.cursors[source_id]}")
        self.store.save(self.cursors)

    # ------------------------------------------------------------------- loop

    def run_forever(self) -> None:
        while True:
            if not self.poll_once():
                time.sleep(self.config.poll_seconds)

    def poll_once(self) -> bool:
        """
        Send one batch for every source that isn't backing off, so neither a
        failing source nor one with a long backlog holds up the others.
        Returns True if a source may have more waiting.
        """
        backlog = False
        for source_id in self.config.sources:
            if time.monotonic() < self.retry_at.get(source_id, 0):
                continue
            try:
                backlog = self.forward_new(source_id) or backlog
            except Exception as e:
                failures = self.failures.get(source_id, 0) + 1
                self.failures[source_id] = failures
                delay = self.backoff_seconds(failures)
                self.retry_at[source_id] = time.monotonic() + delay
                logger.warning(f"{source_id}: upload failed ({failures} in a row), retrying in {delay:.0f}s: {e}")
            else:
                self.failures[source_id] = 0
        return backlog

    def backoff_seconds(self, failures: int) -> float:
        max_exponent = max(0, math.ceil(math.log2(MAX_BACKOFF_SECONDS / self.config.poll_seconds)))
        return min(self.config.poll_seconds * 2 ** min(failures, max_exponent), MAX_BACKOFF_SECONDS)

    def forward_new(self, source_id: str) -> bool:
        """Send the next batch stored after the cursor; True if more may be waiting."""
        records = self.reader.list_new(source_id, self.cursors[source_id], limit=self.config.batch_size)
        if not records:
            return False
        self._send(source_id, records)
        self.cursors[source_id] = records[-1]["created_at"]
        self.store.save(self.cursors)
        return len(records) == self.config.batch_size

    def backfill(self, start: datetime, end: datetime, races_provider: Optional[RacesProvider] = None) -> None:
        """Send a measurement time range once, independent of the cursor (e.g. after a long data gap)."""
        if races_provider is not None:
            self.races_provider = races_provider
        after = format_pocketbase_ts(start)
        until = format_pocketbase_ts(end)
        for source_id in self.config.sources:
            logger.info(f"{source_id}: backfilling {after} .. {until}")
            for page in self.reader.iter_range(source_id, after, until, self.config.batch_size):
                self._send(source_id, page)

    # ---------------------------------------------------------------- helpers

    def _send(self, source_id: str, records: List[Dict[str, Any]]) -> None:
        wind_source_id = self.config.sources[source_id]
        fixes = []
        for record in records:
            fix = to_wind_fix(record)
            if fix is None:
                self.stats["records_skipped"] += 1
            else:
                fixes.append(fix)

        races = self.races_provider() if fixes else None
        if fixes and races == []:
            logger.debug(f"{source_id}: no current race, dropping {len(fixes)} fix(es)")
        elif fixes:
            try:
                answers = self.sap.put_wind(wind_source_id, fixes, races)
            except SapRejectedError as e:
                # Retrying the same batch would block the source forever
                self.stats["batches_rejected"] += 1
                logger.error(f"{source_id}: skipping {len(fixes)} fix(es) SAP refused: {e}")
                answers = None

            if answers is not None:
                self.stats["fixes_sent"] += len(fixes)
                self.stats["fixes_accepted"] += self._log_answers(source_id, len(fixes), answers)
        else:
            logger.debug(f"{source_id}: {len(records)} record(s) without position or true wind, nothing to send")

    def _log_answers(self, source_id: str, count: int, answers: List[Dict[str, Any]]) -> int:
        """Log which races took the fixes; returns how many fixes at least one race accepted."""
        # One answer per race SAP considered; "answer" holds a flag per fix,
        # false where the fix was outside that race's tracking interval.
        races = []
        accepted_by_any = [False] * count
        for answer in answers:
            flags = answer.get("answer")
            if not isinstance(flags, list):
                # SAP answers with a message instead of flags, e.g. for an unsupported source type
                continue
            accepted = 0
            for i, ok in enumerate(flags[:count]):
                if ok:
                    accepted += 1
                    accepted_by_any[i] = True
            if accepted:
                race = answer.get("regattaNameAndRaceName") or {}
                races.append(f"{race.get('regattaName')} / {race.get('raceName')} ({accepted})")
        if races:
            logger.info(f"{source_id}: sent {count} fix(es), accepted by {', '.join(races)}")
        else:
            logger.debug(f"{source_id}: sent {count} fix(es), no tracked race took them")
        return sum(accepted_by_any)
