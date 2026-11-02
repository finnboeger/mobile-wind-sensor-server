"""
Forwards measurements from PocketBase to SAP Sailing's putWind API.

Every source in PocketBase is forwarded, except those in
`config.exclude_sources`, and its `source_id` is used as the wind source ID in
SAP. The service keeps one cursor over all sources: when the newest measurement
it has handed to SAP reached PocketBase (its `created_at` time, not its `ts`),
so measurements that arrive late are still picked up. It is saved to a small
JSON file so a restart resumes where it stopped, and it only moves forward once
SAP has answered a batch, so nothing is lost while SAP is unreachable.
"""

import json
import logging
import math
import os
import queue
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from src.config import UplinkConfig
from src.convert import format_pocketbase_ts, to_wind_fix
from src.pocketbase_reader import PocketbaseReader
from src.sap_client import SapClient, SapRejectedError
from src.status import RaceUploads, UplinkStatus

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 300
# First retry delay; shorter than the poll interval when realtime makes polling rare
RETRY_BASE_SECONDS = 5

# Returns the races to send to: a list ({regattaName, raceName}), or None for
# "no list" (SAP picks every tracked race the login may update)
RacesProvider = Callable[[], Optional[List[Dict[str, str]]]]
# How often to look for new settings while there are none to send with
UNCONFIGURED_RECHECK_SECONDS = 5


class CursorStore:
    def __init__(self, path: str):
        self.path = path

    def load(self) -> Optional[str]:
        try:
            with open(self.path) as f:
                cursor = json.load(f).get("created_at")
            return cursor if isinstance(cursor, str) else None
        except FileNotFoundError:
            return None
        except (OSError, ValueError, AttributeError) as e:
            logger.warning(f"Could not read cursor file {self.path}, starting fresh: {e}")
            return None

    def save(self, cursor: str) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump({"created_at": cursor}, f, indent=2)
        os.replace(tmp_path, self.path)


class Uplink:
    def __init__(
        self,
        config: UplinkConfig,
        reader: PocketbaseReader,
        sap: SapClient,
        store: Optional[CursorStore],
        races_provider: RacesProvider = lambda: None,
        target=None,
        status: Optional[UplinkStatus] = None,
        uploads: Optional[RaceUploads] = None,
    ):
        """
        With a target (src.target.SapTarget), the SAP client and races come
        from it and follow the settings in PocketBase; sap and races_provider are
        then ignored.
        """
        self.config = config
        self.reader = reader
        self.sap = sap
        self.store = store
        self.races_provider = races_provider
        self.target = target
        self.status = status or UplinkStatus(None)
        self.uploads = uploads or RaceUploads(None)
        self.cursor = ""
        # Failed rounds in a row, and when to try again (monotonic)
        self.failures = 0
        self.retry_at = 0.0
        self.stats = {"fixes_sent": 0, "fixes_accepted": 0, "records_skipped": 0, "batches_rejected": 0}

    def init_cursor(self) -> None:
        saved = self.store.load()
        if saved is not None:
            self.cursor = saved
            logger.info(f"Resuming after {saved}")
        else:
            self.cursor = format_pocketbase_ts(self.config.start_from or datetime.now(timezone.utc))
            logger.info(f"No saved cursor, starting after {self.cursor}")
            self.store.save(self.cursor)
        self.status.update(cursor=self.cursor)

    def run_forever(self, wakeups: Optional["queue.Queue[None]"] = None) -> None:
        """
        Forward a batch every poll interval, right away while there is a
        backlog, and, with a wake-up queue (fed by the realtime listener), as
        soon as something arrives on it.
        """
        while True:
            backlog = self.poll_once()
            self.status.tick()
            self.uploads.flush()
            if backlog:
                continue
            # While backing off, wake up when the retry is due
            timeout = self.config.poll_seconds
            if self.retry_at > time.monotonic():
                timeout = min(timeout, self.retry_at - time.monotonic())
            if wakeups is None:
                time.sleep(timeout)
                continue
            try:
                wakeups.get(timeout=timeout)
            except queue.Empty:
                continue
            # One read covers every announcement queued so far
            while True:
                try:
                    wakeups.get_nowait()
                except queue.Empty:
                    break

    def poll_once(self) -> bool:
        """Send the next batch unless backing off; True if more may be waiting."""
        if time.monotonic() < self.retry_at:
            return False
        if self.target is not None and not self._retarget():
            self.retry_at = time.monotonic() + UNCONFIGURED_RECHECK_SECONDS
            return False
        try:
            backlog = self.forward_new()
        except Exception as e:
            self.failures += 1
            delay = self.backoff_seconds(self.failures)
            self.retry_at = time.monotonic() + delay
            logger.warning(f"Upload failed ({self.failures} in a row), retrying in {delay:.0f}s: {e}")
            self.status.failed(str(e))
            return False
        self.failures = 0
        return backlog

    def _retarget(self) -> bool:
        """Follow the target's current settings; False while they don't allow sending."""
        problem = self.target.refresh()
        settings = self.target.settings
        self.status.update(
            configured=problem is None,
            problem=problem or "",
            sap_url=settings.sap_url if settings else "",
            event_id=settings.event_id if settings else "",
            course_area=settings.course_area if settings else "",
        )
        if problem:
            return False
        self.sap = self.target.sap
        self.races_provider = self.target.current_races
        self.target.refresh_races()
        return True

    def backoff_seconds(self, failures: int) -> float:
        base = min(self.config.poll_seconds, RETRY_BASE_SECONDS)
        max_exponent = max(0, math.ceil(math.log2(MAX_BACKOFF_SECONDS / base)))
        return min(base * 2 ** min(failures, max_exponent), MAX_BACKOFF_SECONDS)

    def forward_new(self) -> bool:
        """Send the next batch stored after the cursor; True if more may be waiting."""
        records = self.reader.list_new(self.cursor, limit=self.config.max_batch_size)
        if not records:
            return False
        # If a later source fails, the whole batch is retried; SAP ignores the
        # fixes it already has, so the earlier sources aren't duplicated.
        for source_id, source_records in self._by_source(records).items():
            self._send(source_id, source_records)
        self.cursor = records[-1]["created_at"]
        self.store.save(self.cursor)
        self.status.update(cursor=self.cursor)
        return len(records) == self.config.max_batch_size

    def backfill(
        self,
        start: datetime,
        end: datetime,
        races: Optional[List[Dict[str, str]]] = None,
        on_page: Callable[[], None] = lambda: None,
    ) -> None:
        """
        Send a measurement time range once, independent of the cursor (e.g. after a
        long data gap). With `races`, send to those instead of the current races.
        """
        if races is not None:
            self.races_provider = lambda: races
        after = format_pocketbase_ts(start)
        until = format_pocketbase_ts(end)
        logger.info(f"Backfilling {after} .. {until}")
        for page in self.reader.iter_range(after, until, self.config.max_batch_size):
            for source_id, records in self._by_source(page).items():
                self._send(source_id, records)
            on_page()

    def _by_source(self, records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group records by source, in arrival order, leaving out excluded sources."""
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for record in records:
            source_id = record.get("source_id")
            if source_id and source_id not in self.config.exclude_sources:
                grouped.setdefault(source_id, []).append(record)
        return grouped

    def _send(self, source_id: str, records: List[Dict[str, Any]]) -> None:
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
                answers = self.sap.put_wind(source_id, fixes, races)
            except SapRejectedError as e:
                # Retrying the same batch would block the uplink forever
                self.stats["batches_rejected"] += 1
                logger.error(f"{source_id}: skipping {len(fixes)} fix(es) SAP refused: {e}")
                answers = None

            if answers is not None:
                accepted = self._log_answers(source_id, fixes, answers)
                self.stats["fixes_sent"] += len(fixes)
                self.stats["fixes_accepted"] += accepted
                self.status.sent(accepted > 0)
        else:
            logger.debug(f"{source_id}: {len(records)} record(s) without position or true wind, nothing to send")

    def _log_answers(self, source_id: str, fixes: List[Dict[str, Any]], answers: List[Dict[str, Any]]) -> int:
        """Log and report which races took the fixes; returns how many fixes at least one race accepted."""
        count = len(fixes)
        # One answer per race SAP considered; "answer" holds a flag per fix,
        # false where the fix was outside that race's tracking interval.
        races = []
        accepted_by_any = [False] * count
        for answer in answers:
            flags = answer.get("answer")
            if not isinstance(flags, list):
                # SAP answers with a message instead of flags, e.g. for an unsupported source type
                continue
            timepoints = [fixes[i]["timepoint"] for i, ok in enumerate(flags[:count]) if ok]
            accepted = len(timepoints)
            for i, ok in enumerate(flags[:count]):
                if ok:
                    accepted_by_any[i] = True
            if accepted:
                race = answer.get("regattaNameAndRaceName") or {}
                self.uploads.record(source_id, race, min(timepoints), max(timepoints), accepted)
                races.append(f"{race.get('regattaName')} / {race.get('raceName')} ({accepted})")
        if races:
            logger.info(f"{source_id}: sent {count} fix(es), accepted by {', '.join(races)}")
        else:
            logger.debug(f"{source_id}: sent {count} fix(es), no tracked race took them")
        return sum(accepted_by_any)
