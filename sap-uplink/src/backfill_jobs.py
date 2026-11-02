"""
Runs the backfills requested in PocketBase (sap_backfills), one at a time,
on a thread of its own so live forwarding goes on meanwhile. It has its own
SAP and PocketBase clients; only the race upload ranges are shared.
"""

import logging
import threading
from typing import Any, Dict

from src.config import UplinkConfig
from src.convert import parse_pocketbase_ts
from src.pocketbase_reader import PocketbaseReader
from src.pocketbase_store import PocketbaseStore
from src.settings import SettingsSource
from src.status import RaceUploads, now_ts
from src.target import SapTarget
from src.uplink import Uplink

logger = logging.getLogger(__name__)

BACKFILLS_COLLECTION = "sap_backfills"
POLL_SECONDS = 10


class BackfillWorker:
    def __init__(self, config: UplinkConfig, store: PocketbaseStore, uploads: RaceUploads):
        self.config = config
        self.store = store
        self.uploads = uploads
        self.stop_event = threading.Event()

    def start(self) -> None:
        threading.Thread(target=self.run, name="backfill", daemon=True).start()

    def run(self) -> None:
        self._requeue_interrupted()
        while not self.stop_event.is_set():
            try:
                job = self._next_job()
            except Exception as e:
                logger.warning(f"Could not look for backfill requests: {e}")
                job = None
            if job is None:
                self.stop_event.wait(POLL_SECONDS)
                continue
            self.run_job(job)

    def run_job(self, job: Dict[str, Any]) -> None:
        job_id = job["id"]
        race = {"regattaName": job["regatta_name"], "raceName": job["race_name"]} if job.get("race_name") else None
        name = f"{race['regattaName']} / {race['raceName']}" if race else "every tracked race"
        start = parse_pocketbase_ts(job["range_from"])
        end = parse_pocketbase_ts(job["range_until"])
        self.store.update(BACKFILLS_COLLECTION, job_id, {"status": "running", "started_at": now_ts(), "message": ""})
        logger.info(f"Backfill requested: {name}, {start.isoformat()} .. {end.isoformat()}")

        uplink = None
        try:
            target = SapTarget(self.config, SettingsSource(self.store, self.config.defaults))
            problem = target.refresh()
            if problem:
                raise RuntimeError(problem)
            uplink = Uplink(
                self.config,
                PocketbaseReader(self.config.pocketbase_url, self.config.request_timeout_seconds),
                target.sap,
                store=None,
                uploads=self.uploads,
            )

            def progress() -> None:
                self.uploads.flush()
                self.store.update(BACKFILLS_COLLECTION, job_id, self._counts(uplink))

            uplink.backfill(start, end, target.backfill_races(race), on_page=progress)
            self.uploads.flush(force=True)
            self.store.update(
                BACKFILLS_COLLECTION,
                job_id,
                {"status": "done", "finished_at": now_ts(), "message": self._summary(uplink), **self._counts(uplink)},
            )
            logger.info(f"✓ Backfill of {name} done: {uplink.stats}")
        except Exception as e:
            logger.error(f"Backfill of {name} failed: {e}")
            self.uploads.flush(force=True)
            counts = self._counts(uplink) if uplink else {}
            try:
                self.store.update(
                    BACKFILLS_COLLECTION,
                    job_id,
                    {"status": "failed", "finished_at": now_ts(), "message": str(e)[:2000], **counts},
                )
            except Exception as update_error:
                logger.warning(f"Could not mark the backfill as failed: {update_error}")

    def _next_job(self):
        jobs = self.store.list(BACKFILLS_COLLECTION, 'status = "pending"', sort="created", per_page=1)
        return jobs[0] if jobs else None

    def _requeue_interrupted(self) -> None:
        # A restart interrupted these; sending the range again is harmless
        try:
            for job in self.store.list(BACKFILLS_COLLECTION, 'status = "running"'):
                self.store.update(BACKFILLS_COLLECTION, job["id"], {"status": "pending"})
        except Exception as e:
            logger.warning(f"Could not requeue interrupted backfills: {e}")

    @staticmethod
    def _summary(uplink: Uplink) -> str:
        if uplink.stats["fixes_sent"] == 0:
            return "Nothing sent: no measurement with position and true wind in this range, or no tracked race"
        if uplink.stats["fixes_accepted"] == 0:
            return "SAP took none of the fixes: outside every race's tracking interval, or the token may not update the race"
        return ""

    @staticmethod
    def _counts(uplink: Uplink) -> Dict[str, int]:
        return {"fixes_sent": uplink.stats["fixes_sent"], "fixes_accepted": uplink.stats["fixes_accepted"]}
