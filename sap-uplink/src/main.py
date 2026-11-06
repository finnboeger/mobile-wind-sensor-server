#!/usr/bin/env python3
"""
SAP Sailing Uplink Service

Docker service that:
- Picks up new true wind data from PocketBase (realtime subscription, polling as fallback)
- Forwards it to SAP Sailing Analytics via the putWind API
- Keeps a cursor so outages on either side lose no data
- Takes its SAP settings from PocketBase

Usage:
  python -m src.main                              # run continuously
  python -m src.main --backfill FROM [--until TO]  # send a time range once and exit

Configuration via environment variables, see README.md.
"""

import argparse
import logging
import queue
import signal
import sys
from datetime import datetime, timezone

from src.collections import ensure_collections
from src.config import UplinkConfig, parse_timestamp
from src.pocketbase_reader import PocketbaseReader
from src.pocketbase_store import PocketbaseStore
from src.race_finder import RaceFinder
from src.realtime import RealtimeListener
from src.settings import SettingsSource
from src.sap_client import SapClient, SapError, SapRejectedError
from src.uplink import CursorStore, Uplink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def shutdown_handler(signum, frame):
    logger.info("Shutting down...")
    sys.exit(0)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forward wind measurements to SAP Sailing")
    parser.add_argument("--backfill", metavar="FROM", help="send measurements after this ISO time once, then exit")
    parser.add_argument("--until", metavar="TO", help="end of the backfill range (default: now)")
    args = parser.parse_args(argv)

    args.start = args.end = None
    if args.until and not args.backfill:
        parser.error("--until needs --backfill")
    if args.backfill:
        for flag, value in (("--backfill", args.backfill), ("--until", args.until)):
            if value is None:
                continue
            try:
                parse_timestamp(value)
            except ValueError:
                parser.error(f"{flag} must be an ISO time (e.g. 2027-06-20T09:00:00Z), not '{value}'")
        args.start = parse_timestamp(args.backfill)
        args.end = parse_timestamp(args.until) if args.until else datetime.now(timezone.utc)
        if args.end <= args.start:
            parser.error("--until must be after --backfill")
    return args


def main():
    args = parse_args()

    try:
        logger.info("Loading configuration...")
        try:
            config = UplinkConfig()
        except ValueError as e:
            # A wrong setting, not a bug: one line instead of a traceback
            logger.error(f"Invalid configuration: {e}")
            sys.exit(1)
        logger.info("Configuration loaded")

        if config.log_level in LOG_LEVELS:
            logging.getLogger().setLevel(LOG_LEVELS[config.log_level])
        else:
            logger.warning(f"Unknown LOG_LEVEL '{config.log_level}', using info ({', '.join(LOG_LEVELS)})")

        reader = PocketbaseReader(config.pocketbase_url, config.request_timeout_seconds)
        store = PocketbaseStore(
            config.pocketbase_url,
            config.pocketbase_email,
            config.pocketbase_password,
            config.request_timeout_seconds,
        )
        ensure_collections(store)
        settings = SettingsSource(store, config.defaults).get()
        problem = settings.problem()
        if not problem and not (config.races or settings.event_id or config.all_races):
            problem = "No event set (or SAP_RACES / SAP_ALL_RACES)"
        if problem:
            logger.error(f"Invalid configuration: {problem}")
            sys.exit(1)
        sap = SapClient(
            settings.sap_url,
            bearer_token=settings.access_token,
            timeout_seconds=config.request_timeout_seconds,
        )
        finder = None
        if config.races:
            races_provider = lambda: config.races
        elif settings.event_id:
            finder = RaceFinder(
                sap,
                settings.event_id,
                settings.course_area or None,
                refresh_seconds=config.race_refresh_seconds,
            )
            try:
                finder.check()
            except SapRejectedError as e:
                logger.error(f"Invalid configuration: {e}")
                sys.exit(1)
            except SapError as e:
                # SAP unreachable right now: the uplink keeps retrying the lookup
                logger.warning(f"Could not check event {settings.event_id} yet: {e}")
            races_provider = finder.current_races
        else:
            races_provider = lambda: None
        uplink = Uplink(config, reader, sap, CursorStore(config.state_file), races_provider)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        if config.exclude_sources:
            logger.info(f"Sources: all, excluding {', '.join(sorted(config.exclude_sources))}")
        else:
            logger.info("Sources: all")
        if config.races:
            races = ", ".join(f"{r['regattaName']} / {r['raceName']}" for r in config.races)
            logger.info(f"Races: {races}")
        elif finder:
            area = f"course area '{settings.course_area}'" if settings.course_area else "all course areas"
            logger.info(f"Races: current races of event {settings.event_id}, {area}")
        else:
            logger.warning("Races: every tracked race this login may update (SAP_ALL_RACES=true)")

        if args.backfill:
            # The races running back then may be finished by now: offer every tracked
            # race, SAP only keeps the fixes inside each race's tracking interval
            uplink.backfill(args.start, args.end, finder.all_tracked_races() if finder else None)
            logger.info(f"✓ Backfill done: {uplink.stats}")
            return

        uplink.init_cursor()
        wakeups = None
        if config.realtime:
            # The listener thread only queues wake-ups; forwarding and the
            # cursor stay on this thread
            wakeups = queue.Queue()
            RealtimeListener(
                config.pocketbase_url,
                config.exclude_sources,
                on_wake=lambda: wakeups.put(None),
                connect_timeout_seconds=config.request_timeout_seconds,
            ).start()
        logger.info(
            f"✓ SAP Sailing Uplink Service started successfully "
            f"(realtime {'on' if config.realtime else 'off'}, polling every {config.poll_seconds:g}s)"
        )
        uplink.run_forever(wakeups)

    except Exception:
        logger.exception("SAP Sailing Uplink Service failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
