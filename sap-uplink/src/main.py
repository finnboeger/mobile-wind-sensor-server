#!/usr/bin/env python3
"""
SAP Sailing Uplink Service

Docker service that:
- Picks up new true wind data from PocketBase (realtime subscription, polling as fallback)
- Forwards it to SAP Sailing Analytics via the putWind API
- Keeps a cursor so outages on either side lose no data
- Takes its SAP settings from PocketBase and reports its status and the
  races that accepted wind there

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
from src.realtime import RealtimeListener
from src.settings import SettingsSource
from src.status import RaceUploads, UplinkStatus
from src.target import SapTarget
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
        target = SapTarget(config, SettingsSource(store, config.defaults))

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        if config.exclude_sources:
            logger.info(f"Sources: all, excluding {', '.join(sorted(config.exclude_sources))}")
        else:
            logger.info("Sources: all")

        uploads = RaceUploads(store)

        if args.backfill:
            problem = target.refresh()
            if problem:
                logger.error(f"Invalid configuration: {problem}")
                sys.exit(1)
            uplink = Uplink(config, reader, target.sap, store=None, uploads=uploads)
            # The races running back then may be finished by now: offer every tracked
            # race, SAP only keeps the fixes inside each race's tracking interval
            uplink.backfill(args.start, args.end, target.backfill_races())
            uploads.flush(force=True)
            logger.info(f"✓ Backfill done: {uplink.stats}")
            return

        listener = None
        wakeups = None
        if config.realtime:
            # The listener thread only queues wake-ups; forwarding and the
            # cursor stay on this thread
            wakeups = queue.Queue()
            listener = RealtimeListener(
                config.pocketbase_url,
                config.exclude_sources,
                on_wake=lambda: wakeups.put(None),
                connect_timeout_seconds=config.request_timeout_seconds,
            )

        uplink = None

        def live_status():
            return {
                "realtime": ("connected" if listener.connected else "disconnected") if listener else "off",
                "current_races": target.last_current_races(),
                "all_races": not target.problem and not config.races and target.finder is None,
                "tracked_races": target.tracked_races(),
                "stats": dict(uplink.stats) if uplink else {},
            }

        status = UplinkStatus(store, extra=live_status)
        uplink = Uplink(config, reader, None, CursorStore(config.state_file), target=target, status=status, uploads=uploads)
        uplink.init_cursor()
        if listener:
            listener.start()
        status.start()
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
