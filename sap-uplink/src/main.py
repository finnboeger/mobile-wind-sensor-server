#!/usr/bin/env python3
"""
SAP Sailing Uplink Service

Docker service that:
- Picks up new true wind data from PocketBase (realtime subscription, polling as fallback)
- Forwards it to SAP Sailing Analytics via the putWind API
- Keeps a per-source cursor so outages on either side lose no data

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

from src.config import UplinkConfig, parse_timestamp
from src.pocketbase_reader import PocketbaseReader
from src.race_finder import RaceFinder
from src.realtime import RealtimeListener
from src.sap_client import SapClient
from src.uplink import ALL_SOURCES, CursorStore, Uplink

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def shutdown_handler(signum, frame):
    """Handle SIGINT and SIGTERM"""
    logger.info("Shutting down...")
    sys.exit(0)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Forward wind measurements to SAP Sailing")
    parser.add_argument("--backfill", metavar="FROM", help="send measurements after this ISO time once, then exit")
    parser.add_argument("--until", metavar="TO", help="end of the backfill range (default: now)")
    args = parser.parse_args()

    try:
        logger.info("Loading configuration...")
        config = UplinkConfig()
        logger.info(f"Configuration loaded: {config.sap_url}")

        # Set log level
        if config.log_level == "debug":
            logging.getLogger().setLevel(logging.DEBUG)
        elif config.log_level == "error":
            logging.getLogger().setLevel(logging.ERROR)
        elif config.log_level == "warn":
            logging.getLogger().setLevel(logging.WARNING)

        reader = PocketbaseReader(config.pocketbase_url, config.request_timeout_seconds)
        sap = SapClient(
            config.sap_url,
            config.wind_source_type,
            user=config.sap_user,
            password=config.sap_password,
            bearer_token=config.sap_bearer_token,
            timeout_seconds=config.request_timeout_seconds,
        )
        finder = None
        if config.races:
            races_provider = lambda: config.races
        elif config.event_id:
            finder = RaceFinder(
                sap,
                config.event_id,
                config.course_area,
                refresh_seconds=config.race_refresh_seconds,
                finished_grace_seconds=config.finished_grace_seconds,
            )
            races_provider = finder.current_races
        else:
            races_provider = lambda: None
        uplink = Uplink(config, reader, sap, CursorStore(config.state_file), races_provider)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        mapping = ", ".join(f"{k} -> {v}" for k, v in config.sources.items())
        logger.info(f"Sources: {mapping}")
        if config.races:
            races = ", ".join(f"{r['regattaName']} / {r['raceName']}" for r in config.races)
            logger.info(f"Races: {races}")
        elif finder:
            area = f"course area '{config.course_area}'" if config.course_area else "all course areas"
            logger.info(f"Races: current races of event {config.event_id}, {area}")
        else:
            logger.warning("Races: every tracked race this login may update (SAP_ALL_RACES=true)")

        if args.backfill:
            start = parse_timestamp(args.backfill)
            end = parse_timestamp(args.until) if args.until else datetime.now(timezone.utc)
            # The races running back then may be finished by now: offer every tracked
            # race, SAP only keeps the fixes inside each race's tracking interval
            backfill_races = None
            if finder:
                tracked = finder.all_tracked_races()
                backfill_races = lambda: tracked
            uplink.backfill(start, end, backfill_races)
            logger.info(f"✓ Backfill done: {uplink.stats}")
            return

        uplink.init_cursors()
        wakeups = None
        if config.realtime:
            # The listener thread only queues source ids; forwarding and the
            # cursors stay on this thread
            wakeups = queue.Queue()
            RealtimeListener(
                config.pocketbase_url,
                config.sources,
                on_new=wakeups.put,
                on_connect=lambda: wakeups.put(ALL_SOURCES),
                connect_timeout_seconds=config.request_timeout_seconds,
            ).start()
        logger.info(
            f"✓ SAP Sailing Uplink Service started successfully "
            f"(realtime {'on' if config.realtime else 'off'}, polling every {config.poll_seconds:g}s)"
        )
        uplink.run_forever(wakeups)

    except Exception as e:
        logger.error(f"SAP Sailing Uplink Service failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
