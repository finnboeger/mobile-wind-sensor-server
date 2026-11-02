#!/usr/bin/env python3
"""
SAP Sailing Uplink Service

Docker service that:
- Polls the PocketBase measurements collection for new true wind data
- Forwards it to SAP Sailing Analytics via the putWind API
- Keeps a cursor so outages on either side lose no data

Usage:
  python -m src.main                              # run continuously
  python -m src.main --backfill FROM [--until TO]  # send a time range once and exit

Configuration via environment variables, see README.md.
"""

import argparse
import logging
import signal
import sys
from datetime import datetime, timezone

from src.config import UplinkConfig, parse_timestamp
from src.pocketbase_reader import PocketbaseReader
from src.sap_client import SapClient
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
        logger.info(f"Configuration loaded: {config.sap_url}")

        if config.log_level in LOG_LEVELS:
            logging.getLogger().setLevel(LOG_LEVELS[config.log_level])
        else:
            logger.warning(f"Unknown LOG_LEVEL '{config.log_level}', using info ({', '.join(LOG_LEVELS)})")

        reader = PocketbaseReader(config.pocketbase_url, config.request_timeout_seconds)
        sap = SapClient(
            config.sap_url,
            bearer_token=config.sap_bearer_token,
            timeout_seconds=config.request_timeout_seconds,
        )
        if config.races:
            races_provider = lambda: config.races
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
        else:
            logger.warning("Races: every tracked race this login may update (SAP_ALL_RACES=true)")

        if args.backfill:
            uplink.backfill(args.start, args.end)
            logger.info(f"✓ Backfill done: {uplink.stats}")
            return

        uplink.init_cursor()
        logger.info("✓ SAP Sailing Uplink Service started successfully")
        uplink.run_forever()

    except Exception:
        logger.exception("SAP Sailing Uplink Service failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
