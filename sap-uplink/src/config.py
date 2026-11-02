"""
Environment configuration for the SAP Sailing Uplink Service

All settings are read from environment variables.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


def parse_list(value: Optional[str]) -> Set[str]:
    """Parse a comma-separated list, ignoring empty entries."""
    return {entry.strip() for entry in (value or "").split(",") if entry.strip()}


def positive_number(name: str, default: str, cast=float):
    raw = os.getenv(name) or default
    try:
        value = cast(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number, not '{raw}'") from None
    if not value > 0:
        raise ValueError(f"{name} must be greater than 0")
    return value


def flag(name: str, default: bool) -> bool:
    """Parse a true/false setting; anything unrecognised is an error."""
    value = (os.getenv(name) or "").strip().lower()
    if not value:
        return default
    if value in ("1", "true", "yes"):
        return True
    if value in ("0", "false", "no"):
        return False
    raise ValueError(f"{name} must be true or false, not '{value}'")


def required_url(name: str, example: str) -> str:
    url = (os.getenv(name) or "").strip()
    if not url:
        raise ValueError(f"Missing {name} environment variable (e.g. {example})")
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"{name} must start with http:// or https:// (e.g. {example}), not '{url}'")
    return url.rstrip("/")


def parse_races(value: Optional[str]) -> Optional[List[Dict[str, str]]]:
    """Parse SAP_RACES (JSON list of {regattaName, raceName}) or return None."""
    if not value or not value.strip():
        return None
    try:
        races = json.loads(value)
    except ValueError as e:
        raise ValueError(f"SAP_RACES is not valid JSON: {e}") from None
    if not isinstance(races, list):
        raise ValueError("SAP_RACES must be a JSON list")
    for race in races:
        if not isinstance(race, dict) or not race.get("regattaName") or not race.get("raceName"):
            raise ValueError(
                'Every SAP_RACES entry needs "regattaName" and "raceName"'
            )
    return [{"regattaName": r["regattaName"], "raceName": r["raceName"]} for r in races]


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive values are treated as UTC."""
    ts = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class UplinkConfig:
    def __init__(self):
        self.sap_url = required_url("SAP_URL", "https://my-event.sapsailing.com")
        if self.sap_url.endswith("/sailingserver"):
            raise ValueError("SAP_URL must not include the /sailingserver path (e.g. https://my-event.sapsailing.com)")

        self.sap_bearer_token = os.getenv("SAP_BEARER_TOKEN")
        if not self.sap_bearer_token:
            raise ValueError("Missing SAP_BEARER_TOKEN environment variable (see README for how to get one)")

        # Every source in PocketBase is uploaded under its source_id, except these
        # (e.g. sensors on a test bench)
        self.exclude_sources = parse_list(os.getenv("SAP_EXCLUDE_SOURCES"))

        # Which races get the wind, in order of precedence:
        # 1. SAP_RACES: a fixed list
        # 2. SAP_EVENT_ID (+ SAP_COURSE_AREA): the event's current races, looked up automatically
        # 3. SAP_ALL_RACES=true: no list at all. SAP then adds the wind to every tracked
        #    race the login may update, including other people's races on a shared server.
        self.races = parse_races(os.getenv("SAP_RACES"))
        self.event_id = (os.getenv("SAP_EVENT_ID") or "").strip() or None
        self.course_area = (os.getenv("SAP_COURSE_AREA") or "").strip() or None
        self.all_races = flag("SAP_ALL_RACES", False)
        if not self.races and not self.event_id and not self.all_races:
            raise ValueError(
                "Set SAP_EVENT_ID (and optionally SAP_COURSE_AREA) to follow the current race, "
                "SAP_RACES to fix the target races, or SAP_ALL_RACES=true to send to every tracked race"
            )
        set_options = [
            name
            for name, is_set in (("SAP_RACES", self.races), ("SAP_EVENT_ID", self.event_id), ("SAP_ALL_RACES", self.all_races))
            if is_set
        ]
        if len(set_options) > 1:
            logger.warning(f"{' and '.join(set_options)} are set: using {set_options[0]}, ignoring the rest")
        if self.course_area and not self.event_id:
            raise ValueError("SAP_COURSE_AREA needs SAP_EVENT_ID")
        self.race_refresh_seconds = positive_number("SAP_RACE_REFRESH_SECONDS", "30")

        self.poll_seconds = positive_number("SAP_POLL_SECONDS", "5")
        self.max_batch_size = positive_number("SAP_MAX_BATCH_SIZE", "200", int)
        self.request_timeout_seconds = positive_number("SAP_REQUEST_TIMEOUT_SECONDS", "30")

        start_from = (os.getenv("SAP_START_FROM") or "").strip()
        try:
            self.start_from = parse_timestamp(start_from) if start_from else None
        except ValueError:
            raise ValueError(f"SAP_START_FROM must be an ISO time (e.g. 2027-06-20T09:00:00Z), not '{start_from}'") from None

        self.state_file = os.getenv("SAP_STATE_FILE", "/data/cursor.json")

        # PocketBase (measurements are publicly readable, no login needed)
        self.pocketbase_url = required_url("POCKETBASE_URL", "http://pocketbase:8090")

        self.log_level = os.getenv("LOG_LEVEL", "info").lower()
