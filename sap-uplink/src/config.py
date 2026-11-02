"""
Environment configuration for the SAP Sailing Uplink Service

All settings are read from environment variables.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from src.settings import SapSettings

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


def optional_url(name: str, example: str) -> str:
    url = (os.getenv(name) or "").strip()
    if url and not url.startswith(("http://", "https://")):
        raise ValueError(f"{name} must start with http:// or https:// (e.g. {example}), not '{url}'")
    return url.rstrip("/")


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing {name} environment variable")
    return value


class UplinkConfig:
    def __init__(self):
        # Server, token and event can also be set in PocketBase (sap_settings), which wins
        # over these; until one of them has a server and a token the service waits
        sap_url = optional_url("SAP_URL", "https://my-event.sapsailing.com")
        if sap_url.endswith("/sailingserver"):
            raise ValueError("SAP_URL must not include the /sailingserver path (e.g. https://my-event.sapsailing.com)")
        self.defaults = SapSettings(
            sap_url=sap_url,
            access_token=(os.getenv("SAP_BEARER_TOKEN") or "").strip(),
            event_id=(os.getenv("SAP_EVENT_ID") or "").strip(),
            course_area=(os.getenv("SAP_COURSE_AREA") or "").strip(),
        )

        # Every source in PocketBase is uploaded under its source_id, except these
        # (e.g. sensors on a test bench)
        self.exclude_sources = parse_list(os.getenv("SAP_EXCLUDE_SOURCES"))

        # Which races get the wind, in order of precedence:
        # 1. SAP_RACES: a fixed list
        # 2. the event (sap_settings or SAP_EVENT_ID, + course area): its current races, looked up automatically
        # 3. SAP_ALL_RACES=true: no list at all. SAP then adds the wind to every tracked
        #    race the login may update, including other people's races on a shared server.
        self.races = parse_races(os.getenv("SAP_RACES"))
        self.all_races = flag("SAP_ALL_RACES", False)
        if self.races and self.all_races:
            logger.warning("SAP_RACES and SAP_ALL_RACES are set: using SAP_RACES")
        self.race_refresh_seconds = positive_number("SAP_RACE_REFRESH_SECONDS", "30")

        # With realtime, new measurements wake the service right away and polling
        # is only the fallback, so it can be rarer
        self.realtime = flag("SAP_REALTIME", True)
        self.poll_seconds = positive_number("SAP_POLL_SECONDS", "30" if self.realtime else "5")
        self.max_batch_size = positive_number("SAP_MAX_BATCH_SIZE", "200", int)
        self.request_timeout_seconds = positive_number("SAP_REQUEST_TIMEOUT_SECONDS", "30")

        start_from = (os.getenv("SAP_START_FROM") or "").strip()
        try:
            self.start_from = parse_timestamp(start_from) if start_from else None
        except ValueError:
            raise ValueError(f"SAP_START_FROM must be an ISO time (e.g. 2027-06-20T09:00:00Z), not '{start_from}'") from None

        self.state_file = os.getenv("SAP_STATE_FILE", "/data/cursor.json")

        # PocketBase: measurements are publicly readable, the uplink's own
        # collections (settings, status, upload results) need a superuser
        self.pocketbase_url = required_url("POCKETBASE_URL", "http://pocketbase:8090")
        self.pocketbase_email = required("POCKETBASE_ADMIN_EMAIL")
        self.pocketbase_password = required("POCKETBASE_ADMIN_PASSWORD")

        self.log_level = os.getenv("LOG_LEVEL", "info").lower()
