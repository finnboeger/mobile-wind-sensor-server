"""
Environment configuration for the SAP Sailing Uplink Service

All settings are read from environment variables.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

WIND_SOURCE_TYPES = ("WEB", "EXPEDITION")


def parse_source_mapping(value: str) -> Dict[str, str]:
    """Parse 'source_id=windSourceId,...' into a dict."""
    mapping: Dict[str, str] = {}
    for entry in value.split(","):
        parts = entry.split("=")
        if len(parts) == 2:
            source_id = parts[0].strip()
            wind_source_id = parts[1].strip()
            if source_id and wind_source_id:
                mapping[source_id] = wind_source_id
    return mapping


def parse_races(value: Optional[str]) -> Optional[List[Dict[str, str]]]:
    """Parse SAP_RACES (JSON list of {regattaName, raceName}) or return None."""
    if not value or not value.strip():
        return None
    races = json.loads(value)
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
        # SAP Sailing server
        sap_url = os.getenv("SAP_URL")
        if not sap_url:
            raise ValueError("Missing SAP_URL environment variable (e.g. https://my-event.sapsailing.com)")
        self.sap_url = sap_url.rstrip("/")

        self.sap_user = os.getenv("SAP_USER") or None
        self.sap_password = os.getenv("SAP_PASSWORD") or None
        self.sap_bearer_token = os.getenv("SAP_BEARER_TOKEN") or None
        if not self.sap_bearer_token and not (self.sap_user and self.sap_password):
            raise ValueError("Set SAP_USER and SAP_PASSWORD, or SAP_BEARER_TOKEN")

        sources_str = os.getenv("SAP_SOURCES")
        if not sources_str:
            raise ValueError(
                "Missing SAP_SOURCES environment variable (format: source_id1=windSourceId1,source_id2=windSourceId2)"
            )
        self.sources = parse_source_mapping(sources_str)
        if not self.sources:
            raise ValueError("No valid SAP_SOURCES mappings found")

        # Which races get the wind, in order of precedence:
        # 1. SAP_RACES: a fixed list
        # 2. SAP_EVENT_ID (+ SAP_COURSE_AREA): the event's current races, looked up automatically
        # 3. SAP_ALL_RACES=true: no list at all. SAP then adds the wind to every tracked
        #    race the login may update, including other people's races on a shared server.
        self.races = parse_races(os.getenv("SAP_RACES"))
        self.event_id = (os.getenv("SAP_EVENT_ID") or "").strip() or None
        self.course_area = (os.getenv("SAP_COURSE_AREA") or "").strip() or None
        self.all_races = os.getenv("SAP_ALL_RACES", "false").strip().lower() in ("1", "true", "yes")
        if not self.races and not self.event_id and not self.all_races:
            raise ValueError(
                "Set SAP_EVENT_ID (and optionally SAP_COURSE_AREA) to follow the current race, "
                "SAP_RACES to fix the target races, or SAP_ALL_RACES=true to send to every tracked race"
            )
        if self.course_area and not self.event_id:
            raise ValueError("SAP_COURSE_AREA needs SAP_EVENT_ID")
        self.race_refresh_seconds = float(os.getenv("SAP_RACE_REFRESH_SECONDS", "30"))
        self.finished_grace_seconds = int(os.getenv("SAP_FINISHED_GRACE_SECONDS", "600"))

        self.wind_source_type = os.getenv("SAP_WIND_SOURCE_TYPE", "EXPEDITION").upper()
        if self.wind_source_type not in WIND_SOURCE_TYPES:
            raise ValueError(f"SAP_WIND_SOURCE_TYPE must be one of {', '.join(WIND_SOURCE_TYPES)}")

        # Upload behaviour
        self.poll_seconds = float(os.getenv("SAP_POLL_SECONDS", "5"))
        self.batch_size = int(os.getenv("SAP_BATCH_SIZE", "200"))
        self.request_timeout_seconds = float(os.getenv("SAP_REQUEST_TIMEOUT_SECONDS", "30"))

        start_from = os.getenv("SAP_START_FROM")
        self.start_from = parse_timestamp(start_from) if start_from else None

        self.state_file = os.getenv("SAP_STATE_FILE", "/data/cursor.json")

        # PocketBase (measurements are publicly readable, no login needed)
        pocketbase_url = os.getenv("POCKETBASE_URL")
        if not pocketbase_url:
            raise ValueError("Missing POCKETBASE_URL environment variable")
        self.pocketbase_url = pocketbase_url.rstrip("/")

        self.log_level = os.getenv("LOG_LEVEL", "info").lower()
