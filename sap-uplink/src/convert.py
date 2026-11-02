"""
Conversion from PocketBase measurement records to SAP Sailing wind fixes
"""

import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional

MPS_TO_KNOTS = 3600 / 1852


def parse_pocketbase_ts(value: str) -> datetime:
    """Parse a PocketBase date ('2026-10-28 17:03:29.000Z') into an aware datetime."""
    return datetime.fromisoformat(value.replace(" ", "T").replace("Z", "+00:00")).astimezone(timezone.utc)


def format_pocketbase_ts(ts: datetime) -> str:
    """Format a datetime the way PocketBase stores dates, for use in filters."""
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def _finite(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def to_wind_fix(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Convert a measurement record to a putWind fix, or None if it can't be sent.

    SAP Sailing requires a position, and stores the direction the wind blows
    TO, while true_wind_dir_deg is the direction it comes FROM.
    """
    lat = _finite(record.get("gps_lat"))
    lng = _finite(record.get("gps_lng"))
    direction_from = _finite(record.get("true_wind_dir_deg"))
    speed_mps = _finite(record.get("true_wind_speed_mps"))
    ts = record.get("ts")

    if lat is None or lng is None or direction_from is None or speed_mps is None or not ts:
        return None
    # 0/0 is what a GPS without fix tends to report
    if lat == 0 and lng == 0:
        return None
    if not (-90 <= lat <= 90 and -180 <= lng <= 180) or speed_mps < 0:
        return None

    timepoint = int(parse_pocketbase_ts(ts).timestamp() * 1000)

    return {
        "position": {"latitude_deg": lat, "longitude_deg": lng},
        "timepoint": timepoint,
        "direction": round((direction_from + 180) % 360, 2),
        "speedinknots": round(speed_mps * MPS_TO_KNOTS, 2),
    }
