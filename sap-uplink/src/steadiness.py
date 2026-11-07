"""
Drops measurements taken while the boat speeds up, slows down or turns.

A stopgap until the client fuses GPS with the IMU: GPS speed and course lag
behind the boat's motion, so the true wind computed from them is off for a few
seconds whenever the boat accelerates or turns. This mirrors the onboard
dashboard (`computeUnreliableRanges` in the client's WindView.ts): between two
consecutive measurements of a source,

- a change of `sensor_speed_mps` faster than the acceleration threshold, or
- a change of `sensor_heading_deg` faster than the heading rate threshold while
  the average speed of the two is above 1 m/s

is an event at the midpoint of the two, and every measurement within the
window before or after an event is dropped.

Judging a measurement needs the ones after it, up to the window. Live, the
uplink therefore holds a measurement back until a measurement of the same
source at least a window later is stored, or until it has waited
`max_hold_seconds` (e.g. because the boat stopped sending).
"""

import bisect
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.convert import parse_pocketbase_ts

# Below this speed the GPS heading is noise, as on the dashboard
MIN_SPEED_FOR_HEADING_MPS = 1.0

# (ts in epoch seconds, sensor speed, sensor heading)
Sample = Tuple[float, Optional[float], Optional[float]]


@dataclass
class SteadinessConfig:
    enabled: bool = True
    accel_threshold_ms2: float = 0.3
    heading_rate_threshold_degs: float = 10.0
    window_seconds: float = 5.0
    max_hold_seconds: float = 20.0


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _epoch(value: Any) -> Optional[float]:
    if not value or not isinstance(value, str):
        return None
    try:
        return parse_pocketbase_ts(value).timestamp()
    except ValueError:
        return None


def _sample(record: Dict[str, Any]) -> Optional[Sample]:
    t = _epoch(record.get("ts"))
    if t is None:
        return None
    return (t, _number(record.get("sensor_speed_mps")), _number(record.get("sensor_heading_deg")))


class SteadinessFilter:
    """
    Decides per measurement whether it was taken while the boat moved steadily.
    Keeps the recent measurements of every source it was told about
    (`remember`), so a batch can be judged against the ones before it.
    """

    def __init__(self, config: SteadinessConfig):
        self.config = config
        self.history: Dict[str, List[Sample]] = {}

    def events(self, samples: List[Sample]) -> List[float]:
        """Sorted times of acceleration or turning events between consecutive samples (sorted by time)."""
        events = []
        for (ta, sa, ha), (tb, sb, hb) in zip(samples, samples[1:]):
            dt = tb - ta
            if dt <= 0:
                continue
            mid = (ta + tb) / 2
            if sa is not None and sb is not None and abs(sb - sa) / dt > self.config.accel_threshold_ms2:
                events.append(mid)
                continue
            if ha is not None and hb is not None and ((sa or 0) + (sb or 0)) / 2 > MIN_SPEED_FOR_HEADING_MPS:
                dh = abs(hb - ha) % 360
                if dh > 180:
                    dh = 360 - dh
                if dh / dt > self.config.heading_rate_threshold_degs:
                    events.append(mid)
        return events

    def decide(
        self, records: List[Dict[str, Any]], now: Optional[float], final: bool = False
    ) -> List[Optional[bool]]:
        """
        Per record: True to send it, False to drop it, None if it can't be judged
        yet because the measurements after it haven't arrived. With `final`,
        everything is judged with what is there.
        `now` is the current time in epoch seconds: a record stored (`created_at`)
        more than max_hold_seconds before it is judged with what is there too.
        With None, records wait however old they are (e.g. in a backfill).
        """
        window = self.config.window_seconds
        by_source: Dict[str, List[Sample]] = {}
        for record in records:
            sample = _sample(record)
            if sample is not None:
                by_source.setdefault(record.get("source_id") or "", []).append(sample)

        events: Dict[str, List[float]] = {}
        newest: Dict[str, float] = {}
        for source_id, samples in by_source.items():
            samples = sorted(self.history.get(source_id, []) + samples, key=lambda s: s[0])
            events[source_id] = self.events(samples)
            newest[source_id] = samples[-1][0]

        decisions: List[Optional[bool]] = []
        for record in records:
            t = _epoch(record.get("ts"))
            if t is None:
                # Can't be placed in time; convert decides whether it can be sent
                decisions.append(True)
                continue
            source_id = record.get("source_id") or ""
            if not final and newest[source_id] < t + window:
                created = _epoch(record.get("created_at"))
                if now is None or created is None or now - created < self.config.max_hold_seconds:
                    decisions.append(None)
                    continue
            source_events = events[source_id]
            i = bisect.bisect_left(source_events, t - window)
            decisions.append(not (i < len(source_events) and source_events[i] <= t + window))
        return decisions

    def remember(self, records: List[Dict[str, Any]]) -> None:
        """Add judged records to the history the next batches are compared with."""
        keep = 2 * self.config.window_seconds
        for record in records:
            sample = _sample(record)
            if sample is not None:
                self.history.setdefault(record.get("source_id") or "", []).append(sample)
        for source_id, samples in self.history.items():
            samples.sort(key=lambda s: s[0])
            # Keep one sample before the span, so an event spanning a gap is still found
            cut = bisect.bisect_left([s[0] for s in samples], samples[-1][0] - keep)
            del samples[: max(0, cut - 1)]
