"""Event-time watermarks used to classify late-arriving events.

For each partition the tracker remembers the maximum event time observed so
far. The watermark is that maximum minus the allowed lateness. An event whose
``event_timestamp`` is older than the partition watermark at the moment it is
processed is *late*.

Late events are never dropped: they are stored with ``is_late = true`` and
their lateness in milliseconds so that queries can include or exclude them.

Watermarks are per partition because events are keyed by ``session_id`` and
ordering only holds within a partition. They are kept in memory, so after a
restart the first events of each partition are compared against an empty
watermark and are classified as on time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class LatenessResult:
    is_late: bool
    lateness_ms: int
    watermark: datetime | None


class WatermarkTracker:
    def __init__(self, allowed_lateness: timedelta) -> None:
        if allowed_lateness < timedelta(0):
            raise ValueError("allowed_lateness must not be negative")
        self.allowed_lateness = allowed_lateness
        self._max_event_time: dict[tuple[str, int], datetime] = {}

    def watermark(self, topic: str, partition: int) -> datetime | None:
        max_seen = self._max_event_time.get((topic, partition))
        return None if max_seen is None else max_seen - self.allowed_lateness

    def observe(self, topic: str, partition: int, event_time: datetime) -> LatenessResult:
        current = self.watermark(topic, partition)
        is_late = current is not None and event_time < current
        lateness_ms = int((current - event_time).total_seconds() * 1000) if is_late else 0
        key = (topic, partition)
        previous_max = self._max_event_time.get(key)
        if previous_max is None or event_time > previous_max:
            self._max_event_time[key] = event_time
        return LatenessResult(is_late=is_late, lateness_ms=lateness_ms, watermark=current)

    def forget_partition(self, topic: str, partition: int) -> None:
        self._max_event_time.pop((topic, partition), None)
