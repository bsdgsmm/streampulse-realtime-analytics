"""Bounded in-memory duplicate detection keyed by ``event_id``.

This cache is an optimisation, not the source of truth: it is lost on restart
and only remembers the most recent ``capacity`` identifiers. Storage-level
deduplication (ReplacingMergeTree + ``FINAL`` / ``uniqExact``) is what makes
the analytical results correct when a duplicate slips past the cache.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable


class RecentEventCache:
    """LRU set of event identifiers that have been durably written."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._entries: OrderedDict[str, None] = OrderedDict()
        self.evictions = 0

    def __contains__(self, event_id: object) -> bool:
        return event_id in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def seen(self, event_id: str) -> bool:
        if event_id in self._entries:
            self._entries.move_to_end(event_id)
            return True
        return False

    def remember(self, event_ids: Iterable[str]) -> None:
        for event_id in event_ids:
            self._entries[event_id] = None
            self._entries.move_to_end(event_id)
            if len(self._entries) > self.capacity:
                self._entries.popitem(last=False)
                self.evictions += 1
