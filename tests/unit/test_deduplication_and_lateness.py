from __future__ import annotations

from datetime import timedelta

import pytest

from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from tests.helpers import BASE_TIME


def test_cache_remembers_and_reports_seen_ids() -> None:
    cache = RecentEventCache(3)
    cache.remember(["a", "b"])
    assert cache.seen("a") and not cache.seen("z")
    assert "b" in cache and len(cache) == 2


def test_cache_evicts_least_recently_used() -> None:
    cache = RecentEventCache(2)
    cache.remember(["a", "b"])
    cache.seen("a")
    cache.remember(["c"])
    assert "a" in cache and "c" in cache and "b" not in cache
    assert cache.evictions == 1


def test_cache_requires_positive_capacity() -> None:
    with pytest.raises(ValueError):
        RecentEventCache(0)


def test_first_event_of_partition_is_never_late() -> None:
    tracker = WatermarkTracker(timedelta(seconds=60))
    result = tracker.observe("t", 0, BASE_TIME)
    assert result.is_late is False and result.watermark is None


def test_watermark_advances_only_forward() -> None:
    tracker = WatermarkTracker(timedelta(seconds=60))
    tracker.observe("t", 0, BASE_TIME + timedelta(minutes=10))
    tracker.observe("t", 0, BASE_TIME + timedelta(minutes=1))
    assert tracker.watermark("t", 0) == BASE_TIME + timedelta(minutes=9)


def test_late_boundary_is_exclusive() -> None:
    tracker = WatermarkTracker(timedelta(seconds=60))
    tracker.observe("t", 0, BASE_TIME + timedelta(seconds=120))
    assert tracker.observe("t", 0, BASE_TIME + timedelta(seconds=60)).is_late is False
    result = tracker.observe("t", 0, BASE_TIME + timedelta(seconds=59))
    assert result.is_late is True and result.lateness_ms == 1000


def test_forgetting_a_revoked_partition_resets_its_watermark() -> None:
    tracker = WatermarkTracker(timedelta(seconds=0))
    tracker.observe("t", 3, BASE_TIME + timedelta(hours=1))
    tracker.forget_partition("t", 3)
    assert tracker.observe("t", 3, BASE_TIME).is_late is False


def test_negative_lateness_is_rejected() -> None:
    with pytest.raises(ValueError):
        WatermarkTracker(timedelta(seconds=-1))
