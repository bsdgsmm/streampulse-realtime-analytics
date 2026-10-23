from __future__ import annotations

import zlib
from datetime import UTC, datetime, timedelta

import pytest

from streampulse.analytics.conversion import compute_funnel
from streampulse.analytics.queries import load_queries
from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.processor import EventProcessor, RawMessage
from streampulse.events.generator import EventGenerator, GeneratorConfig
from streampulse.storage.clickhouse import deduplication_token


def processed_sample(count: int = 2000):
    messages = EventGenerator(GeneratorConfig(seed=99)).take(count)
    processor = EventProcessor(
        WatermarkTracker(timedelta(seconds=60)),
        RecentEventCache(100_000),
        clock=lambda: datetime(2026, 2, 1, tzinfo=UTC),
    )
    raw = [
        RawMessage(
            "it-topic",
            zlib.crc32(message.key.encode()) % 3,
            offset,
            message.key.encode(),
            message.encode(),
        )
        for offset, message in enumerate(messages)
    ]
    return processor.process_batch(raw)


def scalar(store, sql: str):
    return next(iter(store.query(sql)[0].values()))


def test_retried_insert_with_same_token_is_deduplicated(isolated_db) -> None:
    _, store = isolated_db
    result = processed_sample(300)
    token = deduplication_token("events", result.event_ids)
    store.insert_events(result.events, token)
    store.insert_events(result.events, token)
    assert scalar(store, "SELECT count() FROM events") == len(result.events)


def test_replayed_events_collapse_with_final(isolated_db) -> None:
    _, store = isolated_db
    result = processed_sample(300)
    store.insert_events(result.events, "first-delivery")
    store.insert_events(result.events, "second-delivery")
    assert scalar(store, "SELECT count() FROM events") == 2 * len(result.events)
    assert scalar(store, "SELECT count() FROM events FINAL") == len(result.events)
    assert scalar(store, "SELECT uniqExact(event_id) FROM events") == len(result.events)


def test_every_analytics_query_runs_on_empty_and_loaded_tables(isolated_db) -> None:
    settings, store = isolated_db
    queries = load_queries(settings.sql_dir)
    for query in queries.values():
        store.query(query.sql)

    result = processed_sample()
    store.insert_events(result.events, "load")
    store.insert_rejections(result.rejections, "load-rejections")
    for query in queries.values():
        rows = store.query(query.sql)
        assert isinstance(rows, list), query.name
    overview = store.query(queries["pipeline_overview"].sql)[0]
    assert overview["unique_events"] == len(result.events)
    assert overview["rejected_messages"] == len(result.rejections)


def test_sql_funnel_matches_python_reference(isolated_db) -> None:
    settings, store = isolated_db
    result = processed_sample(4000)
    store.insert_events(result.events, "funnel")
    store.insert_events(result.events[:500], "funnel-replay")
    expected = compute_funnel(item.event for item in result.events)
    row = store.query(load_queries(settings.sql_dir)["funnel_summary"].sql)[0]
    assert row["sessions_with_view"] == expected.sessions_with_view
    assert row["sessions_with_view_and_cart"] == expected.sessions_with_view_and_cart
    assert row["sessions_with_checkout"] == expected.sessions_with_checkout
    assert row["sessions_with_checkout_and_order"] == expected.sessions_with_checkout_and_order
    assert row["orders_placed"] == expected.orders_placed
    assert row["orders_paid"] == expected.orders_paid
    assert row["view_to_cart_rate"] == pytest.approx(expected.view_to_cart_rate)


def test_late_flags_round_trip(isolated_db) -> None:
    _, store = isolated_db
    result = processed_sample(3000)
    store.insert_events(result.events, "late")
    stored_late = scalar(store, "SELECT countIf(is_late) FROM events FINAL")
    assert stored_late == sum(item.is_late for item in result.events)
    assert stored_late > 0
