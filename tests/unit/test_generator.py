from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.processor import EventProcessor, RawMessage
from streampulse.events.generator import (
    DEFAULT_START_TIME,
    Corruption,
    EventGenerator,
    GeneratorConfig,
    MessageKind,
    rebase_event,
    summarize,
)
from streampulse.events.schemas import EcommerceEvent, EventType

SAMPLE_SIZE = 3000


@pytest.fixture(scope="module")
def sample() -> list:
    return EventGenerator(GeneratorConfig(seed=7)).take(SAMPLE_SIZE)


def test_same_seed_produces_identical_bytes() -> None:
    first = [message.encode() for message in EventGenerator(GeneratorConfig(seed=11)).take(500)]
    second = [message.encode() for message in EventGenerator(GeneratorConfig(seed=11)).take(500)]
    assert first == second


def test_different_seeds_differ() -> None:
    first = [message.encode() for message in EventGenerator(GeneratorConfig(seed=1)).take(50)]
    second = [message.encode() for message in EventGenerator(GeneratorConfig(seed=2)).take(50)]
    assert first != second


def test_output_matches_committed_fixture(fixtures_dir) -> None:
    expected = (fixtures_dir / "generated_seed42_first25.jsonl").read_bytes().splitlines()
    actual = [message.encode() for message in EventGenerator(GeneratorConfig(seed=42)).take(25)]
    assert actual == expected


def test_messages_are_ordered_by_send_time(sample) -> None:
    send_times = [message.send_time for message in sample]
    assert send_times == sorted(send_times)


def test_all_valid_events_are_marked_synthetic(sample) -> None:
    for message in sample:
        assert message.event.is_synthetic is True
        assert message.event.source == "streampulse-synthetic-generator"


def test_event_and_production_timestamps_are_distinct(sample) -> None:
    for message in sample:
        if message.kind is MessageKind.VALID:
            assert message.event.produced_at > message.event.event_timestamp


def test_late_events_are_injected_out_of_order() -> None:
    messages = EventGenerator(GeneratorConfig(seed=7, late_rate=0.03)).take(20_000)
    cutoff = messages[-1].send_time - timedelta(seconds=900)
    valid = [
        message
        for message in messages
        if message.kind is MessageKind.VALID and message.event.event_timestamp < cutoff
    ]
    late = [
        message
        for message in valid
        if message.event.produced_at - message.event.event_timestamp >= timedelta(seconds=120)
    ]
    assert 0.02 < len(late) / len(valid) < 0.04
    event_times = [message.event.event_timestamp for message in valid]
    assert event_times != sorted(event_times)


def test_duplicates_reuse_the_original_event(sample) -> None:
    originals = {
        message.event.event_id: message.encode()
        for message in sample
        if message.kind is MessageKind.VALID
    }
    duplicates = [message for message in sample if message.kind is MessageKind.DUPLICATE]
    assert duplicates, "expected the default duplicate rate to inject duplicates"
    for duplicate in duplicates:
        if duplicate.event.event_id in originals:
            assert duplicate.encode() == originals[duplicate.event.event_id]


def test_invalid_messages_are_rejected_by_the_consumer(sample) -> None:
    invalid = [message for message in sample if message.kind is MessageKind.INVALID]
    assert invalid
    processor = EventProcessor(
        WatermarkTracker(timedelta(seconds=60)),
        RecentEventCache(10_000),
        clock=lambda: datetime(2027, 1, 1, tzinfo=UTC),
    )
    raw = [
        RawMessage("t", 0, offset, b"k", message.encode()) for offset, message in enumerate(invalid)
    ]
    result = processor.process_batch(raw)
    assert result.events == []
    assert len(result.rejections) == len(invalid)


@pytest.mark.parametrize("corruption", list(Corruption))
def test_each_corruption_kind_is_rejected(corruption: Corruption) -> None:
    message = next(
        message
        for message in EventGenerator(GeneratorConfig(seed=3, invalid_rate=1.0)).take(4000)
        if message.corruption is corruption
    )
    processor = EventProcessor(
        WatermarkTracker(timedelta(seconds=60)),
        RecentEventCache(10),
        clock=lambda: datetime(2027, 1, 1, tzinfo=UTC),
    )
    result = processor.process_batch([RawMessage("t", 0, 0, b"k", message.encode())])
    assert len(result.rejections) == 1


def test_sessions_follow_funnel_order_in_event_time(sample) -> None:
    by_session: dict[str, list[EcommerceEvent]] = defaultdict(list)
    for message in sample:
        if message.kind is MessageKind.VALID:
            by_session[message.event.session_id].append(message.event)
    rank = {
        EventType.PRODUCT_VIEWED: 0,
        EventType.PRODUCT_ADDED_TO_CART: 0,
        EventType.CHECKOUT_STARTED: 1,
        EventType.ORDER_PLACED: 2,
        EventType.PAYMENT_COMPLETED: 3,
        EventType.ORDER_CANCELLED: 3,
    }
    for events in by_session.values():
        ordered = sorted(events, key=lambda event: event.event_timestamp)
        ranks = [rank[event.event_type] for event in ordered]
        assert ranks == sorted(ranks)


def test_session_plans_start_with_a_view_and_cart_follows_view() -> None:
    generator = EventGenerator(GeneratorConfig(seed=21))
    for _ in range(500):
        plan = generator._plan_session(DEFAULT_START_TIME)
        assert plan.steps[0][0] is EventType.PRODUCT_VIEWED
        viewed: set[object] = set()
        times = [step_time for _, step_time, _ in plan.steps]
        assert times == sorted(times) and len(set(times)) == len(times)
        for event_type, _, attributes in plan.steps:
            if event_type is EventType.PRODUCT_VIEWED:
                viewed.add(attributes["product_id"])
            if event_type is EventType.PRODUCT_ADDED_TO_CART:
                assert attributes["product_id"] in viewed


def test_extra_fields_are_emitted_on_valid_events() -> None:
    messages = EventGenerator(GeneratorConfig(seed=5, extra_field_rate=1.0)).take(5)
    payload = json.loads(messages[0].encode())
    assert "experiment_bucket" in payload


def test_rebase_preserves_delivery_delay(sample) -> None:
    event = sample[0].event
    target = datetime(2030, 6, 1, 12, 0, tzinfo=UTC)
    rebased = rebase_event(event, target)
    assert rebased.produced_at == target
    assert (
        rebased.produced_at - rebased.event_timestamp == event.produced_at - event.event_timestamp
    )
    assert rebased.event_id == event.event_id


def test_summary_counts_kinds(sample) -> None:
    summary = summarize(sample)
    assert summary["valid"] + summary.get("duplicate", 0) + summary.get("invalid", 0) == SAMPLE_SIZE


@pytest.mark.parametrize(
    "kwargs",
    [
        {"duplicate_rate": 1.5},
        {"late_rate": -0.1},
        {"sessions_per_minute": 0},
        {"start_time": datetime(2026, 1, 1)},
        {"late_delay_seconds": (10.0, 5.0)},
    ],
)
def test_invalid_configuration_is_rejected(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        GeneratorConfig(**kwargs)
