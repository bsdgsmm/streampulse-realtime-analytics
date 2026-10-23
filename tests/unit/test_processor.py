from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.processor import (
    EventProcessor,
    RejectionReason,
    highest_offsets,
)
from tests.helpers import BASE_TIME, event_payload, raw_message

NOW = BASE_TIME + timedelta(minutes=30)


def make_processor(cache: RecentEventCache | None = None, lateness_seconds: int = 60):
    return EventProcessor(
        WatermarkTracker(timedelta(seconds=lateness_seconds)),
        cache if cache is not None else RecentEventCache(1000),
        clock=lambda: NOW,
    )


def event_id(n: int) -> str:
    return f"9d3e8b8a-1c2f-4a5b-8c7d-{n:012d}"


def at(seconds: float) -> dict[str, str]:
    ts = BASE_TIME + timedelta(seconds=seconds)
    return {
        "event_timestamp": ts.isoformat(),
        "produced_at": (ts + timedelta(seconds=1)).isoformat(),
    }


def test_valid_batch_produces_processed_events() -> None:
    messages = [
        raw_message(event_payload(event_id=event_id(i), **at(i)), offset=i) for i in range(3)
    ]
    result = make_processor().process_batch(messages)
    assert len(result.events) == 3
    assert result.rejections == [] and result.duplicates == 0
    first = result.events[0]
    assert first.ingested_at == NOW
    assert first.ingestion_delay_ms == 30 * 60 * 1000
    assert first.end_to_end_latency_ms == 30 * 60 * 1000 - 1000


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, RejectionReason.EMPTY_PAYLOAD),
        (b"", RejectionReason.EMPTY_PAYLOAD),
        (b"{not json", RejectionReason.INVALID_JSON),
        (b"\xff\xfe", RejectionReason.INVALID_JSON),
        (b"[1, 2]", RejectionReason.NOT_AN_OBJECT),
    ],
)
def test_undecodable_payloads_are_rejected(value: bytes | None, reason: RejectionReason) -> None:
    result = make_processor().process_batch([raw_message(value)])
    assert [item.reason for item in result.rejections] == [reason]
    assert result.events == []


def test_missing_field_is_reported_with_its_name() -> None:
    payload = event_payload()
    payload.pop("user_id")
    rejection = make_processor().process_batch([raw_message(payload)]).rejections[0]
    assert rejection.reason is RejectionReason.SCHEMA_VALIDATION
    assert "user_id" in rejection.message
    assert rejection.event_id == payload["event_id"]


def test_unsupported_schema_version_has_its_own_reason() -> None:
    rejection = (
        make_processor().process_batch([raw_message(event_payload(schema_version=2))]).rejections[0]
    )
    assert rejection.reason is RejectionReason.UNSUPPORTED_SCHEMA_VERSION


def test_far_future_event_time_is_rejected() -> None:
    future = NOW + timedelta(hours=2)
    payload = event_payload(
        event_timestamp=future.isoformat(), produced_at=(future + timedelta(seconds=1)).isoformat()
    )
    rejection = make_processor().process_batch([raw_message(payload)]).rejections[0]
    assert rejection.reason is RejectionReason.FUTURE_EVENT_TIMESTAMP


def test_fixture_invalid_messages_are_all_rejected(fixtures_dir) -> None:
    lines = (fixtures_dir / "invalid_messages.jsonl").read_bytes().splitlines()
    messages = [raw_message(line, offset=i) for i, line in enumerate(lines)]
    result = make_processor().process_batch(messages)
    assert result.events == []
    assert [item.reason.value for item in result.rejections] == [
        "invalid_json",
        "not_an_object",
        "unsupported_schema_version",
        "schema_validation",
        "schema_validation",
        "schema_validation",
    ]


def test_unexpected_fields_are_accepted_and_recorded() -> None:
    payload = event_payload(experiment_bucket="variant_a", client_build="1.2.3")
    result = make_processor().process_batch([raw_message(payload)])
    assert result.events[0].unexpected_fields == ("client_build", "experiment_bucket")
    assert result.events_with_unexpected_fields == 1


def test_duplicate_within_a_batch_is_dropped() -> None:
    payload = event_payload()
    result = make_processor().process_batch(
        [raw_message(payload, offset=0), raw_message(payload, offset=1)]
    )
    assert len(result.events) == 1
    assert result.duplicates == 1
    assert result.messages == 2


def test_duplicate_of_previously_written_event_is_dropped() -> None:
    cache = RecentEventCache(100)
    payload = event_payload()
    processor = make_processor(cache)
    first = processor.process_batch([raw_message(payload, offset=0)])
    cache.remember(first.event_ids)
    second = processor.process_batch([raw_message(payload, offset=1)])
    assert second.events == [] and second.duplicates == 1


def test_processing_does_not_remember_ids_before_the_write() -> None:
    cache = RecentEventCache(100)
    processor = make_processor(cache)
    processor.process_batch([raw_message(event_payload())])
    assert len(cache) == 0
    retry = processor.process_batch([raw_message(event_payload())])
    assert len(retry.events) == 1


def test_out_of_order_event_within_allowed_lateness_is_on_time() -> None:
    messages = [
        raw_message(event_payload(event_id=event_id(1), **at(100)), offset=0),
        raw_message(event_payload(event_id=event_id(2), **at(70)), offset=1),
    ]
    result = make_processor(lateness_seconds=60).process_batch(messages)
    assert [item.is_late for item in result.events] == [False, False]


def test_event_behind_watermark_is_late_but_kept() -> None:
    messages = [
        raw_message(event_payload(event_id=event_id(1), **at(600)), offset=0),
        raw_message(event_payload(event_id=event_id(2), **at(300)), offset=1),
    ]
    result = make_processor(lateness_seconds=60).process_batch(messages)
    late = result.events[1]
    assert late.is_late is True
    assert late.lateness_ms == (600 - 60 - 300) * 1000
    assert result.late_events == 1
    assert len(result.events) == 2


def test_watermarks_are_tracked_per_partition() -> None:
    messages = [
        raw_message(event_payload(event_id=event_id(1), **at(600)), partition=0, offset=0),
        raw_message(event_payload(event_id=event_id(2), **at(300)), partition=1, offset=0),
    ]
    result = make_processor().process_batch(messages)
    assert [item.is_late for item in result.events] == [False, False]


def test_rejections_do_not_stop_valid_events_in_the_same_batch() -> None:
    messages = [
        raw_message(event_payload(event_id=event_id(1)), offset=0),
        raw_message(b"garbage", offset=1),
        raw_message(event_payload(event_id=event_id(2)), offset=2),
    ]
    result = make_processor().process_batch(messages)
    assert len(result.events) == 2
    assert len(result.rejections) == 1
    assert result.rejections[0].source.offset == 1


def test_highest_offsets_per_partition() -> None:
    messages = [
        raw_message(b"x", partition=0, offset=5),
        raw_message(b"x", partition=0, offset=3),
        raw_message(b"x", partition=2, offset=9),
    ]
    assert highest_offsets(messages) == {
        ("streampulse.ecommerce.events.v1", 0): 5,
        ("streampulse.ecommerce.events.v1", 2): 9,
    }


def test_processor_uses_wall_clock_by_default() -> None:
    processor = EventProcessor(WatermarkTracker(timedelta(seconds=60)), RecentEventCache(10))
    before = datetime.now(UTC)
    result = processor.process_batch([raw_message(event_payload())])
    assert result.events[0].ingested_at >= before
