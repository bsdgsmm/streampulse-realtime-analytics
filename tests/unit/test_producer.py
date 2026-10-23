from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from streampulse.events.generator import EventGenerator, GeneratorConfig, MessageKind
from streampulse.producer.main import EventPublisher, publish_stream, wait_for_broker
from streampulse.retry import RetriesExhaustedError, RetryPolicy

SEND_TIME = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


class FakeProducer:
    def __init__(self, buffer_full_times: int = 0, fail_delivery: bool = False) -> None:
        self.messages: list[dict[str, Any]] = []
        self.buffer_full_times = buffer_full_times
        self.fail_delivery = fail_delivery
        self.polls = 0

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self.buffer_full_times:
            self.buffer_full_times -= 1
            raise BufferError("queue full")
        self.messages.append({"topic": topic, **kwargs})
        message = SimpleNamespace(key=lambda: kwargs["key"])
        kwargs["on_delivery"]("broker down" if self.fail_delivery else None, message)

    def poll(self, timeout: float = 0) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float = 0) -> int:
        return 0

    def list_topics(self, topic: str | None = None, timeout: float = 0) -> Any:
        raise NotImplementedError


def generated(count: int = 200, **config: Any):
    return EventGenerator(GeneratorConfig(seed=9, **config)).take(count)


def test_live_publishing_restamps_produced_at() -> None:
    producer = FakeProducer()
    publisher = EventPublisher(producer, "events", clock=lambda: SEND_TIME)
    message = next(m for m in generated() if m.kind is MessageKind.VALID)
    publisher.publish(message)
    payload = json.loads(producer.messages[0]["value"])
    assert payload["produced_at"].startswith("2026-10-01T12:00:00")
    delay = message.event.produced_at - message.event.event_timestamp
    assert datetime.fromisoformat(payload["event_timestamp"]) == SEND_TIME - delay
    assert producer.messages[0]["key"] == message.key.encode()


def test_duplicates_are_resent_byte_for_byte() -> None:
    producer = FakeProducer()
    ticks = iter(range(10_000))
    publisher = EventPublisher(
        producer, "events", clock=lambda: SEND_TIME.replace(second=next(ticks) % 60)
    )
    messages = generated(800, duplicate_rate=0.2)
    for message in messages:
        publisher.publish(message)
    by_id: dict[str, list[bytes]] = {}
    for sent, message in zip(producer.messages, messages, strict=True):
        if message.kind is not MessageKind.INVALID:
            by_id.setdefault(str(message.event.event_id), []).append(sent["value"])
    resent = [values for values in by_id.values() if len(values) > 1]
    assert resent
    assert all(len(set(values)) == 1 for values in resent)


def test_keep_generated_timestamps_publishes_simulated_times() -> None:
    producer = FakeProducer()
    publisher = EventPublisher(producer, "events", rebase_timestamps=False)
    message = generated(1)[0]
    publisher.publish(message)
    assert producer.messages[0]["value"] == message.encode()


def test_buffer_full_is_retried_after_polling() -> None:
    producer = FakeProducer(buffer_full_times=2)
    publisher = EventPublisher(producer, "events")
    publisher.publish(generated(1)[0])
    assert len(producer.messages) == 1
    assert publisher.metrics.buffer_full_retries == 2


def test_delivery_callbacks_update_metrics() -> None:
    ok = EventPublisher(FakeProducer(), "events")
    failed = EventPublisher(FakeProducer(fail_delivery=True), "events")
    for message in generated(5):
        ok.publish(message)
        failed.publish(message)
    assert ok.metrics.delivered == 5 and ok.metrics.delivery_failures == 0
    assert failed.metrics.delivery_failures == 5


def test_publish_stream_respects_count_and_rate() -> None:
    producer = FakeProducer()
    publisher = EventPublisher(producer, "events")
    now = [0.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    sent = publish_stream(
        publisher,
        EventGenerator(GeneratorConfig(seed=1)),
        count=50,
        rate_per_second=10.0,
        should_stop=lambda: False,
        monotonic=lambda: now[0],
        sleep=sleep,
    )
    assert sent == 50 and len(producer.messages) == 50
    assert now[0] == pytest.approx(4.9)


def test_publish_stream_stops_on_request() -> None:
    publisher = EventPublisher(FakeProducer(), "events")
    calls = iter([False, False, True])
    sent = publish_stream(
        publisher,
        EventGenerator(GeneratorConfig(seed=1)),
        count=None,
        rate_per_second=None,
        should_stop=lambda: next(calls),
    )
    assert sent == 2


def test_wait_for_broker_reports_missing_topic() -> None:
    class Metadata:
        topics: ClassVar[dict[str, Any]] = {}

    producer = SimpleNamespace(list_topics=lambda topic, timeout: Metadata())
    with pytest.raises(RetriesExhaustedError, match="unavailable"):
        wait_for_broker(producer, "events", RetryPolicy(max_attempts=2, base_delay_seconds=0))


def test_wait_for_broker_accepts_existing_topic() -> None:
    topic = SimpleNamespace(error=None, partitions={0: None, 1: None, 2: None})
    metadata = SimpleNamespace(topics={"events": topic})
    producer = SimpleNamespace(list_topics=lambda topic, timeout: metadata)
    wait_for_broker(producer, "events", RetryPolicy(max_attempts=1))
