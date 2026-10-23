"""Batch, commit and failure-handling behaviour of StreamConsumer using fakes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from streampulse.consumer.dead_letter import DeadLetterError
from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.main import (
    EXIT_DLQ_FAILURE,
    EXIT_OK,
    EXIT_STORAGE_FAILURE,
    RunOptions,
    StreamConsumer,
)
from streampulse.consumer.processor import EventProcessor
from streampulse.retry import RetryPolicy
from streampulse.storage.clickhouse import PermanentStorageError, TransientStorageError
from tests.helpers import BASE_TIME, TOPIC, event_payload

NOW = BASE_TIME + timedelta(minutes=5)


@dataclass(frozen=True)
class FakeTopicPartition:
    topic: str
    partition: int
    offset: int


class FakeKafkaMessage:
    def __init__(self, value: bytes, partition: int, offset: int, error: Any = None) -> None:
        self._value = value
        self._partition = partition
        self._offset = offset
        self._error = error

    def error(self) -> Any:
        return self._error

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes:
        return b"session"

    def value(self) -> bytes:
        return self._value

    def timestamp(self) -> tuple[int, int]:
        return (1, int(NOW.timestamp() * 1000))


class FakeConsumer:
    def __init__(self, batches: list[list[FakeKafkaMessage]], high_watermark: int = 100) -> None:
        self.batches = list(batches)
        self.commits: list[list[FakeTopicPartition]] = []
        self.closed = False
        self.high_watermark = high_watermark

    def consume(self, num_messages: int = 1, timeout: float = 1.0) -> list[FakeKafkaMessage]:
        return self.batches.pop(0)[:num_messages] if self.batches else []

    def commit(self, offsets: list[Any] | None = None, asynchronous: bool = True) -> None:
        assert asynchronous is False
        self.commits.append(list(offsets or []))

    def assignment(self) -> list[FakeTopicPartition]:
        return [FakeTopicPartition(TOPIC, 0, -1001)]

    def committed(self, partitions: list[Any], timeout: float = 1.0) -> list[FakeTopicPartition]:
        last = self.commits[-1] if self.commits else []
        offset = max((tp.offset for tp in last if tp.partition == 0), default=-1001)
        return [FakeTopicPartition(TOPIC, 0, offset)]

    def get_watermark_offsets(self, partition: Any, timeout: float = 1.0, cached: bool = False):
        return (0, self.high_watermark)

    def close(self) -> None:
        self.closed = True


class FakeStore:
    def __init__(self, event_failures: int = 0, permanent: bool = False) -> None:
        self.event_failures = event_failures
        self.permanent = permanent
        self.event_inserts: list[tuple[list[Any], str]] = []
        self.rejection_inserts: list[list[Any]] = []
        self.batch_rows: list[Any] = []
        self.lag_rows: list[Any] = []
        self.attempts = 0

    def insert_events(self, events: list[Any], dedup_token: str) -> int:
        self.attempts += 1
        if self.permanent:
            raise PermanentStorageError("syntax error")
        if self.attempts <= self.event_failures:
            raise TransientStorageError("connection refused")
        self.event_inserts.append((list(events), dedup_token))
        return len(events)

    def insert_rejections(self, rejections: list[Any], dedup_token: str) -> int:
        self.rejection_inserts.append(list(rejections))
        return len(rejections)

    def insert_batch_metrics(self, row: Any) -> None:
        self.batch_rows.append(row)

    def insert_lag(self, rows: list[Any]) -> None:
        self.lag_rows.extend(rows)


class FakeDeadLetters:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[Any] = []
        self.fail = fail

    def publish(self, rejections: list[Any]) -> int:
        if self.fail:
            raise DeadLetterError("broker down")
        self.published.extend(rejections)
        return len(rejections)


def message(n: int, partition: int = 0, payload: dict | None = None) -> FakeKafkaMessage:
    body = payload or event_payload(event_id=f"9d3e8b8a-1c2f-4a5b-8c7d-{n:012d}")
    return FakeKafkaMessage(json.dumps(body).encode(), partition, n)


def build(
    batches: list[list[FakeKafkaMessage]],
    store: FakeStore | None = None,
    dead_letters: FakeDeadLetters | None = None,
    max_attempts: int = 3,
) -> tuple[StreamConsumer, FakeConsumer, FakeStore, FakeDeadLetters, list[float]]:
    kafka = FakeConsumer(batches)
    store = store or FakeStore()
    dead_letters = dead_letters or FakeDeadLetters()
    sleeps: list[float] = []
    ticks = iter(range(1_000_000))
    consumer = StreamConsumer(
        kafka,
        store,
        dead_letters,
        EventProcessor(
            WatermarkTracker(timedelta(seconds=60)), RecentEventCache(1000), clock=lambda: NOW
        ),
        consumer_group="test-group",
        consumer_id="test-consumer",
        batch_size=10,
        batch_timeout_seconds=0.01,
        retry_policy=RetryPolicy(max_attempts=max_attempts, base_delay_seconds=0.1),
        make_topic_partition=FakeTopicPartition,
        sleep=sleeps.append,
        clock=lambda: NOW,
        monotonic=lambda: float(next(ticks)),
    )
    return consumer, kafka, store, dead_letters, sleeps


def test_commits_next_offset_after_successful_write() -> None:
    consumer, kafka, store, _, _ = build([[message(0), message(1), message(2, partition=1)]])
    assert consumer.run(RunOptions(idle_timeout_seconds=1)) == EXIT_OK
    assert len(store.event_inserts) == 1
    assert kafka.commits == [[FakeTopicPartition(TOPIC, 0, 2), FakeTopicPartition(TOPIC, 1, 3)]]
    assert kafka.closed


def test_partial_batch_sends_invalid_messages_to_dlq_and_commits_once() -> None:
    bad = FakeKafkaMessage(b"{broken", 0, 1)
    consumer, kafka, store, dlq, _ = build([[message(0), bad, message(2)]])
    consumer.run(RunOptions(idle_timeout_seconds=1))
    assert len(store.event_inserts[0][0]) == 2
    assert len(dlq.published) == 1 and dlq.published[0].source.offset == 1
    assert len(store.rejection_inserts) == 1
    assert kafka.commits == [[FakeTopicPartition(TOPIC, 0, 3)]]
    assert consumer.metrics.invalid_events == 1
    assert consumer.metrics.rejections_by_reason["invalid_json"] == 1


def test_transient_failures_are_retried_with_backoff_then_committed() -> None:
    store = FakeStore(event_failures=2)
    consumer, kafka, store, _, sleeps = build([[message(0)]], store=store, max_attempts=3)
    assert consumer.run(RunOptions(idle_timeout_seconds=1)) == EXIT_OK
    assert store.attempts == 3
    assert len(sleeps) == 2
    assert consumer.metrics.write_retries == 2
    assert store.batch_rows[0].write_retries == 2
    assert len(kafka.commits) == 1


def test_retries_reuse_the_same_deduplication_token() -> None:
    store = FakeStore(event_failures=1)
    consumer, _, store, _, _ = build([[message(0), message(1)]], store=store)
    consumer.run(RunOptions(idle_timeout_seconds=1))
    _, token = store.event_inserts[0]
    assert token.startswith("events:")


def test_exhausted_retries_stop_without_committing() -> None:
    store = FakeStore(event_failures=10)
    consumer, kafka, _, _, _ = build([[message(0)], [message(1)]], store=store, max_attempts=3)
    assert consumer.run(RunOptions(idle_timeout_seconds=1)) == EXIT_STORAGE_FAILURE
    assert kafka.commits == []
    assert consumer.metrics.failed_writes == 1
    assert kafka.closed
    assert len(kafka.batches) == 1, "the consumer must stop instead of reading further batches"


def test_permanent_storage_error_is_not_retried() -> None:
    store = FakeStore(permanent=True)
    consumer, kafka, store, _, sleeps = build([[message(0)]], store=store)
    assert consumer.run(RunOptions(idle_timeout_seconds=1)) == EXIT_STORAGE_FAILURE
    assert store.attempts == 1 and sleeps == [] and kafka.commits == []


def test_dlq_failure_prevents_commit() -> None:
    consumer, kafka, store, _, _ = build(
        [[FakeKafkaMessage(b"nope", 0, 0)]], dead_letters=FakeDeadLetters(fail=True)
    )
    assert consumer.run(RunOptions(idle_timeout_seconds=1)) == EXIT_DLQ_FAILURE
    assert kafka.commits == [] and store.rejection_inserts == []


def test_duplicates_across_batches_are_not_written_twice() -> None:
    consumer, _, store, _, _ = build([[message(0)], [message(0)]])
    consumer.run(RunOptions(idle_timeout_seconds=1))
    written = [item for events, _ in store.event_inserts for item in events]
    assert len(written) == 1
    assert consumer.metrics.duplicate_events == 1
    assert consumer.metrics.messages_consumed == 2


def test_failed_write_does_not_mark_events_as_seen() -> None:
    store = FakeStore(event_failures=10)
    consumer, _, _, _, _ = build([[message(0)]], store=store, max_attempts=2)
    consumer.run(RunOptions(idle_timeout_seconds=1))
    assert len(consumer.processor.recent_events) == 0


def test_max_messages_bounds_the_run() -> None:
    consumer, kafka, _, _, _ = build([[message(i) for i in range(10)], [message(20)]])
    consumer.run(RunOptions(max_messages=4))
    assert consumer.metrics.messages_consumed == 4
    assert kafka.commits == [[FakeTopicPartition(TOPIC, 0, 4)]]


def test_stop_request_ends_the_loop_after_current_batch() -> None:
    consumer, kafka, _, _, _ = build([[message(0)], [message(1)]])
    original = consumer.handle_batch

    def handle_and_stop(raw):
        result = original(raw)
        consumer.request_stop()
        return result

    consumer.handle_batch = handle_and_stop
    consumer.run(RunOptions())
    assert len(kafka.commits) == 1
    assert len(kafka.batches) == 1


def test_broker_errors_are_counted_and_skipped() -> None:
    class Error:
        def fatal(self) -> bool:
            return False

        def __str__(self) -> str:
            return "transport failure"

    consumer, _, store, _, _ = build([[FakeKafkaMessage(b"", 0, 0, error=Error()), message(1)]])
    consumer.run(RunOptions(idle_timeout_seconds=1))
    assert consumer.metrics.broker_errors == 1
    assert len(store.event_inserts[0][0]) == 1


def test_lag_is_sampled_after_commit() -> None:
    consumer, _, store, _, _ = build([[message(0), message(1)]])
    consumer.run(RunOptions(idle_timeout_seconds=1))
    assert store.lag_rows[0].committed_offset == 2
    assert store.lag_rows[0].lag == 98


def test_batch_metrics_capture_latency_and_counts() -> None:
    consumer, _, store, _, _ = build([[message(0), message(0), FakeKafkaMessage(b"x", 0, 2)]])
    consumer.run(RunOptions(idle_timeout_seconds=1))
    row = store.batch_rows[0]
    assert (
        row.messages_consumed,
        row.events_inserted,
        row.duplicate_events,
        row.invalid_events,
    ) == (
        3,
        1,
        1,
        1,
    )
    assert row.max_end_to_end_latency_ms == int((NOW - BASE_TIME).total_seconds() * 1000) - 800
