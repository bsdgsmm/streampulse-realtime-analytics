"""Consumer entry point: Kafka topic -> validation -> ClickHouse.

Delivery semantics are at-least-once. For every batch the consumer:

1. classifies messages (valid, duplicate, rejected) without side effects;
2. publishes rejected messages to the dead-letter topic and waits for acks;
3. writes rejections and events to ClickHouse with bounded retries, using a
   deterministic ``insert_deduplication_token`` so a retried insert is a no-op;
4. commits the next offsets synchronously.

Offsets are committed only after step 2 and 3 succeeded. If the database stays
unavailable after all retries, the consumer exits without committing and the
batch is redelivered after restart; storage-level deduplication absorbs the
replay.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import FrameType
from typing import Any, Protocol

from streampulse.config import Settings
from streampulse.consumer.dead_letter import DeadLetterError, DeadLetterPublisher
from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.processor import (
    BatchResult,
    EventProcessor,
    RawMessage,
    highest_offsets,
)
from streampulse.logging_config import configure_logging
from streampulse.monitoring.lag import PartitionLag
from streampulse.monitoring.metrics import ConsumerMetrics
from streampulse.retry import RetriesExhaustedError, RetryPolicy, retry_call
from streampulse.storage.clickhouse import (
    BatchMetricsRow,
    ClickHouseStore,
    LagRow,
    PermanentStorageError,
    TransientStorageError,
    deduplication_token,
)

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_STORAGE_FAILURE = 2
EXIT_DLQ_FAILURE = 3


class KafkaConsumerLike(Protocol):
    def consume(self, num_messages: int = ..., timeout: float = ...) -> list[Any]: ...

    def commit(self, offsets: list[Any] | None = ..., asynchronous: bool = ...) -> Any: ...

    def assignment(self) -> list[Any]: ...

    def committed(self, partitions: list[Any], timeout: float = ...) -> list[Any]: ...

    def get_watermark_offsets(
        self, partition: Any, timeout: float = ..., cached: bool = ...
    ) -> tuple[int, int]: ...

    def close(self) -> None: ...


class StoreLike(Protocol):
    def insert_events(self, events: Sequence[Any], dedup_token: str) -> int: ...

    def insert_rejections(self, rejections: Sequence[Any], dedup_token: str) -> int: ...

    def insert_batch_metrics(self, row: BatchMetricsRow) -> None: ...

    def insert_lag(self, rows: Sequence[LagRow]) -> None: ...


class DeadLetterLike(Protocol):
    def publish(self, rejections: Sequence[Any]) -> int: ...


@dataclass(frozen=True)
class RunOptions:
    idle_timeout_seconds: float | None = None
    max_messages: int | None = None
    metrics_log_interval_seconds: float = 30.0


def _utc_now() -> datetime:
    return datetime.now(UTC)


def raw_from_kafka(message: Any) -> RawMessage:
    timestamp_type, timestamp_ms = message.timestamp()
    broker_timestamp = (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
        if timestamp_type and timestamp_ms and timestamp_ms > 0
        else None
    )
    return RawMessage(
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        key=message.key(),
        value=message.value(),
        broker_timestamp=broker_timestamp,
    )


def _default_topic_partition(topic: str, partition: int, offset: int) -> Any:
    from confluent_kafka import TopicPartition

    return TopicPartition(topic, partition, offset)


class StreamConsumer:
    def __init__(
        self,
        consumer: KafkaConsumerLike,
        store: StoreLike,
        dead_letters: DeadLetterLike,
        processor: EventProcessor,
        *,
        consumer_group: str,
        consumer_id: str,
        batch_size: int,
        batch_timeout_seconds: float,
        retry_policy: RetryPolicy,
        make_topic_partition: Callable[[str, int, int], Any] = _default_topic_partition,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = _utc_now,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.consumer = consumer
        self.store = store
        self.dead_letters = dead_letters
        self.processor = processor
        self.consumer_group = consumer_group
        self.consumer_id = consumer_id
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.retry_policy = retry_policy
        self.make_topic_partition = make_topic_partition
        self.sleep = sleep
        self.clock = clock
        self.monotonic = monotonic
        self.metrics = ConsumerMetrics()
        self.commit_failures = 0
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def run(self, options: RunOptions | None = None) -> int:
        options = options or RunOptions()
        exit_code = EXIT_OK
        last_message_at = self.monotonic()
        last_metrics_log = self.monotonic()
        logger.info(
            "Consumer started",
            extra={
                "consumer_group": self.consumer_group,
                "consumer_id": self.consumer_id,
                "batch_size": self.batch_size,
            },
        )
        try:
            while not self._stop_requested:
                limit = self.batch_size
                if options.max_messages is not None:
                    remaining = options.max_messages - self.metrics.messages_consumed
                    if remaining <= 0:
                        logger.info("Reached --max-messages limit")
                        break
                    limit = min(limit, remaining)

                raw = self._poll(limit)
                if not raw:
                    idle_for = self.monotonic() - last_message_at
                    if options.idle_timeout_seconds is not None and (
                        idle_for >= options.idle_timeout_seconds
                    ):
                        logger.info(
                            "No messages within idle timeout, stopping",
                            extra={"idle_timeout_seconds": options.idle_timeout_seconds},
                        )
                        break
                    continue

                last_message_at = self.monotonic()
                self.handle_batch(raw)

                if self.monotonic() - last_metrics_log >= options.metrics_log_interval_seconds:
                    logger.info("Consumer progress", extra=self.metrics.snapshot())
                    last_metrics_log = self.monotonic()
        except (RetriesExhaustedError, PermanentStorageError) as exc:
            self.metrics.failed_writes += 1
            logger.error(
                "Storage write failed; stopping without committing the current batch",
                extra={"error": str(exc)},
            )
            exit_code = EXIT_STORAGE_FAILURE
        except DeadLetterError as exc:
            logger.error(
                "Dead-letter publish failed; stopping without committing the current batch",
                extra={"error": str(exc)},
            )
            exit_code = EXIT_DLQ_FAILURE
        finally:
            self._close()
        logger.info("Consumer stopped", extra={"exit_code": exit_code, **self.metrics.snapshot()})
        return exit_code

    def _poll(self, limit: int) -> list[RawMessage]:
        messages = self.consumer.consume(num_messages=limit, timeout=self.batch_timeout_seconds)
        raw: list[RawMessage] = []
        for message in messages or []:
            error = message.error()
            if error is not None:
                self.metrics.broker_errors += 1
                fatal = bool(getattr(error, "fatal", lambda: False)())
                logger.error("Broker error", extra={"error": str(error), "fatal": fatal})
                if fatal:
                    raise RuntimeError(f"fatal broker error: {error}")
                continue
            raw.append(raw_from_kafka(message))
        return raw

    def handle_batch(self, raw: Sequence[RawMessage]) -> BatchResult:
        started_at = self.clock()
        started_monotonic = self.monotonic()
        result = self.processor.process_batch(raw)
        write_retries = 0

        def count_retry(_attempt: int, _exc: BaseException, _delay: float) -> None:
            nonlocal write_retries
            write_retries += 1
            self.metrics.write_retries += 1

        if result.rejections:
            self.dead_letters.publish(result.rejections)
            self.metrics.dead_lettered += len(result.rejections)
            rejection_token = deduplication_token(
                "rejections",
                (
                    f"{item.source.topic}:{item.source.partition}:{item.source.offset}"
                    for item in result.rejections
                ),
            )
            self._with_retry(
                "insert_rejections",
                lambda: self.store.insert_rejections(result.rejections, rejection_token),
                count_retry,
            )

        if result.events:
            event_token = deduplication_token("events", result.event_ids)
            self._with_retry(
                "insert_events",
                lambda: self.store.insert_events(result.events, event_token),
                count_retry,
            )
            self.processor.recent_events.remember(result.event_ids)

        self._commit(raw)
        self._record_batch(result, started_at, started_monotonic, write_retries)
        return result

    def _with_retry(
        self,
        operation: str,
        func: Callable[[], Any],
        on_retry: Callable[[int, BaseException, float], None],
    ) -> Any:
        return retry_call(
            func,
            self.retry_policy,
            operation=operation,
            retry_on=(TransientStorageError,),
            sleep=self.sleep,
            on_retry=on_retry,
        )

    def _commit(self, raw: Sequence[RawMessage]) -> None:
        offsets = [
            self.make_topic_partition(topic, partition, offset + 1)
            for (topic, partition), offset in sorted(highest_offsets(raw).items())
        ]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except Exception as exc:
            self.commit_failures += 1
            logger.warning(
                "Offset commit failed; the batch may be redelivered and will be deduplicated",
                extra={"error": str(exc)},
            )

    def _record_batch(
        self,
        result: BatchResult,
        started_at: datetime,
        started_monotonic: float,
        write_retries: int,
    ) -> None:
        metrics = self.metrics
        metrics.batches_committed += 1
        metrics.messages_consumed += result.messages
        metrics.events_written += len(result.events)
        metrics.invalid_events += len(result.rejections)
        metrics.duplicate_events += result.duplicates
        metrics.late_events += result.late_events
        metrics.events_with_unexpected_fields += result.events_with_unexpected_fields
        for rejection in result.rejections:
            metrics.rejections_by_reason[rejection.reason.value] += 1

        latencies = [item.end_to_end_latency_ms for item in result.events]
        delays = [item.ingestion_delay_ms for item in result.events]
        row = BatchMetricsRow(
            batch_id=str(uuid.uuid4()),
            consumer_id=self.consumer_id,
            consumer_group=self.consumer_group,
            batch_started_at=started_at,
            batch_finished_at=self.clock(),
            messages_consumed=result.messages,
            events_inserted=len(result.events),
            invalid_events=len(result.rejections),
            duplicate_events=result.duplicates,
            late_events=result.late_events,
            unexpected_field_events=result.events_with_unexpected_fields,
            write_retries=write_retries,
            processing_ms=int((self.monotonic() - started_monotonic) * 1000),
            avg_end_to_end_latency_ms=sum(latencies) / len(latencies) if latencies else 0.0,
            max_end_to_end_latency_ms=max(latencies, default=0),
            max_ingestion_delay_ms=max(delays, default=0),
        )
        lag_rows = self._sample_lag()
        try:
            self._with_retry(
                "insert_batch_metrics",
                lambda: self.store.insert_batch_metrics(row),
                lambda *_: None,
            )
            if lag_rows:
                self._with_retry(
                    "insert_lag", lambda: self.store.insert_lag(lag_rows), lambda *_: None
                )
        except (RetriesExhaustedError, PermanentStorageError) as exc:
            logger.warning(
                "Could not persist batch metrics; events were already committed",
                extra={"error": str(exc)},
            )
        logger.debug(
            "Batch committed", extra={"batch_id": row.batch_id, "events": len(result.events)}
        )

    def _sample_lag(self) -> list[LagRow]:
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return []
            committed = self.consumer.committed(assignment, timeout=5.0)
            observed_at = self.clock()
            rows = []
            for partition in committed:
                low, high = self.consumer.get_watermark_offsets(
                    partition, timeout=5.0, cached=False
                )
                lag = PartitionLag(
                    topic=partition.topic,
                    partition=partition.partition,
                    committed_offset=partition.offset,
                    high_watermark=high,
                    low_watermark=low,
                )
                rows.append(
                    LagRow(
                        observed_at=observed_at,
                        consumer_group=self.consumer_group,
                        topic=lag.topic,
                        partition=lag.partition,
                        committed_offset=lag.committed_offset,
                        high_watermark=lag.high_watermark,
                        lag=lag.lag,
                    )
                )
            return rows
        except Exception as exc:
            logger.warning("Could not sample consumer lag", extra={"error": str(exc)})
            return []

    def _close(self) -> None:
        try:
            self.consumer.close()
        except Exception as exc:
            logger.warning("Error while closing consumer", extra={"error": str(exc)})


def build_kafka_consumer(settings: Settings, watermarks: WatermarkTracker) -> Any:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": settings.kafka.consumer_group,
            "client.id": f"{settings.kafka.client_id}-consumer",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45_000,
            "max.poll.interval.ms": 300_000,
            "isolation.level": "read_committed",
        }
    )

    def on_assign(_consumer: Any, partitions: list[Any]) -> None:
        logger.info(
            "Partitions assigned",
            extra={"partitions": [f"{p.topic}[{p.partition}]" for p in partitions]},
        )

    def on_revoke(_consumer: Any, partitions: list[Any]) -> None:
        for partition in partitions:
            watermarks.forget_partition(partition.topic, partition.partition)
        logger.info(
            "Partitions revoked",
            extra={"partitions": [f"{p.topic}[{p.partition}]" for p in partitions]},
        )

    consumer.subscribe([settings.kafka.events_topic], on_assign=on_assign, on_revoke=on_revoke)
    return consumer


def build_dlq_producer(settings: Settings) -> Any:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "client.id": f"{settings.kafka.client_id}-dlq",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 30_000,
        }
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume events and write them to ClickHouse.")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="stop after this many seconds without new messages (finite demo runs)",
    )
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument(
        "--skip-migrations",
        action="store_true",
        help="do not apply sql/schema.sql on startup",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    args = build_arg_parser().parse_args(argv)
    consumer_id = f"{socket.gethostname()}-{os.getpid()}"

    if not args.skip_migrations:
        from streampulse.storage.migrate import apply_schema_with_retry

        try:
            apply_schema_with_retry(settings)
        except (RetriesExhaustedError, PermanentStorageError) as exc:
            logger.error("ClickHouse is not reachable or schema failed", extra={"error": str(exc)})
            return EXIT_STORAGE_FAILURE

    try:
        store = ClickHouseStore.connect(settings.clickhouse, consumer_id=consumer_id)
    except (TransientStorageError, PermanentStorageError) as exc:
        logger.error("Could not connect to ClickHouse", extra={"error": str(exc)})
        return EXIT_STORAGE_FAILURE

    consumer_settings = settings.consumer
    watermarks = WatermarkTracker(timedelta(seconds=consumer_settings.allowed_lateness_seconds))
    processor = EventProcessor(
        watermarks,
        RecentEventCache(consumer_settings.dedup_cache_size),
        max_future_skew=timedelta(seconds=consumer_settings.max_future_skew_seconds),
    )
    stream_consumer = StreamConsumer(
        build_kafka_consumer(settings, watermarks),
        store,
        DeadLetterPublisher(
            build_dlq_producer(settings), settings.kafka.dlq_topic, settings.kafka.consumer_group
        ),
        processor,
        consumer_group=settings.kafka.consumer_group,
        consumer_id=consumer_id,
        batch_size=consumer_settings.batch_size,
        batch_timeout_seconds=consumer_settings.batch_timeout_seconds,
        retry_policy=RetryPolicy(
            max_attempts=consumer_settings.db_max_attempts,
            base_delay_seconds=consumer_settings.db_retry_base_delay_seconds,
            max_delay_seconds=consumer_settings.db_retry_max_delay_seconds,
        ),
    )

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("Shutdown requested", extra={"signal": signal.Signals(signum).name})
        stream_consumer.request_stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        return stream_consumer.run(
            RunOptions(idle_timeout_seconds=args.idle_timeout, max_messages=args.max_messages)
        )
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
