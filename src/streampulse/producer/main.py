"""Publish deterministic synthetic events to the events topic.

Messages are keyed by ``session_id`` so that all events of a session land on
one partition and keep their production order. The producer is idempotent at
the broker level (``enable.idempotence``), which prevents duplicates caused by
its own internal retries. The generator still injects application-level
duplicates on purpose to exercise consumer deduplication.

Live runs re-stamp each message: ``produced_at`` becomes the wall-clock send
time and ``event_timestamp`` keeps its generated delay relative to it.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from types import FrameType
from typing import Any, Protocol

from streampulse.config import Settings
from streampulse.events.generator import (
    EventGenerator,
    GeneratedMessage,
    GeneratorConfig,
    MessageKind,
)
from streampulse.logging_config import configure_logging
from streampulse.monitoring.metrics import ProducerMetrics
from streampulse.retry import RetriesExhaustedError, RetryPolicy, TransientError, retry_call

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_BROKER_UNAVAILABLE = 2
EXIT_DELIVERY_FAILURES = 3


class ProducerLike(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float = ...) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...

    def list_topics(self, topic: str | None = ..., timeout: float = ...) -> Any: ...


class BrokerUnavailableError(TransientError):
    pass


def _utc_now() -> datetime:
    return datetime.now(UTC)


class EventPublisher:
    def __init__(
        self,
        producer: ProducerLike,
        topic: str,
        *,
        clock: Callable[[], datetime] = _utc_now,
        rebase_timestamps: bool = True,
    ) -> None:
        self.producer = producer
        self.topic = topic
        self.clock = clock
        self.rebase_timestamps = rebase_timestamps
        self.metrics = ProducerMetrics()
        self._sent_payloads: dict[str, bytes] = {}

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.metrics.delivery_failures += 1
            logger.error(
                "Delivery failed",
                extra={"error": str(err), "key": (msg.key() or b"").decode(errors="replace")},
            )
        else:
            self.metrics.delivered += 1

    def encode(self, message: GeneratedMessage) -> bytes:
        event_id = str(message.event.event_id)
        if message.kind is MessageKind.DUPLICATE and event_id in self._sent_payloads:
            return self._sent_payloads.pop(event_id)
        payload = message.encode(self.clock() if self.rebase_timestamps else None)
        if message.kind is MessageKind.VALID:
            self._sent_payloads[event_id] = payload
            if len(self._sent_payloads) > 10_000:
                self._sent_payloads.pop(next(iter(self._sent_payloads)))
        return payload

    def publish(self, message: GeneratedMessage) -> None:
        value = self.encode(message)
        self.metrics.attempted += 1
        self.metrics.by_kind[message.kind.value] += 1
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    key=message.key.encode(),
                    value=value,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self.metrics.buffer_full_retries += 1
                self.producer.poll(0.5)
        self.producer.poll(0)

    def flush(self, timeout_seconds: float) -> int:
        return self.producer.flush(timeout_seconds)


def wait_for_broker(producer: ProducerLike, topic: str, policy: RetryPolicy) -> None:
    def check() -> None:
        try:
            metadata = producer.list_topics(topic, timeout=5.0)
        except Exception as exc:
            raise BrokerUnavailableError(str(exc)) from exc
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            reason = "missing" if topic_metadata is None else str(topic_metadata.error)
            raise BrokerUnavailableError(f"topic {topic!r} unavailable: {reason}")
        logger.info(
            "Broker reachable", extra={"topic": topic, "partitions": len(topic_metadata.partitions)}
        )

    retry_call(check, policy, operation="broker_metadata", retry_on=(BrokerUnavailableError,))


def publish_stream(
    publisher: EventPublisher,
    messages: Iterable[GeneratedMessage],
    *,
    count: int | None,
    rate_per_second: float | None,
    should_stop: Callable[[], bool],
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    started = monotonic()
    sent = 0
    for message in messages:
        if should_stop() or (count is not None and sent >= count):
            break
        if rate_per_second:
            target = started + sent / rate_per_second
            delay = target - monotonic()
            if delay > 0:
                sleep(delay)
        publisher.publish(message)
        sent += 1
        if sent % 1000 == 0:
            logger.info("Producer progress", extra=publisher.metrics.snapshot())
    return sent


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish synthetic e-commerce events.")
    parser.add_argument(
        "--count", type=int, default=2000, help="messages to publish; 0 runs until interrupted"
    )
    parser.add_argument("--rate", type=float, default=200.0, help="messages per second; 0 = max")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sessions-per-minute", type=float, default=120.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--late-rate", type=float, default=0.03)
    parser.add_argument("--invalid-rate", type=float, default=0.01)
    parser.add_argument(
        "--keep-generated-timestamps",
        action="store_true",
        help="publish simulated timestamps instead of re-stamping with the wall clock",
    )
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    return parser


def build_producer(settings: Settings) -> Any:
    from confluent_kafka import Producer

    def on_error(err: Any) -> None:
        logger.error("Producer client error", extra={"error": str(err)})

    return Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "client.id": f"{settings.kafka.client_id}-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
            "message.timeout.ms": 30_000,
            "error_cb": on_error,
        }
    )


def main(argv: list[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    args = build_arg_parser().parse_args(argv)

    stop_requested = False

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        nonlocal stop_requested
        logger.info("Shutdown requested", extra={"signal": signal.Signals(signum).name})
        stop_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    producer = build_producer(settings)
    try:
        wait_for_broker(
            producer,
            settings.kafka.events_topic,
            RetryPolicy(max_attempts=10, base_delay_seconds=1.0, max_delay_seconds=5.0),
        )
    except RetriesExhaustedError as exc:
        logger.error(
            "Broker or topic unavailable",
            extra={"bootstrap_servers": settings.kafka.bootstrap_servers, "error": str(exc)},
        )
        return EXIT_BROKER_UNAVAILABLE

    generator = EventGenerator(
        GeneratorConfig(
            seed=args.seed,
            start_time=_utc_now(),
            sessions_per_minute=args.sessions_per_minute,
            duplicate_rate=args.duplicate_rate,
            late_rate=args.late_rate,
            invalid_rate=args.invalid_rate,
        )
    )
    publisher = EventPublisher(
        producer,
        settings.kafka.events_topic,
        rebase_timestamps=not args.keep_generated_timestamps,
    )
    logger.info(
        "Publishing synthetic events",
        extra={
            "topic": settings.kafka.events_topic,
            "count": args.count or "unbounded",
            "rate_per_second": args.rate or "unthrottled",
            "seed": args.seed,
        },
    )
    sent = publish_stream(
        publisher,
        generator,
        count=args.count or None,
        rate_per_second=args.rate or None,
        should_stop=lambda: stop_requested,
    )
    undelivered = publisher.flush(args.flush_timeout)
    summary = {"sent": sent, "undelivered_after_flush": undelivered, **publisher.metrics.snapshot()}
    if undelivered or publisher.metrics.delivery_failures:
        logger.error("Producer finished with delivery failures", extra=summary)
        return EXIT_DELIVERY_FAILURES
    logger.info("Producer finished", extra=summary)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
