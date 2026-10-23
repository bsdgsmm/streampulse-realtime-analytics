"""Dead-letter publishing for messages that can never be processed as-is.

The original message value and key are forwarded unchanged; the failure
context travels in headers so the payload stays replayable byte-for-byte.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, Protocol

from streampulse.consumer.processor import Rejection

logger = logging.getLogger(__name__)

HEADER_PREFIX = "dlq."


class DeadLetterError(Exception):
    """Raised when rejected messages could not be durably written to the DLQ."""


class ProducerLike(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float = ...) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...


def build_headers(rejection: Rejection, consumer_group: str) -> list[tuple[str, bytes]]:
    source = rejection.source
    values = {
        "error_type": rejection.reason.value,
        "error_message": rejection.message,
        "source_topic": source.topic,
        "source_partition": str(source.partition),
        "source_offset": str(source.offset),
        "failed_at": rejection.rejected_at.isoformat(),
        "consumer_group": consumer_group,
    }
    if rejection.event_id:
        values["event_id"] = rejection.event_id
    return [(HEADER_PREFIX + name, value.encode()) for name, value in values.items()]


def parse_headers(headers: Sequence[tuple[str, bytes | None]] | None) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for name, value in headers or []:
        if name.startswith(HEADER_PREFIX):
            parsed[name[len(HEADER_PREFIX) :]] = (value or b"").decode("utf-8", errors="replace")
    return parsed


class DeadLetterPublisher:
    def __init__(
        self,
        producer: ProducerLike,
        topic: str,
        consumer_group: str,
        flush_timeout_seconds: float = 30.0,
    ) -> None:
        self.producer = producer
        self.topic = topic
        self.consumer_group = consumer_group
        self.flush_timeout_seconds = flush_timeout_seconds

    def publish(self, rejections: Sequence[Rejection]) -> int:
        if not rejections:
            return 0
        errors: list[str] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(str(err))

        for rejection in rejections:
            kwargs = {
                "value": rejection.source.value or b"",
                "key": rejection.source.key,
                "headers": build_headers(rejection, self.consumer_group),
                "on_delivery": on_delivery,
            }
            while True:
                try:
                    self.producer.produce(self.topic, **kwargs)
                    break
                except BufferError:
                    self.producer.poll(0.5)

        remaining = self.producer.flush(self.flush_timeout_seconds)
        if remaining or errors:
            raise DeadLetterError(
                f"{len(errors)} DLQ delivery errors, {remaining} messages still queued: "
                + "; ".join(errors[:3])
            )
        logger.info(
            "Rejected messages sent to dead-letter topic",
            extra={"dlq_topic": self.topic, "count": len(rejections)},
        )
        return len(rejections)
