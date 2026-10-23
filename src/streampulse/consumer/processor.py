"""Pure batch-processing logic: decode, validate, deduplicate and classify lateness.

Nothing here talks to Kafka or ClickHouse, which keeps the rules unit-testable.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from pydantic import ValidationError

from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.events.schemas import EVENT_FIELD_NAMES, SCHEMA_VERSION, EcommerceEvent

MAX_ERROR_MESSAGE_LENGTH = 1_000


class RejectionReason(StrEnum):
    EMPTY_PAYLOAD = "empty_payload"
    INVALID_JSON = "invalid_json"
    NOT_AN_OBJECT = "not_an_object"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"
    SCHEMA_VALIDATION = "schema_validation"
    FUTURE_EVENT_TIMESTAMP = "future_event_timestamp"


@dataclass(frozen=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    broker_timestamp: datetime | None = None


@dataclass(frozen=True)
class ProcessedEvent:
    event: EcommerceEvent
    source: RawMessage
    ingested_at: datetime
    is_late: bool
    lateness_ms: int
    unexpected_fields: tuple[str, ...]

    @property
    def ingestion_delay_ms(self) -> int:
        return _millis(self.ingested_at - self.event.event_timestamp)

    @property
    def end_to_end_latency_ms(self) -> int:
        return _millis(self.ingested_at - self.event.produced_at)


@dataclass(frozen=True)
class Rejection:
    source: RawMessage
    reason: RejectionReason
    message: str
    rejected_at: datetime
    event_id: str | None = None


@dataclass
class BatchResult:
    events: list[ProcessedEvent] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)
    duplicates: int = 0
    late_events: int = 0
    events_with_unexpected_fields: int = 0

    @property
    def messages(self) -> int:
        return len(self.events) + len(self.rejections) + self.duplicates

    @property
    def event_ids(self) -> list[str]:
        return [str(item.event.event_id) for item in self.events]


def _millis(delta: timedelta) -> int:
    return int(delta.total_seconds() * 1000)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _format_validation_error(error: ValidationError) -> str:
    parts = []
    for item in error.errors(include_url=False):
        location = ".".join(str(part) for part in item["loc"]) or "<event>"
        parts.append(f"{location}: {item['msg']}")
    return "; ".join(parts)[:MAX_ERROR_MESSAGE_LENGTH]


class EventProcessor:
    def __init__(
        self,
        watermarks: WatermarkTracker,
        recent_events: RecentEventCache,
        *,
        max_future_skew: timedelta = timedelta(seconds=30),
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.watermarks = watermarks
        self.recent_events = recent_events
        self.max_future_skew = max_future_skew
        self.clock = clock

    def process_batch(self, messages: Sequence[RawMessage]) -> BatchResult:
        """Classify every message in a batch.

        Events are returned in input order. Identifiers are not added to the
        duplicate cache here; the caller does that only after the write has
        succeeded, so a failed write never hides a redelivered event.
        """
        result = BatchResult()
        ids_in_batch: set[str] = set()
        for message in messages:
            ingested_at = self.clock()
            outcome = self._decode_and_validate(message, ingested_at)
            if isinstance(outcome, Rejection):
                result.rejections.append(outcome)
                continue
            event, unexpected = outcome
            event_id = str(event.event_id)
            if event_id in ids_in_batch or self.recent_events.seen(event_id):
                result.duplicates += 1
                continue
            ids_in_batch.add(event_id)

            lateness = self.watermarks.observe(
                message.topic, message.partition, event.event_timestamp
            )
            if lateness.is_late:
                result.late_events += 1
            if unexpected:
                result.events_with_unexpected_fields += 1
            result.events.append(
                ProcessedEvent(
                    event=event,
                    source=message,
                    ingested_at=ingested_at,
                    is_late=lateness.is_late,
                    lateness_ms=lateness.lateness_ms,
                    unexpected_fields=unexpected,
                )
            )
        return result

    def _decode_and_validate(
        self, message: RawMessage, now: datetime
    ) -> tuple[EcommerceEvent, tuple[str, ...]] | Rejection:
        def reject(reason: RejectionReason, text: str, event_id: object = None) -> Rejection:
            return Rejection(
                source=message,
                reason=reason,
                message=text[:MAX_ERROR_MESSAGE_LENGTH],
                rejected_at=now,
                event_id=event_id if isinstance(event_id, str) else None,
            )

        if not message.value:
            return reject(RejectionReason.EMPTY_PAYLOAD, "message value is empty")
        try:
            payload = json.loads(message.value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return reject(RejectionReason.INVALID_JSON, f"payload is not valid JSON: {exc}")
        if not isinstance(payload, dict):
            return reject(RejectionReason.NOT_AN_OBJECT, "payload must be a JSON object")

        event_id = payload.get("event_id")
        version = payload.get("schema_version")
        if version != SCHEMA_VERSION:
            return reject(
                RejectionReason.UNSUPPORTED_SCHEMA_VERSION,
                f"schema_version {version!r} is not supported (expected {SCHEMA_VERSION})",
                event_id,
            )
        try:
            event = EcommerceEvent.model_validate(payload)
        except ValidationError as exc:
            return reject(
                RejectionReason.SCHEMA_VALIDATION, _format_validation_error(exc), event_id
            )

        if event.event_timestamp > now + self.max_future_skew:
            return reject(
                RejectionReason.FUTURE_EVENT_TIMESTAMP,
                f"event_timestamp {event.event_timestamp.isoformat()} is in the future",
                event_id,
            )
        unexpected = tuple(sorted(set(payload) - EVENT_FIELD_NAMES))
        return event, unexpected


def highest_offsets(messages: Iterable[RawMessage]) -> dict[tuple[str, int], int]:
    """Return the highest offset seen per (topic, partition)."""
    offsets: dict[tuple[str, int], int] = {}
    for message in messages:
        key = (message.topic, message.partition)
        if message.offset > offsets.get(key, -1):
            offsets[key] = message.offset
    return offsets
