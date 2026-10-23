"""ClickHouse access layer.

The store converts domain objects into rows and translates driver exceptions
into :class:`TransientStorageError` (retry) or :class:`PermanentStorageError`
(do not retry). The ``clickhouse_connect`` import is deferred so that unit
tests and the producer do not need the driver.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from streampulse.config import ClickHouseSettings
from streampulse.consumer.processor import ProcessedEvent, Rejection
from streampulse.retry import TransientError

logger = logging.getLogger(__name__)

MAX_STORED_PAYLOAD_BYTES = 4_096

EVENT_COLUMNS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "event_timestamp",
    "produced_at",
    "ingested_at",
    "user_id",
    "session_id",
    "product_id",
    "order_id",
    "amount",
    "currency",
    "schema_version",
    "source",
    "is_synthetic",
    "is_late",
    "lateness_ms",
    "ingestion_delay_ms",
    "end_to_end_latency_ms",
    "unexpected_fields",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "broker_timestamp",
    "consumer_id",
)

REJECTION_COLUMNS: tuple[str, ...] = (
    "rejected_at",
    "error_type",
    "error_message",
    "event_id",
    "raw_payload",
    "payload_bytes",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "consumer_id",
)

BATCH_COLUMNS: tuple[str, ...] = (
    "batch_id",
    "consumer_id",
    "consumer_group",
    "batch_started_at",
    "batch_finished_at",
    "messages_consumed",
    "events_inserted",
    "invalid_events",
    "duplicate_events",
    "late_events",
    "unexpected_field_events",
    "write_retries",
    "processing_ms",
    "avg_end_to_end_latency_ms",
    "max_end_to_end_latency_ms",
    "max_ingestion_delay_ms",
)

LAG_COLUMNS: tuple[str, ...] = (
    "observed_at",
    "consumer_group",
    "topic",
    "partition",
    "committed_offset",
    "high_watermark",
    "lag",
)

TRANSIENT_SERVER_ERROR_CODES = frozenset(
    {
        159,
        202,
        209,
        210,
        241,
        252,
        425,
    }
)


class TransientStorageError(TransientError):
    pass


class PermanentStorageError(Exception):
    pass


@dataclass(frozen=True)
class BatchMetricsRow:
    batch_id: str
    consumer_id: str
    consumer_group: str
    batch_started_at: datetime
    batch_finished_at: datetime
    messages_consumed: int
    events_inserted: int
    invalid_events: int
    duplicate_events: int
    late_events: int
    unexpected_field_events: int
    write_retries: int
    processing_ms: int
    avg_end_to_end_latency_ms: float
    max_end_to_end_latency_ms: int
    max_ingestion_delay_ms: int

    def as_row(self) -> list[Any]:
        return [getattr(self, column) for column in BATCH_COLUMNS]


@dataclass(frozen=True)
class LagRow:
    observed_at: datetime
    consumer_group: str
    topic: str
    partition: int
    committed_offset: int
    high_watermark: int
    lag: int

    def as_row(self) -> list[Any]:
        return [getattr(self, column) for column in LAG_COLUMNS]


def event_to_row(item: ProcessedEvent, consumer_id: str) -> list[Any]:
    event = item.event
    source = item.source
    return [
        event.event_id,
        event.event_type.value,
        event.event_timestamp,
        event.produced_at,
        item.ingested_at,
        event.user_id,
        event.session_id,
        event.product_id,
        event.order_id,
        event.amount,
        event.currency,
        event.schema_version,
        event.source,
        event.is_synthetic,
        item.is_late,
        item.lateness_ms,
        item.ingestion_delay_ms,
        item.end_to_end_latency_ms,
        list(item.unexpected_fields),
        source.topic,
        source.partition,
        source.offset,
        source.broker_timestamp,
        consumer_id,
    ]


def rejection_to_row(rejection: Rejection, consumer_id: str) -> list[Any]:
    raw = rejection.source.value or b""
    return [
        rejection.rejected_at,
        rejection.reason.value,
        rejection.message,
        rejection.event_id,
        raw[:MAX_STORED_PAYLOAD_BYTES].decode("utf-8", errors="replace"),
        len(raw),
        rejection.source.topic,
        rejection.source.partition,
        rejection.source.offset,
        consumer_id,
    ]


def deduplication_token(prefix: str, identifiers: Iterable[str]) -> str:
    """Stable token for ClickHouse insert deduplication, independent of order."""
    digest = hashlib.sha256()
    for identifier in sorted(identifiers):
        digest.update(identifier.encode())
        digest.update(b"\n")
    return f"{prefix}:{digest.hexdigest()}"


def classify_exception(exc: BaseException) -> Exception:
    """Map a driver or network exception onto the retry taxonomy."""
    if isinstance(exc, TransientStorageError | PermanentStorageError):
        return exc
    if isinstance(exc, ConnectionError | TimeoutError | OSError):
        return TransientStorageError(str(exc))
    try:
        from clickhouse_connect.driver import exceptions as ch_exceptions
    except ImportError:
        return PermanentStorageError(str(exc))

    if isinstance(exc, ch_exceptions.OperationalError):
        return TransientStorageError(str(exc))
    if isinstance(exc, ch_exceptions.DatabaseError):
        code = getattr(exc, "code", None)
        if code is None:
            code = _extract_error_code(str(exc))
        if code in TRANSIENT_SERVER_ERROR_CODES:
            return TransientStorageError(str(exc))
    return PermanentStorageError(str(exc))


def _extract_error_code(text: str) -> int | None:
    marker = "Code: "
    start = text.find(marker)
    if start == -1:
        return None
    digits = ""
    for char in text[start + len(marker) :]:
        if not char.isdigit():
            break
        digits += char
    return int(digits) if digits else None


class ClickHouseClient(Protocol):
    def insert(self, table: str, data: Sequence[Sequence[Any]], **kwargs: Any) -> Any: ...

    def query(self, query: str, **kwargs: Any) -> Any: ...

    def command(self, cmd: str, **kwargs: Any) -> Any: ...

    def close(self) -> None: ...


class ClickHouseStore:
    def __init__(self, client: ClickHouseClient, consumer_id: str = "unknown") -> None:
        self.client = client
        self.consumer_id = consumer_id

    @classmethod
    def connect(
        cls,
        settings: ClickHouseSettings,
        consumer_id: str = "unknown",
        database: str | None = None,
    ) -> ClickHouseStore:
        import clickhouse_connect

        try:
            client = clickhouse_connect.get_client(
                host=settings.host,
                port=settings.port,
                username=settings.user,
                password=settings.password,
                database=database if database is not None else settings.database,
                secure=settings.secure,
                connect_timeout=settings.connect_timeout_seconds,
                send_receive_timeout=settings.send_receive_timeout_seconds,
                settings={"date_time_input_format": "best_effort"},
            )
        except Exception as exc:
            raise classify_exception(exc) from exc
        return cls(client, consumer_id=consumer_id)

    def _call(self, operation: str, func: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            mapped = classify_exception(exc)
            logger.debug("ClickHouse %s failed: %s", operation, exc)
            raise mapped from exc

    def ping(self) -> bool:
        result = self._call("ping", self.client.command, "SELECT 1")
        return str(result).strip() == "1"

    def insert_events(self, events: Sequence[ProcessedEvent], dedup_token: str) -> int:
        if not events:
            return 0
        rows = [event_to_row(item, self.consumer_id) for item in events]
        self._call(
            "insert_events",
            self.client.insert,
            "events",
            rows,
            column_names=list(EVENT_COLUMNS),
            settings={"insert_deduplication_token": dedup_token},
        )
        return len(rows)

    def insert_rejections(self, rejections: Sequence[Rejection], dedup_token: str) -> int:
        if not rejections:
            return 0
        rows = [rejection_to_row(item, self.consumer_id) for item in rejections]
        self._call(
            "insert_rejections",
            self.client.insert,
            "event_rejections",
            rows,
            column_names=list(REJECTION_COLUMNS),
            settings={"insert_deduplication_token": dedup_token},
        )
        return len(rows)

    def insert_batch_metrics(self, row: BatchMetricsRow) -> None:
        self._call(
            "insert_batch_metrics",
            self.client.insert,
            "consumer_batches",
            [row.as_row()],
            column_names=list(BATCH_COLUMNS),
        )

    def insert_lag(self, rows: Sequence[LagRow]) -> None:
        if not rows:
            return
        self._call(
            "insert_lag",
            self.client.insert,
            "consumer_lag",
            [row.as_row() for row in rows],
            column_names=list(LAG_COLUMNS),
        )

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self._call("query", self.client.query, sql, parameters=parameters or {})
        columns = list(result.column_names)
        return [dict(zip(columns, row, strict=True)) for row in result.result_rows]

    def command(self, sql: str) -> Any:
        return self._call("command", self.client.command, sql)

    def close(self) -> None:
        self.client.close()
