"""In-process counters.

Counters are cumulative for the life of the process and are logged on shutdown
(and periodically by the consumer). Durable, queryable metrics live in the
``consumer_batches`` and ``consumer_lag`` ClickHouse tables.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field


@dataclass
class ProducerMetrics:
    attempted: int = 0
    delivered: int = 0
    delivery_failures: int = 0
    buffer_full_retries: int = 0
    by_kind: Counter[str] = field(default_factory=Counter)

    def snapshot(self) -> dict[str, int]:
        data = {
            "messages_attempted": self.attempted,
            "messages_delivered": self.delivered,
            "delivery_failures": self.delivery_failures,
            "buffer_full_retries": self.buffer_full_retries,
        }
        data.update({f"kind_{kind}": count for kind, count in sorted(self.by_kind.items())})
        return data


@dataclass
class ConsumerMetrics:
    batches_committed: int = 0
    messages_consumed: int = 0
    events_written: int = 0
    invalid_events: int = 0
    duplicate_events: int = 0
    late_events: int = 0
    events_with_unexpected_fields: int = 0
    write_retries: int = 0
    failed_writes: int = 0
    dead_lettered: int = 0
    broker_errors: int = 0
    rejections_by_reason: Counter[str] = field(default_factory=Counter)

    def snapshot(self) -> dict[str, int]:
        data = {
            "batches_committed": self.batches_committed,
            "messages_consumed": self.messages_consumed,
            "events_written": self.events_written,
            "invalid_events": self.invalid_events,
            "duplicate_events": self.duplicate_events,
            "late_events": self.late_events,
            "events_with_unexpected_fields": self.events_with_unexpected_fields,
            "write_retries": self.write_retries,
            "failed_writes": self.failed_writes,
            "dead_lettered": self.dead_lettered,
            "broker_errors": self.broker_errors,
        }
        data.update(
            {
                f"rejected_{reason}": count
                for reason, count in sorted(self.rejections_by_reason.items())
            }
        )
        return data
