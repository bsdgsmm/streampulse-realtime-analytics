"""Deterministic synthetic e-commerce event generator.

Sessions start according to a Poisson process on a simulated clock. Each session
walks a funnel (view -> cart -> checkout -> order -> payment / cancellation).
Every event gets a client-to-producer delivery delay; a configurable share of
events are delayed by minutes, which makes them arrive late and out of order.

The generator also injects the faults the consumer has to cope with:

* duplicates - the same serialized message re-sent, as a producer retry would;
* invalid messages - malformed JSON or contract violations;
* additive fields - valid events carrying a field the schema does not know.

Given the same ``GeneratorConfig`` the output is byte-for-byte identical.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import logging
import random
import sys
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from streampulse.events.schemas import (
    SCHEMA_VERSION,
    SUPPORTED_CURRENCIES,
    SYNTHETIC_SOURCE,
    EcommerceEvent,
    EventType,
)

logger = logging.getLogger(__name__)

DEFAULT_START_TIME = datetime(2026, 1, 5, 9, 0, tzinfo=UTC)


class MessageKind(StrEnum):
    VALID = "valid"
    DUPLICATE = "duplicate"
    INVALID = "invalid"


class Corruption(StrEnum):
    MALFORMED_JSON = "malformed_json"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    NEGATIVE_AMOUNT = "negative_amount"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"
    WRONG_FIELD_TYPE = "wrong_field_type"


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    start_time: datetime = DEFAULT_START_TIME
    sessions_per_minute: float = 120.0
    user_count: int = 5_000
    product_count: int = 300
    duplicate_rate: float = 0.02
    late_rate: float = 0.03
    invalid_rate: float = 0.01
    extra_field_rate: float = 0.01
    normal_delay_seconds: tuple[float, float] = (0.05, 2.0)
    late_delay_seconds: tuple[float, float] = (120.0, 900.0)
    duplicate_delay_seconds: tuple[float, float] = (0.2, 5.0)

    def __post_init__(self) -> None:
        if self.start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware")
        for name in ("duplicate_rate", "late_rate", "invalid_rate", "extra_field_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1, got {value}")
        if self.sessions_per_minute <= 0:
            raise ValueError("sessions_per_minute must be positive")
        if self.user_count < 1 or self.product_count < 1:
            raise ValueError("user_count and product_count must be positive")
        for name in ("normal_delay_seconds", "late_delay_seconds", "duplicate_delay_seconds"):
            low, high = getattr(self, name)
            if low < 0 or high < low:
                raise ValueError(f"{name} must be a non-negative (low, high) range")


@dataclass(frozen=True)
class GeneratedMessage:
    """A message as it will be written to the topic, plus generation metadata."""

    key: str
    kind: MessageKind
    event: EcommerceEvent
    send_time: datetime
    corruption: Corruption | None = None
    extra_fields: dict[str, object] = field(default_factory=dict)

    def payload(self, rebase_to: datetime | None = None) -> dict[str, object] | None:
        """Return the JSON object for this message, or ``None`` for unparsable payloads."""
        if self.corruption is Corruption.MALFORMED_JSON:
            return None
        event = rebase_event(self.event, rebase_to) if rebase_to else self.event
        data = event.to_json_dict()
        data.update(self.extra_fields)
        if self.corruption is Corruption.MISSING_REQUIRED_FIELD:
            data.pop("user_id", None)
        elif self.corruption is Corruption.UNKNOWN_EVENT_TYPE:
            data["event_type"] = "product_teleported"
        elif self.corruption is Corruption.NEGATIVE_AMOUNT:
            data["amount"] = "-" + str(data["amount"])
        elif self.corruption is Corruption.UNSUPPORTED_SCHEMA_VERSION:
            data["schema_version"] = 99
        elif self.corruption is Corruption.WRONG_FIELD_TYPE:
            data["user_id"] = 123456
        return data

    def encode(self, rebase_to: datetime | None = None) -> bytes:
        if self.corruption is Corruption.MALFORMED_JSON:
            event = rebase_event(self.event, rebase_to) if rebase_to else self.event
            text = _dumps(event.to_json_dict())
            return text[: len(text) // 2].encode()
        payload = self.payload(rebase_to)
        return _dumps(payload).encode()


def _dumps(data: object) -> str:
    return json.dumps(data, separators=(",", ":"), sort_keys=True)


def rebase_event(event: EcommerceEvent, produced_at: datetime) -> EcommerceEvent:
    """Shift both timestamps so ``produced_at`` equals the given time.

    The delivery delay (``produced_at - event_timestamp``) is preserved, so late
    events stay late when a live producer re-stamps them with wall-clock time.
    """
    delay = event.produced_at - event.event_timestamp
    return event.model_copy(
        update={"produced_at": produced_at, "event_timestamp": produced_at - delay}
    )


@dataclass
class _SessionPlan:
    user_id: str
    session_id: str
    currency: str
    steps: list[tuple[EventType, datetime, dict[str, object]]]


class EventGenerator:
    """Infinite, deterministic stream of :class:`GeneratedMessage` ordered by send time."""

    def __init__(self, config: GeneratorConfig | None = None) -> None:
        self.config = config or GeneratorConfig()
        self._rng = random.Random(self.config.seed)
        self._sequence = itertools.count()
        self._heap: list[tuple[datetime, int, GeneratedMessage]] = []
        self._clock = self.config.start_time
        product_ids = list(range(self.config.product_count))
        self._product_weights = [1.0 / (index + 1) ** 0.8 for index in product_ids]
        self._product_ids = product_ids

    def __iter__(self) -> Iterator[GeneratedMessage]:
        session_rate_per_second = self.config.sessions_per_minute / 60.0
        while True:
            next_session_start = self._clock + timedelta(
                seconds=round(self._rng.expovariate(session_rate_per_second), 3)
            )
            while self._heap and self._heap[0][0] <= next_session_start:
                yield heapq.heappop(self._heap)[2]
            self._clock = next_session_start
            self._schedule_session(self._plan_session(next_session_start))

    def take(self, count: int) -> list[GeneratedMessage]:
        return list(itertools.islice(self, count))

    def _new_uuid(self) -> uuid.UUID:
        return uuid.UUID(int=self._rng.getrandbits(128), version=4)

    def _gap(self, low: float, high: float) -> timedelta:
        return timedelta(seconds=round(self._rng.uniform(low, high), 3))

    @staticmethod
    def _price(product_index: int) -> Decimal:
        cents = 499 + (product_index * 7919) % 25_000
        return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))

    def _plan_session(self, start: datetime) -> _SessionPlan:
        rng = self._rng
        user_id = f"u_{rng.randrange(self.config.user_count):06d}"
        session_id = f"s_{rng.getrandbits(64):016x}"
        currency = rng.choices(SUPPORTED_CURRENCIES, weights=(60, 25, 10, 5))[0]
        steps: list[tuple[EventType, datetime, dict[str, object]]] = []
        now = start
        cart_total = Decimal("0")

        view_count = 1 + min(int(rng.expovariate(0.55)), 9)
        for _ in range(view_count):
            product_index = rng.choices(self._product_ids, weights=self._product_weights)[0]
            product_id = f"p_{product_index:04d}"
            steps.append((EventType.PRODUCT_VIEWED, now, {"product_id": product_id}))
            now += self._gap(3, 45)
            if rng.random() < 0.3:
                steps.append((EventType.PRODUCT_ADDED_TO_CART, now, {"product_id": product_id}))
                cart_total += self._price(product_index)
                now += self._gap(2, 30)

        if cart_total > 0 and rng.random() < 0.55:
            money = {"amount": cart_total, "currency": currency}
            steps.append((EventType.CHECKOUT_STARTED, now, dict(money)))
            now += self._gap(10, 90)
            if rng.random() < 0.7:
                order_id = f"o_{rng.getrandbits(64):016x}"
                steps.append((EventType.ORDER_PLACED, now, {"order_id": order_id, **money}))
                now += self._gap(1, 15)
                if rng.random() < 0.9:
                    steps.append(
                        (EventType.PAYMENT_COMPLETED, now, {"order_id": order_id, **money})
                    )
                    now += self._gap(30, 300)
                    if rng.random() < 0.04:
                        steps.append(
                            (EventType.ORDER_CANCELLED, now, {"order_id": order_id, **money})
                        )
                else:
                    steps.append((EventType.ORDER_CANCELLED, now, {"order_id": order_id}))

        return _SessionPlan(user_id, session_id, currency, steps)

    def _schedule_session(self, plan: _SessionPlan) -> None:
        cfg = self.config
        rng = self._rng
        for event_type, event_time, attributes in plan.steps:
            late = rng.random() < cfg.late_rate
            delay_range = cfg.late_delay_seconds if late else cfg.normal_delay_seconds
            produced_at = event_time + self._gap(*delay_range)
            event = EcommerceEvent(
                event_id=self._new_uuid(),
                event_type=event_type,
                event_timestamp=event_time,
                produced_at=produced_at,
                user_id=plan.user_id,
                session_id=plan.session_id,
                schema_version=SCHEMA_VERSION,
                source=SYNTHETIC_SOURCE,
                is_synthetic=True,
                **attributes,
            )
            extra: dict[str, object] = {}
            if rng.random() < cfg.extra_field_rate:
                extra = {"experiment_bucket": rng.choice(["control", "variant_a", "variant_b"])}
            self._push(
                GeneratedMessage(
                    plan.session_id, MessageKind.VALID, event, produced_at, extra_fields=extra
                )
            )

            if rng.random() < cfg.duplicate_rate:
                resend_at = produced_at + self._gap(*cfg.duplicate_delay_seconds)
                self._push(
                    GeneratedMessage(
                        plan.session_id,
                        MessageKind.DUPLICATE,
                        event,
                        resend_at,
                        extra_fields=extra,
                    )
                )

            if rng.random() < cfg.invalid_rate:
                self._push(self._make_invalid(plan.session_id, event))

    def _make_invalid(self, key: str, template: EcommerceEvent) -> GeneratedMessage:
        choices = [
            Corruption.MALFORMED_JSON,
            Corruption.MISSING_REQUIRED_FIELD,
            Corruption.UNKNOWN_EVENT_TYPE,
            Corruption.UNSUPPORTED_SCHEMA_VERSION,
            Corruption.WRONG_FIELD_TYPE,
        ]
        if template.amount is not None:
            choices.append(Corruption.NEGATIVE_AMOUNT)
        corruption = self._rng.choice(choices)
        send_time = template.produced_at + self._gap(0.01, 0.5)
        event = template.model_copy(update={"event_id": self._new_uuid(), "produced_at": send_time})
        return GeneratedMessage(key, MessageKind.INVALID, event, send_time, corruption=corruption)

    def _push(self, message: GeneratedMessage) -> None:
        heapq.heappush(self._heap, (message.send_time, next(self._sequence), message))


def summarize(messages: list[GeneratedMessage]) -> dict[str, int]:
    counts = Counter(message.kind.value for message in messages)
    counts["late_valid"] = sum(
        1
        for message in messages
        if message.kind is MessageKind.VALID
        and message.event.produced_at - message.event.event_timestamp > timedelta(seconds=60)
    )
    return dict(counts)


def write_jsonl(messages: list[GeneratedMessage], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        for message in messages:
            handle.write(message.encode())
            handle.write(b"\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic events to JSONL."
    )
    parser.add_argument("--count", type=int, default=1000, help="number of messages to generate")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/sample_events.jsonl"))
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--late-rate", type=float, default=0.03)
    parser.add_argument("--invalid-rate", type=float, default=0.01)
    return parser


def main(argv: list[str] | None = None) -> int:
    from streampulse.config import Settings
    from streampulse.logging_config import configure_logging

    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    args = build_arg_parser().parse_args(argv)
    if args.count < 1:
        logger.error("--count must be positive")
        return 2
    config = GeneratorConfig(
        seed=args.seed,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        invalid_rate=args.invalid_rate,
    )
    messages = EventGenerator(config).take(args.count)
    write_jsonl(messages, args.output)
    logger.info(
        "Wrote synthetic events",
        extra={"path": str(args.output), "seed": args.seed, **summarize(messages)},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
