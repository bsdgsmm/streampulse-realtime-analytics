"""Shared builders for test payloads and broker messages."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from streampulse.consumer.processor import RawMessage
from streampulse.events.schemas import SCHEMA_VERSION, SYNTHETIC_SOURCE

FIXTURES = Path(__file__).parent / "fixtures"
BASE_TIME = datetime(2026, 1, 5, 9, 0, tzinfo=UTC)
TOPIC = "streampulse.ecommerce.events.v1"


def event_payload(**overrides: Any) -> dict[str, Any]:
    """A valid product_viewed payload as it appears on the wire."""
    payload: dict[str, Any] = {
        "event_id": "9d3e8b8a-1c2f-4a5b-8c7d-000000000001",
        "event_type": "product_viewed",
        "event_timestamp": BASE_TIME.isoformat(),
        "produced_at": (BASE_TIME + timedelta(milliseconds=800)).isoformat(),
        "user_id": "u_000042",
        "session_id": "s_00000000000000aa",
        "product_id": "p_0007",
        "schema_version": SCHEMA_VERSION,
        "source": SYNTHETIC_SOURCE,
        "is_synthetic": True,
    }
    payload.update(overrides)
    return {key: value for key, value in payload.items() if value is not None}


def raw_message(
    payload: dict[str, Any] | bytes | None,
    *,
    partition: int = 0,
    offset: int = 0,
    topic: str = TOPIC,
) -> RawMessage:
    if isinstance(payload, dict):
        value: bytes | None = json.dumps(payload).encode()
    else:
        value = payload
    return RawMessage(topic=topic, partition=partition, offset=offset, key=b"k", value=value)
