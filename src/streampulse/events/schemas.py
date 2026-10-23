"""Versioned event contract (schema version 1) for the e-commerce event stream.

Two timestamps travel with every event:

* ``event_timestamp`` - when the action happened on the client (event time).
* ``produced_at`` - when the producer published the message (production time).

The consumer adds a third, ``ingested_at``, when it processes the record. Event
time drives business analytics; production and ingestion time drive operational
metrics such as delivery delay and lateness.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)

SCHEMA_VERSION = 1
SYNTHETIC_SOURCE = "streampulse-synthetic-generator"
MAX_PRODUCER_CLOCK_SKEW = timedelta(seconds=5)


class EventType(StrEnum):
    PRODUCT_VIEWED = "product_viewed"
    PRODUCT_ADDED_TO_CART = "product_added_to_cart"
    CHECKOUT_STARTED = "checkout_started"
    ORDER_PLACED = "order_placed"
    PAYMENT_COMPLETED = "payment_completed"
    ORDER_CANCELLED = "order_cancelled"


PRODUCT_EVENT_TYPES = frozenset({EventType.PRODUCT_VIEWED, EventType.PRODUCT_ADDED_TO_CART})
ORDER_EVENT_TYPES = frozenset(
    {EventType.ORDER_PLACED, EventType.PAYMENT_COMPLETED, EventType.ORDER_CANCELLED}
)
AMOUNT_REQUIRED_EVENT_TYPES = frozenset({EventType.ORDER_PLACED, EventType.PAYMENT_COMPLETED})
AMOUNT_ALLOWED_EVENT_TYPES = AMOUNT_REQUIRED_EVENT_TYPES | {
    EventType.CHECKOUT_STARTED,
    EventType.ORDER_CANCELLED,
}

Currency = Literal["EUR", "USD", "GBP", "TRY"]
SUPPORTED_CURRENCIES: tuple[str, ...] = ("EUR", "USD", "GBP", "TRY")

UserId = Annotated[str, Field(pattern=r"^u_[0-9]{6}$")]
SessionId = Annotated[str, Field(pattern=r"^s_[0-9a-f]{16}$")]
ProductId = Annotated[str, Field(pattern=r"^p_[0-9]{4}$")]
OrderId = Annotated[str, Field(pattern=r"^o_[0-9a-f]{16}$")]
Amount = Annotated[
    Decimal, Field(gt=Decimal("0"), le=Decimal("100000"), max_digits=12, decimal_places=2)
]


class EcommerceEvent(BaseModel):
    """A single validated e-commerce event.

    Unknown fields are ignored rather than rejected so that additive producer
    changes do not break the consumer; the processor records their names.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    event_id: UUID
    event_type: EventType
    event_timestamp: AwareDatetime
    produced_at: AwareDatetime
    user_id: UserId
    session_id: SessionId
    product_id: ProductId | None = None
    order_id: OrderId | None = None
    amount: Amount | None = None
    currency: Currency | None = None
    schema_version: Literal[1]
    source: Annotated[str, Field(min_length=1, max_length=64)]
    is_synthetic: StrictBool

    @field_validator("event_timestamp", "produced_at")
    @classmethod
    def _normalize_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _check_business_rules(self) -> EcommerceEvent:
        event_type = self.event_type

        if event_type in PRODUCT_EVENT_TYPES and self.product_id is None:
            raise ValueError(f"product_id is required for {event_type}")
        if event_type not in PRODUCT_EVENT_TYPES and self.product_id is not None:
            raise ValueError(f"product_id is not allowed for {event_type}")

        if event_type in ORDER_EVENT_TYPES and self.order_id is None:
            raise ValueError(f"order_id is required for {event_type}")
        if event_type not in ORDER_EVENT_TYPES and self.order_id is not None:
            raise ValueError(f"order_id is not allowed for {event_type}")

        if (self.amount is None) != (self.currency is None):
            raise ValueError("amount and currency must be provided together")
        if event_type in AMOUNT_REQUIRED_EVENT_TYPES and self.amount is None:
            raise ValueError(f"amount and currency are required for {event_type}")
        if event_type not in AMOUNT_ALLOWED_EVENT_TYPES and self.amount is not None:
            raise ValueError(f"amount is not allowed for {event_type}")

        if self.event_timestamp > self.produced_at + MAX_PRODUCER_CLOCK_SKEW:
            raise ValueError("event_timestamp is later than produced_at beyond the allowed skew")
        return self

    def to_json_dict(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude_none=True)


EVENT_FIELD_NAMES = frozenset(EcommerceEvent.model_fields)
