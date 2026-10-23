from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from streampulse.events.schemas import EVENT_FIELD_NAMES, EcommerceEvent, EventType
from tests.helpers import event_payload


def test_valid_product_view_is_accepted() -> None:
    event = EcommerceEvent.model_validate(event_payload())
    assert event.event_type is EventType.PRODUCT_VIEWED
    assert event.is_synthetic is True
    assert event.amount is None and event.order_id is None


def test_fixture_order_event_parses_amount_as_decimal(order_placed_payload: dict) -> None:
    event = EcommerceEvent.model_validate(order_placed_payload)
    assert event.amount == Decimal("149.90")
    assert event.currency == "EUR"


def test_timestamps_are_normalized_to_utc() -> None:
    event = EcommerceEvent.model_validate(
        event_payload(
            event_timestamp="2026-01-05T12:00:00+03:00",
            produced_at="2026-01-05T12:00:01+03:00",
        )
    )
    assert event.event_timestamp == datetime(2026, 1, 5, 9, 0, tzinfo=UTC)
    assert event.event_timestamp.utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    "missing",
    [
        "event_id",
        "event_type",
        "event_timestamp",
        "produced_at",
        "user_id",
        "session_id",
        "schema_version",
        "source",
        "is_synthetic",
    ],
)
def test_required_fields(missing: str) -> None:
    payload = event_payload()
    payload.pop(missing)
    with pytest.raises(ValidationError) as info:
        EcommerceEvent.model_validate(payload)
    assert missing in str(info.value)


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError):
        EcommerceEvent.model_validate(event_payload(event_timestamp="2026-01-05T09:00:00"))


def test_product_id_required_for_product_events() -> None:
    with pytest.raises(ValidationError, match="product_id is required"):
        EcommerceEvent.model_validate(event_payload(product_id=None))


def test_product_id_not_allowed_on_checkout() -> None:
    with pytest.raises(ValidationError, match="product_id is not allowed"):
        EcommerceEvent.model_validate(event_payload(event_type="checkout_started"))


def test_order_events_require_order_id(order_placed_payload: dict) -> None:
    order_placed_payload.pop("order_id")
    with pytest.raises(ValidationError, match="order_id is required"):
        EcommerceEvent.model_validate(order_placed_payload)


def test_payment_requires_amount_and_currency(order_placed_payload: dict) -> None:
    order_placed_payload["event_type"] = "payment_completed"
    order_placed_payload.pop("amount")
    order_placed_payload.pop("currency")
    with pytest.raises(ValidationError, match="required for payment_completed"):
        EcommerceEvent.model_validate(order_placed_payload)


def test_amount_without_currency_is_rejected(order_placed_payload: dict) -> None:
    order_placed_payload.pop("currency")
    with pytest.raises(ValidationError, match="provided together"):
        EcommerceEvent.model_validate(order_placed_payload)


def test_cancellation_amount_is_optional(order_placed_payload: dict) -> None:
    order_placed_payload.update(event_type="order_cancelled")
    order_placed_payload.pop("amount")
    order_placed_payload.pop("currency")
    event = EcommerceEvent.model_validate(order_placed_payload)
    assert event.amount is None


@pytest.mark.parametrize("amount", ["0", "-1.00", "12.345", "100000.01"])
def test_amount_bounds_and_precision(order_placed_payload: dict, amount: str) -> None:
    order_placed_payload["amount"] = amount
    with pytest.raises(ValidationError):
        EcommerceEvent.model_validate(order_placed_payload)


def test_unsupported_currency(order_placed_payload: dict) -> None:
    order_placed_payload["currency"] = "JPY"
    with pytest.raises(ValidationError):
        EcommerceEvent.model_validate(order_placed_payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [("user_id", "user-1"), ("session_id", "s_XYZ"), ("product_id", "p_12"), ("user_id", 123456)],
)
def test_identifier_formats(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        EcommerceEvent.model_validate(event_payload(**{field: value}))


def test_is_synthetic_must_be_a_real_boolean() -> None:
    with pytest.raises(ValidationError):
        EcommerceEvent.model_validate(event_payload(is_synthetic="yes"))


def test_event_time_after_production_time_is_rejected() -> None:
    with pytest.raises(ValidationError, match="later than produced_at"):
        EcommerceEvent.model_validate(
            event_payload(
                event_timestamp="2026-01-05T09:01:00Z", produced_at="2026-01-05T09:00:00Z"
            )
        )


def test_small_clock_skew_is_tolerated() -> None:
    event = EcommerceEvent.model_validate(
        event_payload(event_timestamp="2026-01-05T09:00:03Z", produced_at="2026-01-05T09:00:00Z")
    )
    assert event.event_timestamp > event.produced_at


def test_unexpected_fields_are_ignored_not_rejected() -> None:
    event = EcommerceEvent.model_validate(event_payload(experiment_bucket="variant_a"))
    assert "experiment_bucket" not in event.model_dump()
    assert "experiment_bucket" not in EVENT_FIELD_NAMES


def test_round_trip_through_json() -> None:
    event = EcommerceEvent.model_validate(event_payload())
    assert EcommerceEvent.model_validate(event.to_json_dict()) == event
