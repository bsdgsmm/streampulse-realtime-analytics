from __future__ import annotations

from datetime import timedelta
from itertools import count

from streampulse.analytics.conversion import FunnelMetrics, compute_funnel
from streampulse.events.schemas import EcommerceEvent
from tests.helpers import BASE_TIME, event_payload

_ids = count(1)


def ev(event_type: str, session: str, **extra) -> EcommerceEvent:
    n = next(_ids)
    ts = BASE_TIME + timedelta(seconds=n)
    fields = {
        "event_id": f"00000000-0000-4000-8000-{n:012d}",
        "event_type": event_type,
        "session_id": f"s_{session:0>16}",
        "event_timestamp": ts.isoformat(),
        "produced_at": (ts + timedelta(seconds=1)).isoformat(),
        "product_id": None,
    }
    if event_type in {"product_viewed", "product_added_to_cart"}:
        fields["product_id"] = "p_0001"
    if event_type in {"order_placed", "payment_completed", "order_cancelled"}:
        fields["order_id"] = f"o_{extra.pop('order'):0>16}"
    if event_type in {"order_placed", "payment_completed"}:
        fields.update(amount="10.00", currency="EUR")
    return EcommerceEvent.model_validate(event_payload(**fields))


def test_funnel_counts_sessions_and_orders() -> None:
    events = [
        ev("product_viewed", "a"),
        ev("product_added_to_cart", "a"),
        ev("checkout_started", "a"),
        ev("order_placed", "a", order="1"),
        ev("payment_completed", "a", order="1"),
        ev("product_viewed", "b"),
        ev("product_added_to_cart", "b"),
        ev("checkout_started", "b"),
        ev("product_viewed", "c"),
        ev("product_viewed", "d"),
        ev("product_added_to_cart", "d"),
        ev("checkout_started", "d"),
        ev("order_placed", "d", order="2"),
        ev("order_cancelled", "d", order="2"),
    ]
    funnel = compute_funnel(events)
    assert funnel == FunnelMetrics(
        sessions_with_view=4,
        sessions_with_view_and_cart=3,
        sessions_with_checkout=3,
        sessions_with_checkout_and_order=2,
        orders_placed=2,
        orders_paid=1,
    )
    assert funnel.view_to_cart_rate == 0.75
    assert funnel.checkout_to_order_rate == 0.6667
    assert funnel.payment_completion_rate == 0.5


def test_duplicates_do_not_inflate_conversion() -> None:
    view = ev("product_viewed", "a")
    cart = ev("product_added_to_cart", "a")
    once = compute_funnel([view, cart])
    twice = compute_funnel([view, view, cart, cart])
    assert once == twice


def test_cart_without_view_is_not_counted_as_conversion() -> None:
    funnel = compute_funnel([ev("product_added_to_cart", "e")])
    assert funnel.sessions_with_view == 0
    assert funnel.sessions_with_view_and_cart == 0
    assert funnel.view_to_cart_rate is None


def test_payment_without_order_placed_is_ignored() -> None:
    funnel = compute_funnel([ev("payment_completed", "f", order="9")])
    assert funnel.orders_placed == 0 and funnel.payment_completion_rate is None


def test_empty_input_has_undefined_rates() -> None:
    funnel = compute_funnel([])
    assert funnel.view_to_cart_rate is None
    assert funnel.checkout_to_order_rate is None
