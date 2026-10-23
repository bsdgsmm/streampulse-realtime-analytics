"""Reference implementation of the funnel conversion metrics.

These functions define the metrics precisely and are used in tests to check
that the SQL in ``sql/analytics.sql`` computes the same numbers.

Definitions (all keyed on event time, deduplicated by ``event_id``):

* **Session view-to-cart rate** = sessions with at least one
  ``product_added_to_cart`` and at least one ``product_viewed`` / sessions with
  at least one ``product_viewed``.
* **Session checkout-to-order rate** = sessions with at least one
  ``order_placed`` and at least one ``checkout_started`` / sessions with at
  least one ``checkout_started``.
* **Order payment completion rate** = orders (by ``order_id``) with a
  ``payment_completed`` event / orders with an ``order_placed`` event.

A rate is ``None`` when its denominator is zero.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from streampulse.events.schemas import EcommerceEvent, EventType


@dataclass(frozen=True)
class FunnelMetrics:
    sessions_with_view: int
    sessions_with_view_and_cart: int
    sessions_with_checkout: int
    sessions_with_checkout_and_order: int
    orders_placed: int
    orders_paid: int

    @staticmethod
    def _rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    @property
    def view_to_cart_rate(self) -> float | None:
        return self._rate(self.sessions_with_view_and_cart, self.sessions_with_view)

    @property
    def checkout_to_order_rate(self) -> float | None:
        return self._rate(self.sessions_with_checkout_and_order, self.sessions_with_checkout)

    @property
    def payment_completion_rate(self) -> float | None:
        return self._rate(self.orders_paid, self.orders_placed)


def deduplicate(events: Iterable[EcommerceEvent]) -> list[EcommerceEvent]:
    unique: dict[str, EcommerceEvent] = {}
    for event in events:
        unique.setdefault(str(event.event_id), event)
    return list(unique.values())


def compute_funnel(events: Iterable[EcommerceEvent]) -> FunnelMetrics:
    session_types: dict[str, set[EventType]] = defaultdict(set)
    order_types: dict[str, set[EventType]] = defaultdict(set)
    for event in deduplicate(events):
        session_types[event.session_id].add(event.event_type)
        if event.order_id is not None:
            order_types[event.order_id].add(event.event_type)

    viewed = [types for types in session_types.values() if EventType.PRODUCT_VIEWED in types]
    checkout = [types for types in session_types.values() if EventType.CHECKOUT_STARTED in types]
    placed = [types for types in order_types.values() if EventType.ORDER_PLACED in types]
    return FunnelMetrics(
        sessions_with_view=len(viewed),
        sessions_with_view_and_cart=sum(
            EventType.PRODUCT_ADDED_TO_CART in types for types in viewed
        ),
        sessions_with_checkout=len(checkout),
        sessions_with_checkout_and_order=sum(EventType.ORDER_PLACED in types for types in checkout),
        orders_placed=len(placed),
        orders_paid=sum(EventType.PAYMENT_COMPLETED in types for types in placed),
    )
