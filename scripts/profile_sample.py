"""Profile the deterministic synthetic stream without any infrastructure.

Generates messages with the given seed, runs them through the same
EventProcessor the consumer uses (validation, deduplication, watermarks) and
the Python funnel reference, then renders a PNG. Partitions are assigned with
CRC32(session_id) % 3, which mirrors key-based partitioning but is not
identical to the broker's murmur2 partitioner.

Usage:
    python scripts/profile_sample.py --count 6000 --output docs/images/sample_data_profile.png
"""

from __future__ import annotations

import argparse
import logging
import sys
import zlib
from collections import Counter
from datetime import timedelta
from pathlib import Path

from streampulse.analytics.conversion import compute_funnel
from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.processor import EventProcessor, RawMessage
from streampulse.events.generator import EventGenerator, GeneratorConfig
from streampulse.logging_config import configure_logging

logger = logging.getLogger("profile_sample")

INK = "#1f2933"
BLUE = "#2563eb"
AMBER = "#f59e0b"
GREEN = "#10b981"
RED = "#ef4444"


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Profile the synthetic event stream.")
    parser.add_argument("--count", type=int, default=6000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("docs/images/sample_data_profile.png"))
    args = parser.parse_args(argv)

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    messages = EventGenerator(GeneratorConfig(seed=args.seed)).take(args.count)
    send_times = iter([message.send_time for message in messages])
    current = {"t": messages[0].send_time}

    def clock():
        return current["t"]

    processor = EventProcessor(
        WatermarkTracker(timedelta(seconds=60)), RecentEventCache(100_000), clock=clock
    )
    events, rejections, duplicates = [], Counter(), 0
    for offset, message in enumerate(messages):
        current["t"] = next(send_times)
        raw = RawMessage(
            "profile",
            zlib.crc32(message.key.encode()) % 3,
            offset,
            message.key.encode(),
            message.encode(),
        )
        result = processor.process_batch([raw])
        processor.recent_events.remember(result.event_ids)
        events.extend(result.events)
        duplicates += result.duplicates
        rejections.update(item.reason.value for item in result.rejections)

    funnel = compute_funnel(item.event for item in events)
    late = [item for item in events if item.is_late]

    fig, axes = plt.subplots(2, 2, figsize=(12, 7.5), constrained_layout=True)
    fig.suptitle(
        f"Synthetic stream profile - seed {args.seed}, {args.count:,} messages "
        "(processed in-memory by the consumer logic, no broker or database)",
        fontsize=12,
        color=INK,
        fontweight="bold",
    )

    by_minute_event = Counter(
        item.event.event_timestamp.replace(second=0, microsecond=0) for item in events
    )
    by_minute_ingest = Counter(item.ingested_at.replace(second=0, microsecond=0) for item in events)
    minutes = sorted(set(by_minute_event) | set(by_minute_ingest))
    ax = axes[0][0]
    ax.plot(
        minutes,
        [by_minute_event[m] for m in minutes],
        color=BLUE,
        marker="o",
        markersize=3,
        label="by event time",
    )
    ax.plot(
        minutes,
        [by_minute_ingest[m] for m in minutes],
        color=AMBER,
        marker="o",
        markersize=3,
        label="by processing time",
    )
    ax.set_title("Events per minute (UTC, simulated clock)", color=INK)
    ax.legend(frameon=False)
    ax.tick_params(axis="x", rotation=30)

    ax = axes[0][1]
    stages = [
        ("Sessions with view", funnel.sessions_with_view),
        ("...and add to cart", funnel.sessions_with_view_and_cart),
        ("Sessions with checkout", funnel.sessions_with_checkout),
        ("...and order placed", funnel.sessions_with_checkout_and_order),
        ("Orders placed", funnel.orders_placed),
        ("...and paid", funnel.orders_paid),
    ][::-1]
    bars = ax.barh(
        [s for s, _ in stages],
        [v for _, v in stages],
        color=[BLUE, BLUE, AMBER, AMBER, GREEN, GREEN],
    )
    ax.bar_label(bars, padding=3, color=INK)
    ax.set_title(
        f"Funnel  view->cart {funnel.view_to_cart_rate:.1%}  "
        f"checkout->order {funnel.checkout_to_order_rate:.1%}  "
        f"paid {funnel.payment_completion_rate:.1%}",
        color=INK,
    )

    ax = axes[1][0]
    type_counts = Counter(item.event.event_type.value for item in events)
    late_counts = Counter(item.event.event_type.value for item in late)
    types = sorted(type_counts)
    on_time = [type_counts[t] - late_counts[t] for t in types]
    ax.bar(types, on_time, color=BLUE, label="on time")
    ax.bar(
        types,
        [late_counts[t] for t in types],
        bottom=on_time,
        color=RED,
        label="late (behind 60 s watermark)",
    )
    ax.set_title(f"Accepted events by type ({len(late)} late of {len(events):,})", color=INK)
    ax.legend(frameon=False)
    ax.tick_params(axis="x", rotation=20)

    ax = axes[1][1]
    labels = [*sorted(rejections), "duplicates dropped"]
    values = [rejections[r] for r in sorted(rejections)] + [duplicates]
    bars = ax.barh(labels, values, color=[RED] * len(rejections) + [AMBER])
    ax.bar_label(bars, padding=3, color=INK)
    ax.set_title("Rejected messages by reason and duplicates", color=INK)

    for row in axes:
        for axis in row:
            axis.spines[["top", "right"]].set_visible(False)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=120)
    logger.info(
        "Profile written",
        extra={
            "path": str(args.output),
            "accepted_events": len(events),
            "late_events": len(late),
            "duplicates": duplicates,
            "rejected": sum(rejections.values()),
            "view_to_cart_rate": funnel.view_to_cart_rate,
            "checkout_to_order_rate": funnel.checkout_to_order_rate,
            "payment_completion_rate": funnel.payment_completion_rate,
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
