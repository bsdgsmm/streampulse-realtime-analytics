"""Render a PNG summary of the pipeline from live ClickHouse query results.

The figure is built only from rows returned by the queries in
sql/analytics.sql; nothing is hard-coded. If the database is empty the script
exits with an error instead of drawing an empty chart.

Usage:
    python scripts/render_report.py --output docs/images/analytics_report.png
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from streampulse.analytics.queries import load_queries
from streampulse.config import Settings
from streampulse.logging_config import configure_logging
from streampulse.storage.clickhouse import ClickHouseStore

logger = logging.getLogger("render_report")

INK = "#1f2933"
MUTED = "#7b8794"
SERIES_A = "#2563eb"
SERIES_B = "#f59e0b"
SERIES_C = "#10b981"
SERIES_D = "#ef4444"


def main(argv: list[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    parser = argparse.ArgumentParser(description="Render the analytics report image.")
    parser.add_argument("--output", type=Path, default=Path("docs/images/analytics_report.png"))
    args = parser.parse_args(argv)

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    queries = load_queries(settings.sql_dir)
    store = ClickHouseStore.connect(settings.clickhouse)
    try:

        def run(name: str) -> list[dict]:
            return store.query(queries[name].sql)

        overview = run("pipeline_overview")[0]
        if not overview["unique_events"]:
            logger.error("No events stored yet; run the demo before rendering the report")
            return 1
        timeline = run("event_time_vs_ingestion_time")
        funnel = run("funnel_summary")[0]
        by_type = run("late_events_by_type")
        rejections = run("rejections_by_error_type")
        quality = run("duplicate_and_invalid_rates")[0]
    finally:
        store.close()

    plt.rcParams.update({"font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK})
    fig, axes = plt.subplots(2, 2, figsize=(12, 7.5), constrained_layout=True)
    fig.suptitle(
        "StreamPulse - results from a local demo run (synthetic data)",
        fontsize=13,
        color=INK,
        fontweight="bold",
    )

    ax = axes[0][0]
    minutes = [row["minute_utc"] for row in timeline]
    ax.plot(
        minutes,
        [row["by_event_time"] for row in timeline],
        color=SERIES_A,
        marker="o",
        markersize=3,
        label="by event time",
    )
    ax.plot(
        minutes,
        [row["by_ingestion_time"] for row in timeline],
        color=SERIES_B,
        marker="o",
        markersize=3,
        label="by ingestion time",
    )
    ax.set_title("Events per minute: event time vs ingestion time (UTC)", color=INK)
    ax.set_ylabel("events")
    ax.legend(frameon=False)
    ax.tick_params(axis="x", rotation=30)

    ax = axes[0][1]
    stages = [
        ("Sessions with view", funnel["sessions_with_view"]),
        ("...and add to cart", funnel["sessions_with_view_and_cart"]),
        ("Sessions with checkout", funnel["sessions_with_checkout"]),
        ("...and order placed", funnel["sessions_with_checkout_and_order"]),
        ("Orders placed", funnel["orders_placed"]),
        ("...and paid", funnel["orders_paid"]),
    ]
    labels = [label for label, _ in stages][::-1]
    values = [value for _, value in stages][::-1]
    colors = [SERIES_C, SERIES_C, SERIES_B, SERIES_B, SERIES_A, SERIES_A][::-1]
    bars = ax.barh(labels, values, color=colors)
    ax.bar_label(bars, padding=3, color=INK)
    ax.set_title(
        f"Funnel  view->cart {funnel['view_to_cart_rate']:.1%}  "
        f"checkout->order {funnel['checkout_to_order_rate']:.1%}  "
        f"paid {funnel['payment_completion_rate']:.1%}",
        color=INK,
    )

    ax = axes[1][0]
    types = [row["event_type"] for row in by_type]
    on_time = [row["event_count"] - row["late_events"] for row in by_type]
    late = [row["late_events"] for row in by_type]
    ax.bar(types, on_time, color=SERIES_A, label="on time")
    ax.bar(types, late, bottom=on_time, color=SERIES_D, label="late (behind watermark)")
    ax.set_title("Stored events by type and lateness", color=INK)
    ax.legend(frameon=False)
    ax.tick_params(axis="x", rotation=20)

    ax = axes[1][1]
    reasons = [row["error_type"] for row in rejections] or ["none"]
    counts = [row["rejected_messages"] for row in rejections] or [0]
    bars = ax.barh(reasons, counts, color=SERIES_D)
    ax.bar_label(bars, padding=3, color=INK)
    ax.set_title(
        f"Rejected messages by reason  (invalid rate {quality['invalid_rate']:.2%}, "
        f"duplicates dropped {quality['duplicates_dropped']})",
        color=INK,
    )

    for row in axes:
        for axis in row:
            axis.spines[["top", "right"]].set_visible(False)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=130)
    logger.info(
        "Report written",
        extra={
            "path": str(args.output),
            "unique_events": overview["unique_events"],
            "rejected_messages": overview["rejected_messages"],
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
