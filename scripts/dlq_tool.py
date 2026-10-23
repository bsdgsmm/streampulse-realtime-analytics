"""Inspect or replay messages from the dead-letter topic.

Inspection reads the DLQ with a throwaway consumer group (no offsets are
committed), summarises failures by error type and shows samples.

Replay republishes the original key and value of selected DLQ messages to the
events topic. Use it after fixing the cause (for example after deploying a
consumer that supports a new schema version). Replayed messages that still
violate the contract are rejected again and land back in the DLQ.

Usage:
    python scripts/dlq_tool.py inspect [--samples 5]
    python scripts/dlq_tool.py replay --error-type unsupported_schema_version [--execute]
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from collections import Counter
from typing import Any

from streampulse.config import Settings
from streampulse.consumer.dead_letter import parse_headers
from streampulse.logging_config import configure_logging

logger = logging.getLogger("dlq_tool")


def read_dlq(settings: Settings, max_messages: int, idle_seconds: float) -> list[Any]:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": f"streampulse-dlq-inspect-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.kafka.dlq_topic])
    collected: list[Any] = []
    empty_polls = 0
    try:
        while len(collected) < max_messages and empty_polls * 1.0 < idle_seconds:
            batch = consumer.consume(num_messages=500, timeout=1.0)
            if not batch:
                empty_polls += 1
                continue
            empty_polls = 0
            for message in batch:
                if message.error() is None:
                    collected.append(message)
    finally:
        consumer.close()
    return collected[:max_messages]


def main(argv: list[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    parser = argparse.ArgumentParser(description="Inspect or replay dead-lettered messages.")
    sub = parser.add_subparsers(dest="command", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--samples", type=int, default=3)
    replay = sub.add_parser("replay")
    replay.add_argument("--error-type", required=True)
    replay.add_argument("--execute", action="store_true", help="publish (default is a dry run)")
    for command in (inspect, replay):
        command.add_argument("--max-messages", type=int, default=10_000)
        command.add_argument("--idle-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)

    messages = read_dlq(settings, args.max_messages, args.idle_seconds)
    parsed = [(message, parse_headers(message.headers())) for message in messages]
    by_type = Counter(headers.get("error_type", "unknown") for _, headers in parsed)
    sys.stdout.write(f"DLQ topic {settings.kafka.dlq_topic}: {len(messages)} messages\n")
    for error_type, count in by_type.most_common():
        sys.stdout.write(f"  {error_type}: {count}\n")

    if args.command == "inspect":
        for error_type in by_type:
            samples = [item for item in parsed if item[1].get("error_type") == error_type]
            sys.stdout.write(f"\n-- {error_type} --\n")
            for message, headers in samples[: args.samples]:
                value = (message.value() or b"")[:300].decode("utf-8", errors="replace")
                sys.stdout.write(
                    f"source={headers.get('source_topic')}[{headers.get('source_partition')}]"
                    f"@{headers.get('source_offset')} error={headers.get('error_message')}\n"
                    f"  payload={value}\n"
                )
        return 0

    selected = [
        message for message, headers in parsed if headers.get("error_type") == args.error_type
    ]
    if not args.execute:
        sys.stdout.write(
            f"\nDry run: {len(selected)} messages would be replayed to "
            f"{settings.kafka.events_topic}. Re-run with --execute to publish.\n"
        )
        return 0

    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    for message in selected:
        producer.produce(
            settings.kafka.events_topic,
            key=message.key(),
            value=message.value(),
            headers=[("replayed_from", settings.kafka.dlq_topic.encode())],
        )
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining:
        logger.error("Replay incomplete", extra={"undelivered": remaining})
        return 1
    logger.info("Replay complete", extra={"replayed": len(selected), "error_type": args.error_type})
    return 0


if __name__ == "__main__":
    sys.exit(main())
