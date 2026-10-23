"""Producer -> Redpanda -> consumer -> ClickHouse, on throwaway topics and database."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import timedelta

import pytest

from streampulse.consumer.dead_letter import DeadLetterPublisher
from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.main import (
    EXIT_OK,
    RunOptions,
    StreamConsumer,
    build_dlq_producer,
    build_kafka_consumer,
)
from streampulse.consumer.processor import EventProcessor
from streampulse.events.generator import EventGenerator, GeneratorConfig, MessageKind
from streampulse.producer.main import EventPublisher, build_producer
from streampulse.retry import RetryPolicy

MESSAGE_COUNT = 1500


@pytest.fixture
def topics(settings):
    from confluent_kafka.admin import AdminClient, NewTopic

    suffix = uuid.uuid4().hex[:8]
    events_topic = f"streampulse.it.events.{suffix}"
    dlq_topic = f"streampulse.it.events.dlq.{suffix}"
    admin = AdminClient({"bootstrap.servers": settings.kafka.bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(events_topic, num_partitions=3), NewTopic(dlq_topic, num_partitions=1)]
    )
    for future in futures.values():
        future.result(timeout=30)
    yield events_topic, dlq_topic
    for future in admin.delete_topics([events_topic, dlq_topic]).values():
        future.result(timeout=30)


def run_consumer(settings, group: str) -> StreamConsumer:
    settings = replace(settings, kafka=replace(settings.kafka, consumer_group=group))
    from streampulse.storage.clickhouse import ClickHouseStore

    watermarks = WatermarkTracker(timedelta(seconds=60))
    store = ClickHouseStore.connect(settings.clickhouse, consumer_id=f"it-{group}")
    consumer = StreamConsumer(
        build_kafka_consumer(settings, watermarks),
        store,
        DeadLetterPublisher(build_dlq_producer(settings), settings.kafka.dlq_topic, group),
        EventProcessor(watermarks, RecentEventCache(100_000)),
        consumer_group=group,
        consumer_id=f"it-{group}",
        batch_size=200,
        batch_timeout_seconds=0.5,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.2),
    )
    try:
        assert consumer.run(RunOptions(idle_timeout_seconds=15)) == EXIT_OK
    finally:
        store.close()
    return consumer


def high_watermark_total(settings, topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer(
        {"bootstrap.servers": settings.kafka.bootstrap_servers, "group.id": "it-probe"}
    )
    try:
        metadata = probe.list_topics(topic, timeout=10)
        return sum(
            probe.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)[1]
            for partition in metadata.topics[topic].partitions
        )
    finally:
        probe.close()


def test_pipeline_end_to_end(isolated_db, topics) -> None:
    settings, store = isolated_db
    events_topic, dlq_topic = topics
    settings = replace(
        settings, kafka=replace(settings.kafka, events_topic=events_topic, dlq_topic=dlq_topic)
    )

    messages = EventGenerator(GeneratorConfig(seed=2024, duplicate_rate=0.05)).take(MESSAGE_COUNT)
    publisher = EventPublisher(build_producer(settings), events_topic)
    for message in messages:
        publisher.publish(message)
    assert publisher.flush(30) == 0
    assert publisher.metrics.delivered == MESSAGE_COUNT

    unique_valid = {
        str(message.event.event_id)
        for message in messages
        if message.kind in (MessageKind.VALID, MessageKind.DUPLICATE)
    }
    invalid = sum(message.kind is MessageKind.INVALID for message in messages)

    group = f"it-group-{uuid.uuid4().hex[:8]}"
    first = run_consumer(settings, group)
    assert first.metrics.messages_consumed == MESSAGE_COUNT
    assert first.metrics.invalid_events == invalid
    assert first.metrics.events_written == len(unique_valid)

    def scalar(sql: str):
        return next(iter(store.query(sql)[0].values()))

    assert scalar("SELECT count() FROM events FINAL") == len(unique_valid)
    assert scalar("SELECT count() FROM event_rejections FINAL") == invalid
    assert high_watermark_total(settings, dlq_topic) == invalid
    latest_lag = (
        "SELECT sum(lag) FROM (SELECT argMax(lag, observed_at) AS lag "
        "FROM consumer_lag GROUP BY topic, partition)"
    )
    assert scalar(latest_lag) == 0

    restarted = run_consumer(settings, group)
    assert restarted.metrics.messages_consumed == 0, "committed offsets must survive a restart"

    replay = run_consumer(settings, f"{group}-replay")
    assert replay.metrics.messages_consumed == MESSAGE_COUNT
    assert scalar("SELECT count() FROM events") >= len(unique_valid)
    assert scalar("SELECT count() FROM events FINAL") == len(unique_valid)
    assert scalar("SELECT count() FROM event_rejections FINAL") == invalid
