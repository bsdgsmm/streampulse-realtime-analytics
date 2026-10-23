from __future__ import annotations

import json
import logging
from datetime import timedelta

import pytest

from streampulse.config import ConfigError, Settings
from streampulse.consumer.dead_letter import (
    DeadLetterError,
    DeadLetterPublisher,
    build_headers,
    parse_headers,
)
from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.processor import EventProcessor
from streampulse.logging_config import JsonFormatter
from streampulse.monitoring.lag import PartitionLag, total_lag
from streampulse.monitoring.metrics import ConsumerMetrics
from tests.helpers import raw_message


def test_defaults_target_local_host_ports() -> None:
    settings = Settings.from_env({})
    assert settings.kafka.bootstrap_servers == "localhost:19092"
    assert settings.clickhouse.port == 8123
    assert settings.consumer.batch_size == 500


def test_environment_overrides() -> None:
    settings = Settings.from_env(
        {
            "STREAMPULSE_BOOTSTRAP_SERVERS": "redpanda:9092",
            "CLICKHOUSE_HOST": "clickhouse",
            "STREAMPULSE_BATCH_SIZE": "50",
            "CLICKHOUSE_SECURE": "true",
            "STREAMPULSE_LOG_FORMAT": "JSON",
        }
    )
    assert settings.kafka.bootstrap_servers == "redpanda:9092"
    assert settings.clickhouse.host == "clickhouse"
    assert settings.consumer.batch_size == 50
    assert settings.clickhouse.secure is True
    assert settings.log_format == "json"


@pytest.mark.parametrize(
    "env",
    [
        {"STREAMPULSE_BATCH_SIZE": "lots"},
        {"STREAMPULSE_BATCH_SIZE": "0"},
        {"CLICKHOUSE_SECURE": "maybe"},
        {"STREAMPULSE_LOG_FORMAT": "xml"},
        {"STREAMPULSE_BATCH_TIMEOUT_SECONDS": "-1"},
    ],
)
def test_invalid_values_fail_fast(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError):
        Settings.from_env(env)


def test_password_is_not_exposed_in_repr() -> None:
    settings = Settings.from_env({"CLICKHOUSE_PASSWORD": "s3cr3t-value"})
    assert settings.clickhouse.password == "s3cr3t-value"
    assert "s3cr3t-value" not in repr(settings)


def test_lag_uses_committed_offset() -> None:
    lag = PartitionLag("t", 0, committed_offset=40, high_watermark=100, low_watermark=0)
    assert lag.lag == 60


def test_lag_without_commit_counts_retained_range() -> None:
    lag = PartitionLag("t", 0, committed_offset=-1001, high_watermark=100, low_watermark=20)
    assert lag.lag == 80


def test_total_lag() -> None:
    parts = [
        PartitionLag("t", 0, 10, 15, 0),
        PartitionLag("t", 1, 7, 7, 0),
    ]
    assert total_lag(parts) == 5


def test_consumer_metrics_snapshot_includes_reasons() -> None:
    metrics = ConsumerMetrics(events_written=3)
    metrics.rejections_by_reason["invalid_json"] += 2
    snapshot = metrics.snapshot()
    assert snapshot["events_written"] == 3
    assert snapshot["rejected_invalid_json"] == 2


def rejection():
    processor = EventProcessor(WatermarkTracker(timedelta(0)), RecentEventCache(1))
    return processor.process_batch([raw_message(b"{bad", partition=2, offset=17)]).rejections[0]


def test_dlq_headers_round_trip() -> None:
    headers = parse_headers(build_headers(rejection(), "group-a"))
    assert headers["error_type"] == "invalid_json"
    assert headers["source_partition"] == "2"
    assert headers["source_offset"] == "17"
    assert headers["consumer_group"] == "group-a"


class DlqProducer:
    def __init__(self, error: str | None = None, remaining: int = 0) -> None:
        self.sent: list[dict] = []
        self.error = error
        self.remaining = remaining

    def produce(self, topic: str, **kwargs) -> None:
        self.sent.append({"topic": topic, **kwargs})
        kwargs["on_delivery"](self.error, None)

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float = 0) -> int:
        return self.remaining


def test_dlq_forwards_original_payload() -> None:
    producer = DlqProducer()
    DeadLetterPublisher(producer, "dlq", "g").publish([rejection()])
    assert producer.sent[0]["value"] == b"{bad"
    assert producer.sent[0]["topic"] == "dlq"


@pytest.mark.parametrize("producer", [DlqProducer(error="timeout"), DlqProducer(remaining=1)])
def test_dlq_failures_raise(producer: DlqProducer) -> None:
    with pytest.raises(DeadLetterError):
        DeadLetterPublisher(producer, "dlq", "g").publish([rejection()])


def test_json_log_formatter_includes_extra_fields() -> None:
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", None, None)
    record.batch_size = 10
    data = json.loads(JsonFormatter().format(record))
    assert data["message"] == "hello" and data["batch_size"] == 10
