"""The contract between Python rows, sql/schema.sql and sql/analytics.sql."""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from streampulse.analytics.queries import load_queries, parse_queries
from streampulse.config import DEFAULT_SQL_DIR
from streampulse.consumer.deduplication import RecentEventCache
from streampulse.consumer.lateness import WatermarkTracker
from streampulse.consumer.processor import EventProcessor
from streampulse.events.schemas import EventType
from streampulse.storage.clickhouse import (
    BATCH_COLUMNS,
    EVENT_COLUMNS,
    LAG_COLUMNS,
    REJECTION_COLUMNS,
    ClickHouseStore,
    PermanentStorageError,
    TransientStorageError,
    classify_exception,
    deduplication_token,
    event_to_row,
    rejection_to_row,
)
from streampulse.storage.migrate import split_statements
from tests.helpers import BASE_TIME, event_payload, raw_message

SCHEMA = (DEFAULT_SQL_DIR / "schema.sql").read_text()


def table_columns(table: str) -> list[str]:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS {table}\s*\((.*?)\n\)\s*\nENGINE", SCHEMA, re.DOTALL
    )
    assert match, f"table {table} not found in schema.sql"
    return [line.split()[0] for line in match.group(1).strip().splitlines() if line.strip()]


@pytest.mark.parametrize(
    ("table", "columns"),
    [
        ("events", EVENT_COLUMNS),
        ("event_rejections", REJECTION_COLUMNS),
        ("consumer_batches", BATCH_COLUMNS),
        ("consumer_lag", LAG_COLUMNS),
    ],
)
def test_python_columns_match_schema(table: str, columns: tuple[str, ...]) -> None:
    assert table_columns(table) == list(columns)


def processed(**overrides: Any):
    processor = EventProcessor(
        WatermarkTracker(timedelta(seconds=60)),
        RecentEventCache(10),
        clock=lambda: BASE_TIME + timedelta(seconds=5),
    )
    return processor.process_batch([raw_message(event_payload(**overrides), offset=42)])


def test_event_row_aligns_with_columns() -> None:
    item = processed(experiment_bucket="b").events[0]
    row = dict(zip(EVENT_COLUMNS, event_to_row(item, "c1"), strict=True))
    assert row["event_type"] == "product_viewed"
    assert row["kafka_offset"] == 42
    assert row["unexpected_fields"] == ["experiment_bucket"]
    assert row["is_synthetic"] is True and row["is_late"] is False
    assert row["ingestion_delay_ms"] == 5000
    assert row["consumer_id"] == "c1"


def test_rejection_row_truncates_payload() -> None:
    processor = EventProcessor(WatermarkTracker(timedelta(0)), RecentEventCache(10))
    rejection = processor.process_batch([raw_message(b"x" * 10_000)]).rejections[0]
    row = dict(zip(REJECTION_COLUMNS, rejection_to_row(rejection, "c1"), strict=True))
    assert len(row["raw_payload"]) == 4096
    assert row["payload_bytes"] == 10_000
    assert row["error_type"] == "invalid_json"


def test_deduplication_token_is_order_independent() -> None:
    assert deduplication_token("events", ["b", "a"]) == deduplication_token("events", ["a", "b"])
    assert deduplication_token("events", ["a"]) != deduplication_token("events", ["a", "b"])


def test_schema_statements_split_cleanly() -> None:
    statements = split_statements(SCHEMA)
    assert len(statements) == 4
    assert all(statement.startswith("CREATE TABLE IF NOT EXISTS") for statement in statements)


class RecordingClient:
    def __init__(self, error: Exception | None = None) -> None:
        self.inserts: list[tuple[str, list, dict]] = []
        self.error = error

    def insert(self, table: str, data, **kwargs):
        if self.error:
            raise self.error
        self.inserts.append((table, list(data), kwargs))

    def query(self, query: str, **kwargs):
        class Result:
            column_names = ("a", "b")
            result_rows = ((1, "x"), (2, "y"))

        return Result()

    def command(self, cmd: str, **kwargs):
        return 1

    def close(self) -> None:
        pass


def test_store_passes_dedup_token_and_column_names() -> None:
    client = RecordingClient()
    store = ClickHouseStore(client, consumer_id="c1")
    store.insert_events(processed().events, "events:abc")
    table, rows, kwargs = client.inserts[0]
    assert table == "events"
    assert kwargs["column_names"] == list(EVENT_COLUMNS)
    assert kwargs["settings"] == {"insert_deduplication_token": "events:abc"}
    assert len(rows) == 1


def test_store_skips_empty_inserts() -> None:
    client = RecordingClient()
    store = ClickHouseStore(client)
    assert store.insert_events([], "t") == 0
    assert store.insert_rejections([], "t") == 0
    store.insert_lag([])
    assert client.inserts == []


def test_store_query_returns_dicts() -> None:
    assert ClickHouseStore(RecordingClient()).query("SELECT 1") == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
    ]


def test_connection_errors_become_transient() -> None:
    store = ClickHouseStore(RecordingClient(error=ConnectionRefusedError("refused")))
    with pytest.raises(TransientStorageError):
        store.insert_events(processed().events, "t")


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (TimeoutError("slow"), TransientStorageError),
        (OSError("reset"), TransientStorageError),
        (ValueError("bad"), PermanentStorageError),
    ],
)
def test_exception_classification(exc: Exception, expected: type) -> None:
    assert isinstance(classify_exception(exc), expected)


EXPECTED_QUERIES = {
    "pipeline_overview",
    "events_per_minute",
    "event_time_vs_ingestion_time",
    "daily_event_counts_by_type",
    "product_engagement_trend",
    "funnel_summary",
    "view_to_cart_conversion_daily",
    "checkout_to_order_conversion_daily",
    "top_products_view_to_cart",
    "order_payment_trend",
    "payment_amount_by_currency",
    "late_event_rate",
    "late_events_by_type",
    "duplicate_and_invalid_rates",
    "storage_duplicate_check",
    "rejections_by_error_type",
    "consumer_lag_latest",
    "consumer_batch_health",
    "session_event_sequences",
    "top_session_paths",
}


def test_analytics_catalogue_is_complete() -> None:
    queries = load_queries(DEFAULT_SQL_DIR)
    assert set(queries) == EXPECTED_QUERIES
    for query in queries.values():
        assert query.description, f"{query.name} needs a description"
        assert ";" not in query.sql


def test_analytics_reference_only_known_tables_and_event_types() -> None:
    known_tables = {"events", "event_rejections", "consumer_batches", "consumer_lag"}
    known_types = {event_type.value for event_type in EventType}
    for query in load_queries(DEFAULT_SQL_DIR).values():
        tables = set(re.findall(r"\bFROM\s+([a-z_]+)", query.sql))
        assert tables <= known_tables, (query.name, tables - known_tables)
        literals = re.findall(r"event_type\s*=\s*'([a-z_]+)'", query.sql)
        literals += re.findall(r"'([a-z_]+)'", "".join(re.findall(r"IN \(([^)]*)\)", query.sql)))
        assert set(literals) <= known_types, (query.name, set(literals) - known_types)


def test_query_parser_rejects_duplicates_and_empty_bodies() -> None:
    with pytest.raises(ValueError):
        parse_queries("-- name: a\nSELECT 1;\n-- name: a\nSELECT 2;")
    with pytest.raises(ValueError):
        parse_queries("-- name: empty\n-- nothing here\n")


def test_sql_dir_exists() -> None:
    assert Path(DEFAULT_SQL_DIR, "analytics.sql").is_file()
