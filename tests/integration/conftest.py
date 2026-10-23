"""Integration fixtures. They need the Docker Compose stack (`make up`) and run only
when STREAMPULSE_RUN_INTEGRATION=1, using an isolated database and topics per test."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import replace

import pytest

from streampulse.config import Settings
from streampulse.storage.clickhouse import ClickHouseStore
from streampulse.storage.migrate import apply_schema

RUN_INTEGRATION = os.environ.get("STREAMPULSE_RUN_INTEGRATION") == "1"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip = pytest.mark.skip(reason="set STREAMPULSE_RUN_INTEGRATION=1 and run `make up`")
    for item in items:
        if "tests/integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            if not RUN_INTEGRATION:
                item.add_marker(skip)


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture
def isolated_db(settings: Settings) -> Iterator[tuple[Settings, ClickHouseStore]]:
    database = f"streampulse_it_{uuid.uuid4().hex[:10]}"
    apply_schema(settings.clickhouse, settings.sql_dir / "schema.sql", database=database)
    scoped = replace(settings, clickhouse=replace(settings.clickhouse, database=database))
    store = ClickHouseStore.connect(scoped.clickhouse, consumer_id="integration-test")
    try:
        yield scoped, store
    finally:
        store.command(f"DROP DATABASE IF EXISTS {database}")
        store.close()
