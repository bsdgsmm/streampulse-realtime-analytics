"""Create the database and apply ``sql/schema.sql`` idempotently."""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

from streampulse.config import ClickHouseSettings, Settings
from streampulse.logging_config import configure_logging
from streampulse.retry import RetriesExhaustedError, RetryPolicy, retry_call
from streampulse.storage.clickhouse import (
    ClickHouseStore,
    PermanentStorageError,
    TransientStorageError,
)

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def split_statements(sql_text: str) -> list[str]:
    """Split a SQL script on semicolons after removing ``--`` comment lines."""
    lines = [line for line in sql_text.splitlines() if not line.lstrip().startswith("--")]
    statements = [chunk.strip() for chunk in "\n".join(lines).split(";")]
    return [statement for statement in statements if statement]


def apply_schema(
    settings: ClickHouseSettings, schema_path: Path, database: str | None = None
) -> int:
    database = database or settings.database
    if not _IDENTIFIER.match(database):
        raise PermanentStorageError(f"invalid database name: {database!r}")

    admin = ClickHouseStore.connect(settings, database="default")
    try:
        admin.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    finally:
        admin.close()

    statements = split_statements(schema_path.read_text())
    store = ClickHouseStore.connect(settings, database=database)
    try:
        for statement in statements:
            store.command(statement)
    finally:
        store.close()
    logger.info("Schema applied", extra={"database": database, "statements": len(statements)})
    return len(statements)


def apply_schema_with_retry(
    settings: Settings, policy: RetryPolicy | None = None, database: str | None = None
) -> int:
    return retry_call(
        lambda: apply_schema(settings.clickhouse, settings.sql_dir / "schema.sql", database),
        policy or RetryPolicy(max_attempts=10, base_delay_seconds=1.0, max_delay_seconds=5.0),
        operation="apply_schema",
        retry_on=(TransientStorageError,),
    )


def main() -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    try:
        apply_schema_with_retry(settings)
    except (RetriesExhaustedError, PermanentStorageError) as exc:
        logger.error("Schema migration failed", extra={"error": str(exc)})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
