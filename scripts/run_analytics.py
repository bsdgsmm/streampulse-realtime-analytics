"""Run the named queries from sql/analytics.sql and write the results as text tables.

Usage:
    python scripts/run_analytics.py
    python scripts/run_analytics.py funnel_summary
    python scripts/run_analytics.py --list
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from typing import Any

from streampulse.analytics.queries import load_queries
from streampulse.config import Settings
from streampulse.logging_config import configure_logging
from streampulse.storage.clickhouse import (
    ClickHouseStore,
    PermanentStorageError,
    TransientStorageError,
)

logger = logging.getLogger("run_analytics")


def format_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".") if value == value else "nan"
    return str(value)


def render_table(rows: Sequence[dict[str, Any]], max_width: int = 80) -> str:
    if not rows:
        return "(no rows)\n"
    columns = list(rows[0].keys())
    cells = [[format_value(row[column])[:max_width] for column in columns] for row in rows]
    widths = [
        max(len(column), *(len(line[index]) for line in cells))
        for index, column in enumerate(columns)
    ]
    header = " | ".join(column.ljust(width) for column, width in zip(columns, widths, strict=True))
    divider = "-+-".join("-" * width for width in widths)
    body = [
        " | ".join(value.ljust(width) for value, width in zip(line, widths, strict=True))
        for line in cells
    ]
    return "\n".join([header, divider, *body]) + "\n"


def main(argv: list[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("names", nargs="*", help="query names to run (default: all)")
    parser.add_argument("--list", action="store_true", help="list available queries")
    args = parser.parse_args(argv)

    queries = load_queries(settings.sql_dir)
    if args.list:
        for query in queries.values():
            sys.stdout.write(f"{query.name}: {query.description}\n")
        return 0

    unknown = [name for name in args.names if name not in queries]
    if unknown:
        logger.error("Unknown query names", extra={"names": unknown})
        return 2

    try:
        store = ClickHouseStore.connect(settings.clickhouse)
    except (TransientStorageError, PermanentStorageError) as exc:
        logger.error("Could not connect to ClickHouse", extra={"error": str(exc)})
        return 1
    try:
        for name in args.names or list(queries):
            query = queries[name]
            rows = store.query(query.sql)
            sys.stdout.write(f"\n== {name} ==\n{query.description}\n\n{render_table(rows)}")
    except (TransientStorageError, PermanentStorageError) as exc:
        logger.error("Query failed", extra={"error": str(exc)})
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
