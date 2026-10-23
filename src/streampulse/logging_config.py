"""Logging setup shared by every entry point."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

_RESERVED_ATTRIBUTES = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per line, including any ``extra=`` fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRIBUTES and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class KeyValueFormatter(logging.Formatter):
    """Human-readable lines that still surface structured ``extra=`` fields."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED_ATTRIBUTES and not key.startswith("_")
        }
        if not extras:
            return base
        rendered = " ".join(f"{key}={value}" for key, value in sorted(extras.items()))
        return f"{base} | {rendered}"


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else KeyValueFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
