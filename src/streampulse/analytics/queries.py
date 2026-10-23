"""Load named queries from ``sql/analytics.sql``.

Each query starts with a ``-- name: <identifier>`` line; following ``--``
lines up to the SQL body form its description.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_NAME_LINE = re.compile(r"^--\s*name:\s*([a-z0-9_]+)\s*$")


@dataclass(frozen=True)
class NamedQuery:
    name: str
    description: str
    sql: str


def parse_queries(text: str) -> dict[str, NamedQuery]:
    queries: dict[str, NamedQuery] = {}
    current_name: str | None = None
    description: list[str] = []
    body: list[str] = []

    def flush() -> None:
        if current_name is None:
            return
        sql = "\n".join(body).strip().rstrip(";").strip()
        if not sql:
            raise ValueError(f"query {current_name!r} has no SQL body")
        queries[current_name] = NamedQuery(current_name, " ".join(description).strip(), sql)

    for line in text.splitlines():
        match = _NAME_LINE.match(line.strip())
        if match:
            flush()
            current_name = match.group(1)
            if current_name in queries:
                raise ValueError(f"duplicate query name {current_name!r}")
            description, body = [], []
        elif current_name is not None:
            stripped = line.strip()
            if stripped.startswith("--") and not body:
                description.append(stripped.lstrip("-").strip())
            elif stripped or body:
                body.append(line)
    flush()
    return queries


def load_queries(sql_dir: Path) -> dict[str, NamedQuery]:
    return parse_queries((sql_dir / "analytics.sql").read_text())
