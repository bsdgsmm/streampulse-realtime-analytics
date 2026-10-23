"""Runtime configuration loaded from environment variables.

Every setting has a local-development default except secrets. The ClickHouse
password defaults to an empty string and is never included in ``repr`` output.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


class ConfigError(ValueError):
    """Raised when an environment variable cannot be parsed."""


def _get_str(env: Mapping[str, str], name: str, default: str) -> str:
    value = env.get(name, default).strip()
    return value if value else default


def _get_int(env: Mapping[str, str], name: str, default: int, minimum: int = 0) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


def _get_float(env: Mapping[str, str], name: str, default: float, minimum: float = 0.0) -> float:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


def _get_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{name} must be a boolean, got {raw!r}")


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = "localhost:19092"
    events_topic: str = "streampulse.ecommerce.events.v1"
    dlq_topic: str = "streampulse.ecommerce.events.dlq.v1"
    consumer_group: str = "streampulse-clickhouse-writer"
    client_id: str = "streampulse"


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str = "localhost"
    port: int = 8123
    database: str = "streampulse"
    user: str = "streampulse"
    password: str = field(default="", repr=False)
    secure: bool = False
    connect_timeout_seconds: int = 10
    send_receive_timeout_seconds: int = 30


@dataclass(frozen=True)
class ConsumerSettings:
    batch_size: int = 500
    batch_timeout_seconds: float = 1.0
    allowed_lateness_seconds: int = 60
    dedup_cache_size: int = 100_000
    db_max_attempts: int = 5
    db_retry_base_delay_seconds: float = 0.5
    db_retry_max_delay_seconds: float = 8.0
    max_future_skew_seconds: int = 30


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings
    clickhouse: ClickHouseSettings
    consumer: ConsumerSettings
    log_level: str = "INFO"
    log_format: str = "text"
    sql_dir: Path = DEFAULT_SQL_DIR

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        kafka_defaults = KafkaSettings()
        ch_defaults = ClickHouseSettings()
        consumer_defaults = ConsumerSettings()

        kafka = KafkaSettings(
            bootstrap_servers=_get_str(
                env, "STREAMPULSE_BOOTSTRAP_SERVERS", kafka_defaults.bootstrap_servers
            ),
            events_topic=_get_str(env, "STREAMPULSE_EVENTS_TOPIC", kafka_defaults.events_topic),
            dlq_topic=_get_str(env, "STREAMPULSE_DLQ_TOPIC", kafka_defaults.dlq_topic),
            consumer_group=_get_str(
                env, "STREAMPULSE_CONSUMER_GROUP", kafka_defaults.consumer_group
            ),
            client_id=_get_str(env, "STREAMPULSE_CLIENT_ID", kafka_defaults.client_id),
        )
        clickhouse = ClickHouseSettings(
            host=_get_str(env, "CLICKHOUSE_HOST", ch_defaults.host),
            port=_get_int(env, "CLICKHOUSE_PORT", ch_defaults.port, minimum=1),
            database=_get_str(env, "CLICKHOUSE_DATABASE", ch_defaults.database),
            user=_get_str(env, "CLICKHOUSE_USER", ch_defaults.user),
            password=env.get("CLICKHOUSE_PASSWORD", ""),
            secure=_get_bool(env, "CLICKHOUSE_SECURE", ch_defaults.secure),
            connect_timeout_seconds=_get_int(
                env, "CLICKHOUSE_CONNECT_TIMEOUT_SECONDS", ch_defaults.connect_timeout_seconds, 1
            ),
            send_receive_timeout_seconds=_get_int(
                env,
                "CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS",
                ch_defaults.send_receive_timeout_seconds,
                1,
            ),
        )
        consumer = ConsumerSettings(
            batch_size=_get_int(env, "STREAMPULSE_BATCH_SIZE", consumer_defaults.batch_size, 1),
            batch_timeout_seconds=_get_float(
                env, "STREAMPULSE_BATCH_TIMEOUT_SECONDS", consumer_defaults.batch_timeout_seconds
            ),
            allowed_lateness_seconds=_get_int(
                env,
                "STREAMPULSE_ALLOWED_LATENESS_SECONDS",
                consumer_defaults.allowed_lateness_seconds,
            ),
            dedup_cache_size=_get_int(
                env, "STREAMPULSE_DEDUP_CACHE_SIZE", consumer_defaults.dedup_cache_size, 1
            ),
            db_max_attempts=_get_int(
                env, "STREAMPULSE_DB_MAX_ATTEMPTS", consumer_defaults.db_max_attempts, 1
            ),
            db_retry_base_delay_seconds=_get_float(
                env,
                "STREAMPULSE_DB_RETRY_BASE_DELAY_SECONDS",
                consumer_defaults.db_retry_base_delay_seconds,
            ),
            db_retry_max_delay_seconds=_get_float(
                env,
                "STREAMPULSE_DB_RETRY_MAX_DELAY_SECONDS",
                consumer_defaults.db_retry_max_delay_seconds,
            ),
            max_future_skew_seconds=_get_int(
                env,
                "STREAMPULSE_MAX_FUTURE_SKEW_SECONDS",
                consumer_defaults.max_future_skew_seconds,
            ),
        )
        log_format = _get_str(env, "STREAMPULSE_LOG_FORMAT", "text").lower()
        if log_format not in {"text", "json"}:
            raise ConfigError(
                f"STREAMPULSE_LOG_FORMAT must be 'text' or 'json', got {log_format!r}"
            )
        sql_dir_raw = env.get("STREAMPULSE_SQL_DIR")
        return cls(
            kafka=kafka,
            clickhouse=clickhouse,
            consumer=consumer,
            log_level=_get_str(env, "STREAMPULSE_LOG_LEVEL", "INFO").upper(),
            log_format=log_format,
            sql_dir=Path(sql_dir_raw) if sql_dir_raw else DEFAULT_SQL_DIR,
        )
