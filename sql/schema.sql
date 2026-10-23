CREATE TABLE IF NOT EXISTS events
(
    event_id              UUID,
    event_type            LowCardinality(String),
    event_timestamp       DateTime64(3, 'UTC'),
    produced_at           DateTime64(3, 'UTC'),
    ingested_at           DateTime64(3, 'UTC'),
    user_id               String,
    session_id            String,
    product_id            Nullable(String),
    order_id              Nullable(String),
    amount                Nullable(Decimal(12, 2)),
    currency              LowCardinality(Nullable(String)),
    schema_version        UInt8,
    source                LowCardinality(String),
    is_synthetic          Bool,
    is_late               Bool,
    lateness_ms           UInt64,
    ingestion_delay_ms    Int64,
    end_to_end_latency_ms Int64,
    unexpected_fields     Array(String),
    kafka_topic           LowCardinality(String),
    kafka_partition       UInt32,
    kafka_offset          UInt64,
    broker_timestamp      Nullable(DateTime64(3, 'UTC')),
    consumer_id           LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_type, event_timestamp, event_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS event_rejections
(
    rejected_at     DateTime64(3, 'UTC'),
    error_type      LowCardinality(String),
    error_message   String,
    event_id        Nullable(String),
    raw_payload     String,
    payload_bytes   UInt32,
    kafka_topic     LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    consumer_id     LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (kafka_topic, kafka_partition, kafka_offset)
TTL toDateTime(rejected_at) + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS consumer_batches
(
    batch_id                  UUID,
    consumer_id               LowCardinality(String),
    consumer_group            LowCardinality(String),
    batch_started_at          DateTime64(3, 'UTC'),
    batch_finished_at         DateTime64(3, 'UTC'),
    messages_consumed         UInt32,
    events_inserted           UInt32,
    invalid_events            UInt32,
    duplicate_events          UInt32,
    late_events               UInt32,
    unexpected_field_events   UInt32,
    write_retries             UInt32,
    processing_ms             UInt32,
    avg_end_to_end_latency_ms Float64,
    max_end_to_end_latency_ms Int64,
    max_ingestion_delay_ms    Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(batch_started_at)
ORDER BY (batch_started_at, batch_id)
TTL toDateTime(batch_started_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS consumer_lag
(
    observed_at      DateTime64(3, 'UTC'),
    consumer_group   LowCardinality(String),
    topic            LowCardinality(String),
    partition        UInt32,
    committed_offset Int64,
    high_watermark   Int64,
    lag              Int64
)
ENGINE = MergeTree
ORDER BY (consumer_group, topic, partition, observed_at)
TTL toDateTime(observed_at) + INTERVAL 7 DAY;
