-- StreamPulse analytical queries (ClickHouse SQL).
-- Every query is addressable by its "-- name:" line; the dashboard,
-- scripts/run_analytics.py and the integration tests load them from here.
-- Times are UTC. Reads from `events` use FINAL so that duplicate rows not yet
-- collapsed by a background merge are not counted twice.
-- Time windows are anchored to the newest stored timestamp rather than now(),
-- so the queries also work on replayed or historical data.

-- name: pipeline_overview
-- Headline counts across the stored data. late_events and rejected_messages
-- are directly measured; duplicates_dropped_by_consumer counts duplicates the
-- consumer filtered before writing (reprocessed batches after a restart are
-- counted again).
SELECT
    (SELECT count() FROM events FINAL) AS unique_events,
    (SELECT count() FROM events) AS physical_rows,
    (SELECT countIf(is_late) FROM events FINAL) AS late_events,
    (SELECT count() FROM event_rejections FINAL) AS rejected_messages,
    (SELECT sum(duplicate_events) FROM consumer_batches) AS duplicates_dropped_by_consumer,
    (SELECT uniqExact(session_id) FROM events FINAL) AS sessions,
    (SELECT max(ingested_at) FROM events) AS last_ingested_at_utc;

-- name: events_per_minute
-- Event volume per minute of event time over the latest 60 minutes of data.
SELECT
    toStartOfMinute(event_timestamp) AS minute_utc,
    count() AS event_count,
    countIf(event_type = 'product_viewed') AS product_views,
    countIf(event_type = 'order_placed') AS orders
FROM events FINAL
WHERE event_timestamp >= (SELECT max(event_timestamp) FROM events) - INTERVAL 60 MINUTE
GROUP BY minute_utc
ORDER BY minute_utc;

-- name: event_time_vs_ingestion_time
-- The same events bucketed by when they happened and by when the consumer
-- stored them. The gap between the two series is delivery and processing
-- delay; late events show up in early event-time minutes long after they
-- were ingested.
SELECT
    minute_utc,
    sumIf(event_count, basis = 'event_time') AS by_event_time,
    sumIf(event_count, basis = 'ingestion_time') AS by_ingestion_time
FROM
(
    SELECT toStartOfMinute(event_timestamp) AS minute_utc, 'event_time' AS basis, count() AS event_count
    FROM events FINAL
    GROUP BY minute_utc
    UNION ALL
    SELECT toStartOfMinute(ingested_at) AS minute_utc, 'ingestion_time' AS basis, count() AS event_count
    FROM events FINAL
    GROUP BY minute_utc
)
WHERE minute_utc >= (SELECT max(ingested_at) FROM events) - INTERVAL 120 MINUTE
GROUP BY minute_utc
ORDER BY minute_utc;

-- name: daily_event_counts_by_type
-- Daily counts per event type (UTC calendar day of event time).
SELECT
    toDate(event_timestamp) AS event_date_utc,
    event_type,
    count() AS event_count,
    uniqExact(session_id) AS sessions,
    uniqExact(user_id) AS users
FROM events FINAL
GROUP BY event_date_utc, event_type
ORDER BY event_date_utc, event_type;

-- name: product_engagement_trend
-- Product views and add-to-cart events per five-minute event-time bucket.
SELECT
    toStartOfFiveMinutes(event_timestamp) AS bucket_utc,
    countIf(event_type = 'product_viewed') AS product_views,
    countIf(event_type = 'product_added_to_cart') AS add_to_cart_events,
    round(add_to_cart_events / nullIf(product_views, 0), 4) AS add_to_cart_events_per_view
FROM events FINAL
GROUP BY bucket_utc
ORDER BY bucket_utc;

-- name: funnel_summary
-- Session and order funnel over all stored data. Definitions match
-- streampulse.analytics.conversion and are documented in the README.
SELECT
    countIf(viewed) AS sessions_with_view,
    countIf(viewed AND carted) AS sessions_with_view_and_cart,
    round(sessions_with_view_and_cart / nullIf(sessions_with_view, 0), 4) AS view_to_cart_rate,
    countIf(checked_out) AS sessions_with_checkout,
    countIf(checked_out AND ordered) AS sessions_with_checkout_and_order,
    round(sessions_with_checkout_and_order / nullIf(sessions_with_checkout, 0), 4)
        AS checkout_to_order_rate,
    (
        SELECT countIf(placed)
        FROM
        (
            SELECT order_id, countIf(event_type = 'order_placed') > 0 AS placed
            FROM events FINAL
            WHERE order_id IS NOT NULL
            GROUP BY order_id
        )
    ) AS orders_placed,
    (
        SELECT countIf(placed AND paid)
        FROM
        (
            SELECT
                order_id,
                countIf(event_type = 'order_placed') > 0 AS placed,
                countIf(event_type = 'payment_completed') > 0 AS paid
            FROM events FINAL
            WHERE order_id IS NOT NULL
            GROUP BY order_id
        )
    ) AS orders_paid,
    round(orders_paid / nullIf(orders_placed, 0), 4) AS payment_completion_rate
FROM
(
    SELECT
        session_id,
        countIf(event_type = 'product_viewed') > 0 AS viewed,
        countIf(event_type = 'product_added_to_cart') > 0 AS carted,
        countIf(event_type = 'checkout_started') > 0 AS checked_out,
        countIf(event_type = 'order_placed') > 0 AS ordered
    FROM events FINAL
    GROUP BY session_id
);

-- name: view_to_cart_conversion_daily
-- Session view-to-cart rate per UTC day; a session belongs to the day of its
-- first event.
SELECT
    session_date_utc,
    countIf(viewed) AS sessions_with_view,
    countIf(viewed AND carted) AS sessions_with_view_and_cart,
    round(sessions_with_view_and_cart / nullIf(sessions_with_view, 0), 4) AS view_to_cart_rate
FROM
(
    SELECT
        session_id,
        toDate(min(event_timestamp)) AS session_date_utc,
        countIf(event_type = 'product_viewed') > 0 AS viewed,
        countIf(event_type = 'product_added_to_cart') > 0 AS carted
    FROM events FINAL
    GROUP BY session_id
)
GROUP BY session_date_utc
ORDER BY session_date_utc;

-- name: checkout_to_order_conversion_daily
-- Session checkout-to-order rate per UTC day of the session's first event.
SELECT
    session_date_utc,
    countIf(checked_out) AS sessions_with_checkout,
    countIf(checked_out AND ordered) AS sessions_with_checkout_and_order,
    round(sessions_with_checkout_and_order / nullIf(sessions_with_checkout, 0), 4)
        AS checkout_to_order_rate
FROM
(
    SELECT
        session_id,
        toDate(min(event_timestamp)) AS session_date_utc,
        countIf(event_type = 'checkout_started') > 0 AS checked_out,
        countIf(event_type = 'order_placed') > 0 AS ordered
    FROM events FINAL
    GROUP BY session_id
)
GROUP BY session_date_utc
ORDER BY session_date_utc;

-- name: top_products_view_to_cart
-- Product-level view-to-cart rate: distinct sessions that added the product
-- divided by distinct sessions that viewed it (products with 5+ viewers).
SELECT
    product_id,
    uniqExactIf(session_id, event_type = 'product_viewed') AS viewing_sessions,
    uniqExactIf(session_id, event_type = 'product_added_to_cart') AS carting_sessions,
    round(carting_sessions / nullIf(viewing_sessions, 0), 4) AS session_view_to_cart_rate
FROM events FINAL
WHERE product_id IS NOT NULL
GROUP BY product_id
HAVING viewing_sessions >= 5
ORDER BY viewing_sessions DESC, product_id
LIMIT 20;

-- name: order_payment_trend
-- Order lifecycle events per five-minute event-time bucket.
SELECT
    toStartOfFiveMinutes(event_timestamp) AS bucket_utc,
    countIf(event_type = 'order_placed') AS orders_placed,
    countIf(event_type = 'payment_completed') AS payments_completed,
    countIf(event_type = 'order_cancelled') AS orders_cancelled
FROM events FINAL
WHERE event_type IN ('order_placed', 'payment_completed', 'order_cancelled')
GROUP BY bucket_utc
ORDER BY bucket_utc;

-- name: payment_amount_by_currency
-- Completed payment value per currency. Amounts are not converted, so values
-- in different currencies must not be summed together.
SELECT
    currency,
    count() AS payments,
    sum(amount) AS total_amount,
    round(avg(amount), 2) AS avg_amount
FROM events FINAL
WHERE event_type = 'payment_completed'
GROUP BY currency
ORDER BY payments DESC;

-- name: late_event_rate
-- Share of events that arrived behind their partition watermark, per
-- five-minute bucket of ingestion time, with delivery delay percentiles.
SELECT
    toStartOfFiveMinutes(ingested_at) AS ingestion_bucket_utc,
    count() AS event_count,
    countIf(is_late) AS late_events,
    round(late_events / count(), 4) AS late_rate,
    quantileExact(0.5)(ingestion_delay_ms) AS p50_ingestion_delay_ms,
    quantileExact(0.95)(ingestion_delay_ms) AS p95_ingestion_delay_ms,
    max(ingestion_delay_ms) AS max_ingestion_delay_ms
FROM events FINAL
GROUP BY ingestion_bucket_utc
ORDER BY ingestion_bucket_utc;

-- name: late_events_by_type
-- Late events per event type with their lateness beyond the watermark.
SELECT
    event_type,
    count() AS event_count,
    countIf(is_late) AS late_events,
    round(late_events / count(), 4) AS late_rate,
    round(avgIf(lateness_ms, is_late) / 1000, 1) AS avg_lateness_seconds
FROM events FINAL
GROUP BY event_type
ORDER BY event_type;

-- name: duplicate_and_invalid_rates
-- Rates relative to all messages the consumer read, from committed batches.
SELECT
    sum(messages_consumed) AS messages_consumed,
    sum(events_inserted) AS events_written,
    sum(duplicate_events) AS duplicates_dropped,
    sum(invalid_events) AS invalid_messages,
    round(duplicates_dropped / nullIf(messages_consumed, 0), 4) AS duplicate_rate,
    round(invalid_messages / nullIf(messages_consumed, 0), 4) AS invalid_rate
FROM consumer_batches;

-- name: storage_duplicate_check
-- Physical rows versus distinct event IDs. A positive difference means
-- duplicates reached storage and are waiting for a merge; FINAL hides them.
SELECT
    count() AS physical_rows,
    uniqExact(event_id) AS unique_event_ids,
    physical_rows - unique_event_ids AS physical_duplicate_rows
FROM events;

-- name: rejections_by_error_type
-- Invalid messages grouped by rejection reason.
SELECT
    error_type,
    count() AS rejected_messages,
    min(rejected_at) AS first_seen_utc,
    max(rejected_at) AS last_seen_utc,
    any(error_message) AS example_error
FROM event_rejections FINAL
GROUP BY error_type
ORDER BY rejected_messages DESC;

-- name: consumer_lag_latest
-- Most recent lag sample per partition (high watermark minus committed offset).
SELECT
    consumer_group,
    topic,
    partition,
    argMax(committed_offset, observed_at) AS committed_offset,
    argMax(high_watermark, observed_at) AS high_watermark,
    argMax(lag, observed_at) AS lag,
    max(observed_at) AS observed_at_utc
FROM consumer_lag
GROUP BY consumer_group, topic, partition
ORDER BY consumer_group, topic, partition;

-- name: consumer_batch_health
-- Throughput, retries and latency of committed batches per minute of
-- processing time. Latency = ingested_at - produced_at (same-host clocks).
SELECT
    toStartOfMinute(batch_started_at) AS minute_utc,
    count() AS batches,
    sum(messages_consumed) AS messages,
    sum(events_inserted) AS events_written,
    sum(write_retries) AS write_retries,
    round(avg(processing_ms), 1) AS avg_batch_processing_ms,
    round(avg(avg_end_to_end_latency_ms), 1) AS avg_end_to_end_latency_ms,
    max(max_end_to_end_latency_ms) AS max_end_to_end_latency_ms
FROM consumer_batches
GROUP BY minute_utc
ORDER BY minute_utc;

-- name: session_event_sequences
-- The 25 most recent sessions with their events ordered by event time.
SELECT
    session_id,
    user_id,
    min(event_timestamp) AS session_start_utc,
    count() AS event_count,
    arrayStringConcat(
        arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((event_timestamp, event_type)))),
        ' > '
    ) AS event_sequence
FROM events FINAL
GROUP BY session_id, user_id
ORDER BY session_start_utc DESC, session_id
LIMIT 25;

-- name: top_session_paths
-- Most common complete event sequences across sessions.
SELECT
    event_sequence,
    count() AS sessions
FROM
(
    SELECT
        session_id,
        arrayStringConcat(
            arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((event_timestamp, event_type)))),
            ' > '
        ) AS event_sequence
    FROM events FINAL
    GROUP BY session_id
)
GROUP BY event_sequence
ORDER BY sessions DESC, event_sequence
LIMIT 15;
