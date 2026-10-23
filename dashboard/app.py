"""Streamlit monitoring dashboard backed by the queries in sql/analytics.sql.

Run with:  streamlit run dashboard/app.py
"""

from __future__ import annotations

from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from streampulse.analytics.queries import load_queries
from streampulse.config import Settings
from streampulse.storage.clickhouse import (
    ClickHouseStore,
    PermanentStorageError,
    TransientStorageError,
)

st.set_page_config(page_title="StreamPulse Monitor", layout="wide")


@st.cache_resource
def get_settings() -> Settings:
    return Settings.from_env()


@st.cache_resource
def get_store() -> ClickHouseStore:
    return ClickHouseStore.connect(get_settings().clickhouse, consumer_id="dashboard")


@st.cache_data(ttl=5)
def run_query(name: str) -> pd.DataFrame:
    query = load_queries(get_settings().sql_dir)[name]
    rows: list[dict[str, Any]] = get_store().query(query.sql)
    return pd.DataFrame(rows)


def metric_value(frame: pd.DataFrame, column: str, default: Any = 0) -> Any:
    if frame.empty or column not in frame or pd.isna(frame.iloc[0][column]):
        return default
    return frame.iloc[0][column]


def percent(value: Any) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{float(value):.1%}"


st.title("StreamPulse - pipeline monitor")
st.caption(
    "All data is synthetic, produced by the StreamPulse generator. Times are UTC. "
    "Measured values come straight from stored rows; values marked *approx.* are sampled."
)

if st.button("Refresh"):
    st.cache_data.clear()

try:
    overview = run_query("pipeline_overview")
except (TransientStorageError, PermanentStorageError) as exc:
    st.error(f"ClickHouse is not reachable: {exc}")
    st.info("Start the stack with `make up` and check the CLICKHOUSE_* settings.")
    st.stop()

if int(metric_value(overview, "unique_events")) == 0:
    st.info(
        "No events stored yet. Run `make produce` and `make consume` (or `make demo`) "
        "and press Refresh."
    )
    st.stop()

quality = run_query("duplicate_and_invalid_rates")
funnel = run_query("funnel_summary")

col = st.columns(6)
col[0].metric("Unique events stored", f"{int(metric_value(overview, 'unique_events')):,}")
col[1].metric("Messages consumed", f"{int(metric_value(quality, 'messages_consumed')):,}")
col[2].metric("Invalid (DLQ)", f"{int(metric_value(overview, 'rejected_messages')):,}")
col[3].metric("Duplicates dropped", f"{int(metric_value(quality, 'duplicates_dropped')):,}")
col[4].metric("Late events", f"{int(metric_value(overview, 'late_events')):,}")
col[5].metric("Sessions", f"{int(metric_value(overview, 'sessions')):,}")
st.caption(f"Last ingestion: {metric_value(overview, 'last_ingested_at_utc', '-')} UTC")

st.subheader("Event volume: event time vs ingestion time")
timeline = run_query("event_time_vs_ingestion_time")
if timeline.empty:
    st.write("No events in the last 120 minutes of ingestion time.")
else:
    long = timeline.melt("minute_utc", var_name="basis", value_name="events")
    st.altair_chart(
        alt.Chart(long)
        .mark_line(point=True)
        .encode(
            x=alt.X("minute_utc:T", title="minute (UTC)"),
            y=alt.Y("events:Q"),
            color=alt.Color("basis:N", title=None),
        ),
        use_container_width=True,
    )
    st.caption(
        "Event-time counts keep changing as late events arrive; ingestion-time counts are "
        "final once a minute has passed."
    )

left, right = st.columns(2)
with left:
    st.subheader("Conversion funnel")
    rates = st.columns(3)
    rates[0].metric(
        "View -> cart (sessions)", percent(metric_value(funnel, "view_to_cart_rate", None))
    )
    rates[1].metric(
        "Checkout -> order (sessions)",
        percent(metric_value(funnel, "checkout_to_order_rate", None)),
    )
    rates[2].metric(
        "Order -> payment (orders)", percent(metric_value(funnel, "payment_completion_rate", None))
    )
    st.dataframe(funnel, hide_index=True, use_container_width=True)
with right:
    st.subheader("Orders and payments (event time)")
    orders = run_query("order_payment_trend")
    if orders.empty:
        st.write("No order events yet.")
    else:
        st.bar_chart(orders.set_index("bucket_utc"))

left, right = st.columns(2)
with left:
    st.subheader("Late events by type")
    st.dataframe(run_query("late_events_by_type"), hide_index=True, use_container_width=True)
    st.dataframe(run_query("late_event_rate"), hide_index=True, use_container_width=True)
with right:
    st.subheader("Rejected messages")
    rejections = run_query("rejections_by_error_type")
    if rejections.empty:
        st.write("No rejected messages.")
    else:
        st.dataframe(rejections, hide_index=True, use_container_width=True)
    st.dataframe(quality, hide_index=True, use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Consumer lag (approx.)")
    st.caption("Sampled after each commit: high watermark minus committed offset.")
    lag = run_query("consumer_lag_latest")
    if lag.empty:
        st.write("No lag samples yet.")
    else:
        st.dataframe(lag, hide_index=True, use_container_width=True)
with right:
    st.subheader("Batch health")
    st.caption("Latency = ingested_at - produced_at; assumes producer and consumer clocks agree.")
    st.dataframe(run_query("consumer_batch_health"), hide_index=True, use_container_width=True)

st.subheader("Recent session event sequences")
st.dataframe(run_query("session_event_sequences"), hide_index=True, use_container_width=True)
