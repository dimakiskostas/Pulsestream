"""PulseStream dashboard: a Streamlit page over the API (ADR 0007).

    uv run pulsestream dashboard      # == streamlit run src/pulsestream/dashboard/app.py

Environment: PULSE_API_URL (default http://127.0.0.1:8000),
PULSE_DASHBOARD_AUTOREFRESH ("1" to re-run every 10 s, the default).
"""

import os
import time
from datetime import UTC, datetime

import httpx
import streamlit as st

from pulsestream.dashboard.data import ApiClient

st.set_page_config(page_title="PulseStream", layout="wide")
st.title("PulseStream")

api_url = st.sidebar.text_input("API URL", os.environ.get("PULSE_API_URL", "http://127.0.0.1:8000"))
minutes = st.sidebar.slider("Window (minutes)", min_value=5, max_value=180, value=60, step=5)
auto_refresh = st.sidebar.checkbox(
    "Auto-refresh every 10 s", value=os.environ.get("PULSE_DASHBOARD_AUTOREFRESH", "1") == "1"
)

client = ApiClient(api_url)
try:
    health = client.health()
    stats = client.stats()
    lake = client.lake()
    jobs = client.jobs()
    minutely = client.minutely(minutes)
    top = client.top_entities(minutes)
except httpx.HTTPError as exc:
    st.error(f"API not reachable at {api_url}: {exc}")
    st.stop()
finally:
    client.close()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Events stored", f"{stats['events_total']:,}")
m2.metric("Last minute", stats["events_last_min"])
lag = stats["lag_seconds"]
m3.metric("Lag (s)", "n/a" if lag is None else f"{lag:.1f}")
m4.metric("Pipeline", health["status"], delta=f"queue {health['pipeline']['queue_depth']}")

st.subheader(f"Edits per minute, last {minutes} min (all wikis)")
if minutely.is_empty():
    st.info(
        "No aggregates yet. The refresh job runs every minute; "
        "POST /jobs/refresh_aggregates/run forces it."
    )
else:
    st.line_chart(minutely, x="bucket_ts", y=["count", "bot_count"])

left, right = st.columns(2)
with left:
    st.subheader("Busiest wikis")
    if top.is_empty():
        st.info("No data in the window yet.")
    else:
        st.bar_chart(top, x="entity", y="count", horizontal=True)
with right:
    st.subheader("Raw lake")
    if lake["enabled"]:
        st.write(
            f"{lake['files']} files in {lake['partitions']} partitions, "
            f"{lake['bytes'] / 1e6:.1f} MB, {lake['rows']:,} rows"
        )
    else:
        st.write("disabled")
    st.subheader("Maintenance jobs")
    st.dataframe(
        [
            {
                "job": job["name"],
                "runs": job["runs"],
                "failures": job["failures"],
                "last result": job["last_result"],
                "last error": job["last_error"],
                "next run": job["next_run_at"],
            }
            for job in jobs
        ],
        hide_index=True,
    )

st.caption(f"Reading {api_url} at {datetime.now(UTC):%H:%M:%S} UTC")
if auto_refresh:
    time.sleep(10)
    st.rerun()
