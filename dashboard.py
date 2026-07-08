import streamlit as st
import json
import pandas as pd
from confluent_kafka import Consumer
from datetime import datetime

# Streamlit dashboard that continuously polls the Kafka topic and shows KPIs
# in real time.

CONFIG_PATH = "client.properties"
TOPIC = "tax-evaluation-applications"

st.set_page_config(page_title="Tax Applications Dashboard", layout="wide")


@st.cache_resource
def load_config(path=CONFIG_PATH):
    config = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                k, v = line.split("=", 1)
                config[k.strip()] = v.strip()
    return config


@st.cache_resource
def get_consumer(_config, group_id):
    """Create the Kafka consumer ONCE and reuse it across Streamlit reruns.

    Without @st.cache_resource here, a brand new consumer (with the same
    group.id) would be created on every rerun, which repeatedly triggers a
    consumer-group rebalance and makes the dashboard unstable.
    """
    conf = _config.copy()
    conf["group.id"] = group_id
    # Start from the earliest offset so the dashboard can read historic events
    # the first time this consumer group is ever created.
    conf.setdefault("auto.offset.reset", "earliest")
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    return consumer


st.title("Tax Applications — Real-time KPI Dashboard")

st.sidebar.header("Settings")
refresh_seconds = st.sidebar.number_input(
    "Auto-refresh interval (seconds)", min_value=1, value=3
)
poll_batch_size = st.sidebar.number_input(
    "Max messages to drain per refresh", min_value=1, value=50
)
window_limit = st.sidebar.number_input(
    "Messages to retain in view", min_value=10, value=200
)
group_id = st.sidebar.text_input("Consumer group id", value="dashboard_consumer_group")

config = load_config()
consumer = get_consumer(config, group_id)

st.sidebar.write(f"Topic: `{TOPIC}`")
st.sidebar.write(f"Consumer group: `{group_id}`")

if "buffer" not in st.session_state:
    st.session_state.buffer = []
if "last_poll_count" not in st.session_state:
    st.session_state.last_poll_count = 0


@st.fragment(run_every=refresh_seconds)
def live_dashboard():
    new_messages = 0
    errors = []

    # Drain a batch of messages each cycle instead of just one, so the
    # dashboard can keep up with a real, continuously-produced stream.
    for _ in range(int(poll_batch_size)):
        msg = consumer.poll(timeout=0.2)
        if msg is None:
            break
        if msg.error():
            errors.append(str(msg.error()))
            continue

        try:
            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            errors.append(f"Skipped unparseable message: {e}")
            continue

        value["customer_id"] = key or value.get("customer_id")
        value["_received_at"] = datetime.utcnow().isoformat()
        st.session_state.buffer.append(value)
        new_messages += 1

    st.session_state.last_poll_count = new_messages

    # Trim buffer to the retention window
    if len(st.session_state.buffer) > window_limit:
        st.session_state.buffer = st.session_state.buffer[-window_limit:]

    for err in errors:
        st.error(f"Consumer error: {err}")

    df = pd.DataFrame(st.session_state.buffer)

    status_line = f"Last refresh pulled {new_messages} new message(s)."
    st.caption(status_line)

    st.subheader("KPIs")
    if df.empty:
        st.write("No data yet — waiting for messages on the topic.")
    else:
        total = len(df)
        avg_tax = pd.to_numeric(df.get("tax_due"), errors="coerce").mean()
        avg_income = pd.to_numeric(df.get("income"), errors="coerce").mean()
        by_status = (
            df["status"].value_counts().rename_axis("status").reset_index(name="count")
            if "status" in df.columns
            else pd.DataFrame()
        )

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total events", total)
        col2.metric("Avg tax_due", f"{avg_tax:,.2f}" if pd.notna(avg_tax) else "—")
        col3.metric("Avg income", f"{avg_income:,.2f}" if pd.notna(avg_income) else "—")
        col4.metric(
            "Distinct customers",
            df["customer_id"].nunique() if "customer_id" in df.columns else 0,
        )

        if not by_status.empty:
            st.markdown("**Status distribution**")
            st.dataframe(by_status, use_container_width=True)

        st.subheader("Recent Events")
        sort_col = "submitted_date" if "submitted_date" in df.columns else "_received_at"
        st.dataframe(
            df.sort_values(sort_col, ascending=False).head(50),
            use_container_width=True,
        )


live_dashboard()

st.sidebar.divider()
st.sidebar.write(f"Buffer size: {len(st.session_state.buffer)} / {window_limit}")
if st.sidebar.button("Clear buffer"):
    st.session_state.buffer = []
    st.rerun()