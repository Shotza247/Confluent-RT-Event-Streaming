import streamlit as st
import json
import pandas as pd
from confluent_kafka import Consumer
import time

# Streamlit dashboard that polls the Kafka topic and shows KPIs

CONFIG_PATH = "client.properties"
TOPIC = "tax-evaluation-applications"

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


def create_consumer(conf):
    conf = conf.copy()
    conf["group.id"] = "dashboard_consumer_group"
    # Start from the earliest offset so the dashboard can read historic events
    conf["auto.offset.reset"] = "earliest"
    return Consumer(conf)


st.title("Tax Applications — Real-time KPI Dashboard")

st.sidebar.header("Settings")
poll_interval = st.sidebar.number_input("Poll interval (seconds)", min_value=1, value=5)

config = load_config()
consumer = create_consumer(config)
consumer.subscribe([TOPIC])

st.sidebar.write(f"Topic: {TOPIC}")

# In-memory rolling window
window_limit = st.sidebar.number_input("Messages to retain", min_value=10, value=200)

if 'buffer' not in st.session_state:
    st.session_state.buffer = []

placeholder_kpis = st.container()
placeholder_table = st.container()

with st.sidebar:
    st.write("Consumer group: dashboard_consumer_group")

st.write("Polling for new messages... Refresh the browser to reset state.")

try:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        st.write("No new messages this poll.")
    elif msg.error():
        st.error(f"Consumer error: {msg.error()}")
    else:
        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8'))
        value['customer_id'] = key or value.get('customer_id')
        st.session_state.buffer.append(value)
        # trim buffer
        if len(st.session_state.buffer) > window_limit:
            st.session_state.buffer = st.session_state.buffer[-window_limit:]

    # Build DataFrame for KPIs
    df = pd.DataFrame(st.session_state.buffer)

    with placeholder_kpis:
        st.subheader("KPIs")
        if df.empty:
            st.write("No data yet.")
        else:
            total = len(df)
            avg_tax = df['tax_due'].astype(float).mean()
            by_status = df['status'].value_counts().to_frame().reset_index()
            col1, col2, col3 = st.columns(3)
            col1.metric("Total events", total)
            col2.metric("Avg tax_due", f"{avg_tax:,.2f}")
            col3.metric("Distinct customers", df['customer_id'].nunique())

            st.markdown("**Status distribution**")
            st.dataframe(by_status)

    with placeholder_table:
        st.subheader("Recent Events")
        if not df.empty:
            st.dataframe(df.sort_values('event_time', ascending=False).head(50))

except Exception as e:
    st.error(f"Error polling consumer: {e}")

finally:
    # Leave consumer open for subsequent polls during the Streamlit run
    pass
