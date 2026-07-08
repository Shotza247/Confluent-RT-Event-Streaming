import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from confluent_kafka import Consumer
from datetime import datetime

# Streamlit dashboard that continuously polls the Kafka topic and shows KPIs
# in real time.

CONFIG_PATH = "client.properties"
TOPIC = "tax-evaluation-applications"

st.set_page_config(page_title="Tax Applications Dashboard", layout="wide")

# ---- Minimal dark / warm-neutral palette for charts ----
BG = "#111827"        # Deep slate background
GRID = "#374151"      # Soft gray grid lines
TEXT = "#F9FAFB"      # Crisp off-white text

ACCENTS = [
    "#3B82F6",  # Blue
    "#10B981",  # Emerald
    "#F59E0B",  # Amber
    "#EF4444",  # Coral Red
    "#8B5CF6",  # Purple
    "#06B6D4",  # Cyan
]

CHART_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=BG,
    font=dict(color=TEXT, family="Inter, sans-serif"),
    margin=dict(l=30, r=20, t=40, b=30),
    xaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    yaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    colorway=ACCENTS,
)


def style(fig):
    fig.update_layout(**CHART_LAYOUT)
    return fig


@st.cache_resource
def load_config(path=CONFIG_PATH):
    """Load Kafka client config.

    Priority:
    1. Streamlit secrets (st.secrets["kafka"]) — used on Streamlit Community
       Cloud, where client.properties is never committed to the repo.
    2. Local client.properties file — used for local development.

    st.secrets raises StreamlitSecretNotFoundError (rather than just being
    empty) when no secrets.toml exists anywhere at all, which is the normal
    case for local dev — so that specific error is treated the same as
    "no kafka secrets configured" and we fall through to the local file.
    """
    try:
        has_kafka_secret = "kafka" in st.secrets
    except st.errors.StreamlitSecretNotFoundError:
        has_kafka_secret = False

    if has_kafka_secret:
        # st.secrets returns an AttrDict-like mapping; convert to plain dict
        # and make sure keys/values are strings.
        #
        # Defensive: if the secrets.toml used unquoted dotted keys (e.g.
        # bootstrap.servers = "...") TOML parses that as a NESTED TABLE
        # (bootstrap -> {servers: "..."}) rather than a literal key, which
        # confluent-kafka can't understand. Flatten any such nested tables
        # back into dotted keys so this works either way.
        raw = dict(st.secrets["kafka"])
        flat = {}
        for k, v in raw.items():
            if hasattr(v, "items"):
                for sub_k, sub_v in v.items():
                    flat[f"{k}.{sub_k}"] = str(sub_v)
            else:
                flat[str(k)] = str(v)
        return flat

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

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total events", total)
        col2.metric("Avg tax_due", f"{avg_tax:,.2f}" if pd.notna(avg_tax) else "—")
        col3.metric("Avg income", f"{avg_income:,.2f}" if pd.notna(avg_income) else "—")
        col4.metric(
            "Distinct customers",
            df["customer_id"].nunique() if "customer_id" in df.columns else 0,
        )

        st.divider()
        chart_col1, chart_col2 = st.columns(2)

        # --- Status distribution ---
        if "status" in df.columns:
            by_status = (
                df["status"].value_counts().rename_axis("status").reset_index(name="count")
            )
            fig_status = px.bar(
                by_status.sort_values("count"),
                x="count",
                y="status",
                orientation="h",
                title="Applications by Status",
            )
            fig_status.update_traces(marker_line_width=0)
            chart_col1.plotly_chart(style(fig_status), use_container_width=True)

        # --- Employment type breakdown (donut) ---
        if "employment_type" in df.columns:
            by_emp = (
                df["employment_type"]
                .value_counts()
                .rename_axis("employment_type")
                .reset_index(name="count")
            )
            fig_emp = px.pie(
                by_emp,
                names="employment_type",
                values="count",
                hole=0.55,
                title="Employment Type Mix",
            )
            fig_emp.update_traces(marker=dict(line=dict(color=BG, width=2)))
            chart_col2.plotly_chart(style(fig_emp), use_container_width=True)

        chart_col3, chart_col4 = st.columns(2)

        # --- Avg tax_due by province ---
        if {"province", "tax_due"}.issubset(df.columns):
            by_province = (
                df.assign(tax_due=pd.to_numeric(df["tax_due"], errors="coerce"))
                .groupby("province", as_index=False)["tax_due"]
                .mean()
                .sort_values("tax_due")
            )
            fig_prov = px.bar(
                by_province,
                x="tax_due",
                y="province",
                orientation="h",
                title="Avg Tax Due by Province",
            )
            fig_prov.update_traces(marker_line_width=0)
            chart_col3.plotly_chart(style(fig_prov), use_container_width=True)

        # --- Income distribution ---
        if "income" in df.columns:
            fig_income = px.histogram(
                df.assign(income=pd.to_numeric(df["income"], errors="coerce")),
                x="income",
                nbins=20,
                title="Income Distribution",
            )
            fig_income.update_traces(marker_line_width=0)
            chart_col4.plotly_chart(style(fig_income), use_container_width=True)

        # --- Tax due trend over submitted_date ---
        if {"submitted_date", "tax_due"}.issubset(df.columns):
            trend = df.copy()
            trend["submitted_date"] = pd.to_datetime(
                trend["submitted_date"], errors="coerce"
            )
            trend["tax_due"] = pd.to_numeric(trend["tax_due"], errors="coerce")
            trend = trend.dropna(subset=["submitted_date"]).sort_values("submitted_date")
            if not trend.empty:
                fig_trend = go.Figure()
                fig_trend.add_trace(
                    go.Scatter(
                        x=trend["submitted_date"],
                        y=trend["tax_due"],
                        mode="lines+markers",
                        line=dict(color=ACCENTS[0], width=2),
                        marker=dict(size=5, color=ACCENTS[0]),
                    )
                )
                fig_trend.update_layout(title="Tax Due Over Submitted Date")
                st.plotly_chart(style(fig_trend), use_container_width=True)

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