import time
import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from datetime import datetime

# Streamlit dashboard that continuously polls MULTIPLE Kafka topics —
# the raw producer topic AND the Flink SQL-derived summary/filter topics —
# and shows KPIs / charts for each in real time.

CONFIG_PATH = "client.properties"

# ---- Topics ----
RAW_TOPIC = "tax-evaluation-applications"

# Flink-derived topics, from your Confluent Cloud Stream Lineage graph.
#
# VERIFY THESE: two names were truncated in the screenshot you shared —
# "high_income_applicatio..." and "employment_summary_...". I've guessed
# high_income_applications and employment_summary_table (matching the
# _table naming pattern used by tax_year_summary_table /
# regional_summary_table). If either is wrong, that tab will just silently
# show "no data yet" forever — Kafka doesn't error on subscribing to a
# topic name that doesn't match, it just never receives anything for it.
# Fix the values below to match exactly what's in Confluent Cloud.
FLINK_TOPICS = {
    "High Income Applications": "high_income_applications",
    "Status Summary": "status_summary",
    "Tax Year Summary": "tax_year_summary_table",
    "Regional Summary": "regional_summary_table",
    "Province Tax Revenue": "province_tax_revenue",
    "Income by Employment": "income_by_employment",
    "High Tax Due": "high_tax_due",
    "Employment Summary": "employment_summary_table",  # <-- verify exact name
}

ALL_TOPICS = [RAW_TOPIC] + list(FLINK_TOPICS.values())

st.set_page_config(page_title="Tax Applications Dashboard", layout="wide")

# ---- Palette ----
BG = "#0F172A"
GRID = "#334155"
TEXT = "#F8FAFC"
ACCENTS = ["#38BDF8", "#22C55E", "#F97316", "#A855F7", "#F43F5E", "#EAB308"]

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
    """Load Kafka (+ Schema Registry, if present) client config.

    Priority:
    1. Streamlit secrets (st.secrets["kafka"]) — used on Streamlit Community
       Cloud, where client.properties is never committed to the repo.
    2. Local client.properties file — used for local development.

    Schema Registry settings, if you add them, should use the
    "schema.registry." prefix, e.g.:
        schema.registry.url=https://psrc-xxxxx.region.aws.confluent.cloud
        schema.registry.basic.auth.user.info=SR_API_KEY:SR_API_SECRET
    (Get these from Confluent Cloud -> Environment -> Schema Registry ->
    API keys — this is a SEPARATE key/secret pair from your Kafka cluster
    credentials.)
    """
    try:
        has_kafka_secret = "kafka" in st.secrets
    except st.errors.StreamlitSecretNotFoundError:
        has_kafka_secret = False

    if has_kafka_secret:
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


def split_kafka_and_schema_registry_config(config):
    """Separate plain Kafka client config from schema.registry.* config."""
    kafka_conf = {}
    sr_conf = {}
    for k, v in config.items():
        if k.startswith("schema.registry."):
            sr_conf[k[len("schema.registry."):]] = v
        else:
            kafka_conf[k] = v
    return kafka_conf, sr_conf


@st.cache_resource
def get_consumer(_kafka_conf, group_id, topics):
    """Create ONE Kafka consumer subscribed to ALL topics, cached across
    Streamlit reruns. Messages are routed to the right buffer using
    msg.topic() in the poll loop below.

    Without @st.cache_resource, a brand new consumer (with the same
    group.id) would be created on every rerun, repeatedly triggering a
    consumer-group rebalance.
    """
    conf = _kafka_conf.copy()
    conf["group.id"] = group_id
    conf.setdefault("auto.offset.reset", "earliest")
    consumer = Consumer(conf)
    consumer.subscribe(list(topics))
    return consumer


@st.cache_resource
def get_avro_deserializer(_sr_conf):
    """Build a schema-registry-aware Avro deserializer, if SR config is
    present. Returns None if no schema.registry.* settings were provided —
    in that case Avro topics will show a clear warning instead of silently
    failing.

    Passing schema_str=None to AvroDeserializer means it looks up the
    correct writer schema per-message from the registry (using the schema
    ID embedded in the Confluent wire format), so ONE deserializer works
    across all your Flink topics even though they have different schemas.
    """
    if not _sr_conf.get("url"):
        return None
    client = SchemaRegistryClient(_sr_conf)
    return AvroDeserializer(client, schema_str=None)


def safe_deserialize(msg, avro_deserializer):
    """Try JSON first, then Avro (if a schema registry is configured).
    Returns (value_dict, key, error_str)."""
    try:
        key = msg.key().decode("utf-8") if msg.key() else None
    except UnicodeDecodeError:
        key = None

    raw_value = msg.value()

    # Attempt 1: plain JSON (works for your raw producer topic, and any
    # Flink topic explicitly created with 'value.format' = 'json-registry')
    try:
        value = json.loads(raw_value.decode("utf-8"))
        return value, key, None
    except UnicodeDecodeError:
        pass  # fall through to Avro attempt below
    except json.JSONDecodeError as e:
        return None, key, f"JSON decode error: {e}"

    # Attempt 2: Avro via Schema Registry (Confluent Cloud Flink's default
    # sink format when no explicit value.format is set)
    if avro_deserializer is not None:
        try:
            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            value = avro_deserializer(raw_value, ctx)
            if value is None:
                return None, key, "Avro payload decoded to null."
            return dict(value), key, None
        except Exception as e:
            return None, key, f"Avro decode failed: {e}"

    return None, key, (
        "Payload isn't valid UTF-8/JSON, and no Schema Registry is "
        "configured to try Avro. Add schema.registry.url and "
        "schema.registry.basic.auth.user.info to client.properties (or "
        "st.secrets['kafka']) — see README for details."
    )


st.title("Tax Applications — Real-time Dashboard")

st.sidebar.header("Settings")
refresh_seconds = st.sidebar.number_input(
    "Auto-refresh interval (seconds)", min_value=1, value=3
)
poll_batch_size = st.sidebar.number_input(
    "Max messages to drain per refresh", min_value=1, value=50
)
window_limit = st.sidebar.number_input(
    "Messages to retain per topic", min_value=10, value=200
)

# --- Consumer group handling ---
# A stable group id RESUMES from wherever it last left off (including
# "already at the end, nothing new to read" if a prior run drained
# everything). A fresh group id always starts from the earliest offset on
# every topic. Default to fresh so "no data showing up" isn't silently
# caused by a stale group — flip this off once you deliberately want to
# resume a specific group's progress.
use_fresh_group = st.sidebar.checkbox(
    "Use a fresh consumer group (recommended — re-reads all topics from "
    "the earliest offset)",
    value=True,
)
if "fresh_group_suffix" not in st.session_state:
    st.session_state.fresh_group_suffix = str(int(time.time()))

if use_fresh_group:
    group_id = f"dashboard_consumer_group_{st.session_state.fresh_group_suffix}"
else:
    group_id = st.sidebar.text_input("Consumer group id", value="dashboard_consumer_group")

config = load_config()
kafka_conf, sr_conf = split_kafka_and_schema_registry_config(config)
consumer = get_consumer(kafka_conf, group_id, ALL_TOPICS)
avro_deserializer = get_avro_deserializer(sr_conf)

st.sidebar.write(f"Active group id: `{group_id}`")
if avro_deserializer is None:
    st.sidebar.caption(
        "⚠️ No Schema Registry configured — Avro topics will show a decode "
        "warning instead of data."
    )
else:
    st.sidebar.caption("✅ Schema Registry configured — Avro topics supported.")

st.sidebar.write("**Subscribed topics:**")
for label, topic in {"Raw Applications": RAW_TOPIC, **FLINK_TOPICS}.items():
    st.sidebar.caption(f"{label}: `{topic}`")

# Per-topic buffers + per-topic decode error tracking, persisted in session
if "buffers" not in st.session_state:
    st.session_state.buffers = {t: [] for t in ALL_TOPICS}
if "topic_errors" not in st.session_state:
    st.session_state.topic_errors = {t: None for t in ALL_TOPICS}


@st.fragment(run_every=refresh_seconds)
def live_dashboard():
    new_counts = {t: 0 for t in ALL_TOPICS}

    for _ in range(int(poll_batch_size)):
        msg = consumer.poll(timeout=0.2)
        if msg is None:
            break
        if msg.error():
            continue

        topic = msg.topic()
        value, key, err = safe_deserialize(msg, avro_deserializer)

        if err is not None:
            st.session_state.topic_errors[topic] = err
            continue

        st.session_state.topic_errors[topic] = None
        value["customer_id"] = value.get("customer_id") or key
        value["_received_at"] = datetime.utcnow().isoformat()
        st.session_state.buffers.setdefault(topic, []).append(value)
        new_counts[topic] = new_counts.get(topic, 0) + 1

        buf = st.session_state.buffers[topic]
        if len(buf) > window_limit:
            st.session_state.buffers[topic] = buf[-window_limit:]

    total_new = sum(new_counts.values())
    st.caption(f"Last refresh pulled {total_new} new message(s) across all topics.")

    tab_labels = ["Raw Applications"] + list(FLINK_TOPICS.keys())
    tab_topics = [RAW_TOPIC] + list(FLINK_TOPICS.values())
    tabs = st.tabs(tab_labels)

    for tab, label, topic in zip(tabs, tab_labels, tab_topics):
        with tab:
            err = st.session_state.topic_errors.get(topic)
            if err:
                st.warning(f"`{topic}`: {err}")

            df = pd.DataFrame(st.session_state.buffers.get(topic, []))
            if df.empty:
                st.write("No data yet — waiting for messages on this topic.")
                continue

            if topic == RAW_TOPIC:
                render_raw_feed(df)
            else:
                render_flink_topic(df, label)


def render_raw_feed(df):
    """Rich, hand-tuned view for the raw producer topic."""
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

    if "status" in df.columns:
        by_status = (
            df["status"].value_counts().rename_axis("status").reset_index(name="count")
        )
        fig_status = px.bar(
            by_status.sort_values("count"), x="count", y="status",
            orientation="h", title="Applications by Status",
        )
        fig_status.update_traces(marker_line_width=0)
        chart_col1.plotly_chart(style(fig_status), use_container_width=True)

    if "employment_type" in df.columns:
        by_emp = (
            df["employment_type"].value_counts()
            .rename_axis("employment_type").reset_index(name="count")
        )
        fig_emp = px.pie(
            by_emp, names="employment_type", values="count", hole=0.55,
            title="Employment Type Mix",
        )
        fig_emp.update_traces(marker=dict(line=dict(color=BG, width=2)))
        chart_col2.plotly_chart(style(fig_emp), use_container_width=True)

    chart_col3, chart_col4 = st.columns(2)

    if {"province", "tax_due"}.issubset(df.columns):
        by_province = (
            df.assign(tax_due=pd.to_numeric(df["tax_due"], errors="coerce"))
            .groupby("province", as_index=False)["tax_due"].mean()
            .sort_values("tax_due")
        )
        fig_prov = px.bar(
            by_province, x="tax_due", y="province", orientation="h",
            title="Avg Tax Due by Province",
        )
        fig_prov.update_traces(marker_line_width=0)
        chart_col3.plotly_chart(style(fig_prov), use_container_width=True)

    if "income" in df.columns:
        fig_income = px.histogram(
            df.assign(income=pd.to_numeric(df["income"], errors="coerce")),
            x="income", nbins=20, title="Income Distribution",
        )
        fig_income.update_traces(marker_line_width=0)
        chart_col4.plotly_chart(style(fig_income), use_container_width=True)

    if {"submitted_date", "tax_due"}.issubset(df.columns):
        trend = df.copy()
        trend["submitted_date"] = pd.to_datetime(trend["submitted_date"], errors="coerce")
        trend["tax_due"] = pd.to_numeric(trend["tax_due"], errors="coerce")
        trend = trend.dropna(subset=["submitted_date"]).sort_values("submitted_date")
        if not trend.empty:
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=trend["submitted_date"], y=trend["tax_due"],
                mode="lines+markers",
                line=dict(color=ACCENTS[0], width=2),
                marker=dict(size=5, color=ACCENTS[0]),
            ))
            fig_trend.update_layout(title="Tax Due Over Submitted Date")
            st.plotly_chart(style(fig_trend), use_container_width=True)

    st.subheader("Recent Events")
    sort_col = "submitted_date" if "submitted_date" in df.columns else "_received_at"
    st.dataframe(df.sort_values(sort_col, ascending=False).head(50), use_container_width=True)


def render_flink_topic(df, label):
    """Generic view for Flink-derived topics, whose exact schema we haven't
    confirmed yet. Shows KPIs + an auto-detected chart, plus the raw
    records so you can sanity-check the actual fields against what I've
    assumed. Once you confirm the real field names, tell me and I'll
    tailor a proper view per topic."""
    total = len(df)
    st.metric("Total records", total)

    numeric_cols = [
        c for c in df.columns
        if c not in ("_received_at",)
        and pd.to_numeric(df[c], errors="coerce").notna().any()
    ]
    id_like_suffixes = ("_id", "email", "_at", "raw_json", "name")
    categorical_cols = [
        c for c in df.columns
        if c not in numeric_cols
        and not any(c.lower().endswith(suf) or suf in c.lower() for suf in id_like_suffixes)
        and df[c].nunique() <= 30
    ]

    if categorical_cols and numeric_cols:
        cat_col, num_col = categorical_cols[0], numeric_cols[0]
        agg = (
            df.assign(**{num_col: pd.to_numeric(df[num_col], errors="coerce")})
            .groupby(cat_col, as_index=False)[num_col]
            .last()
            .sort_values(num_col)
        )
        fig = px.bar(
            agg, x=num_col, y=cat_col, orientation="h",
            title=f"{label}: {num_col} by {cat_col}",
        )
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(style(fig), use_container_width=True)
    elif numeric_cols:
        fig = px.histogram(
            df.assign(**{numeric_cols[0]: pd.to_numeric(df[numeric_cols[0]], errors="coerce")}),
            x=numeric_cols[0], nbins=20, title=f"{label}: {numeric_cols[0]} distribution",
        )
        st.plotly_chart(style(fig), use_container_width=True)
    else:
        st.info("No numeric/categorical columns detected yet to auto-chart.")

    st.subheader("Recent Records")
    sort_col = "_received_at"
    st.dataframe(df.sort_values(sort_col, ascending=False).head(50), use_container_width=True)


live_dashboard()

st.sidebar.divider()
for topic in ALL_TOPICS:
    n = len(st.session_state.buffers.get(topic, []))
    st.sidebar.caption(f"`{topic}`: {n} / {window_limit} buffered")

if st.sidebar.button("Clear all buffers"):
    st.session_state.buffers = {t: [] for t in ALL_TOPICS}
    st.session_state.topic_errors = {t: None for t in ALL_TOPICS}
    st.rerun()