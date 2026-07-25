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

# Streamlit dashboard consuming the two Flink-derived analytics topics that
# now carry everything the old 9-topic setup was split across:
#   - application_analytics: row-level, every dimension (province, status,
#     employment_type, tax_year, gender, income, tax_due,
#     effective_tax_rate, is_high_tax, is_high_income)
#   - customer_risk_profile: per-customer rollup

CONFIG_PATH = "client.properties"
ANALYTICS_TOPIC = "application_analytics"
RISK_TOPIC = "customer_risk_profile"
ALL_TOPICS = [ANALYTICS_TOPIC, RISK_TOPIC]

st.set_page_config(page_title="Tax Applications Analytics", layout="wide")

# ---- Palette ----
BG = "#0F172A"
GRID = "#334155"
TEXT = "#F8FAFC"

CHART_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=BG,
    font=dict(color=TEXT, family="Inter, sans-serif"),
    margin=dict(l=30, r=20, t=40, b=30),
    xaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
    yaxis=dict(gridcolor=GRID, zerolinecolor=GRID),
)


def style(fig):
    fig.update_layout(**CHART_LAYOUT)
    return fig


# ---- Fixed, consistent color maps per categorical dimension ----
# Same province/status/etc always renders in the same color, everywhere.
PROVINCE_COLORS = {
    "Gauteng": "#EAB308",         # gold
    "Free State": "#F43F5E",      # red
    "KwaZulu-Natal": "#38BDF8",   # blue
    "Limpopo": "#22C55E",         # green
    "Western Cape": "#A855F7",    # violet
    "Eastern Cape": "#F97316",    # orange
    "Mpumalanga": "#14B8A6",      # teal
    "North West": "#EC4899",      # pink
    "Northern Cape": "#94A3B8",   # slate/grey
}

STATUS_COLORS = {
    "Submitted": "#38BDF8",
    "Pending Review": "#EAB308",
    "Under Audit": "#F97316",
    "Approved": "#22C55E",
    "Rejected": "#F43F5E",
    "Processing": "#A855F7",
    "Refund Issued": "#14B8A6",
    "Payment Outstanding": "#EC4899",
}

EMPLOYMENT_COLORS = {
    "Full-Time": "#38BDF8",
    "Part-Time": "#EAB308",
    "Self-Employed": "#22C55E",
    "Contractor": "#A855F7",
    "Retired": "#94A3B8",
}

GENDER_COLORS = {
    "Male": "#38BDF8",
    "Female": "#EC4899",
}

QUADRANT_COLORS = {
    "High Income & High Tax": "#22C55E",
    "High Income Only": "#EAB308",
    "High Tax Only": "#F43F5E",
    "Baseline": "#334155",
}


@st.cache_resource
def load_config(path=CONFIG_PATH):
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
    conf = _kafka_conf.copy()
    conf["group.id"] = group_id
    conf.setdefault("auto.offset.reset", "earliest")
    consumer = Consumer(conf)
    consumer.subscribe(list(topics))
    return consumer


@st.cache_resource
def get_avro_deserializer(_sr_conf):
    if not _sr_conf.get("url"):
        return None
    client = SchemaRegistryClient(_sr_conf)
    return AvroDeserializer(client, schema_str=None)


def safe_deserialize(msg, avro_deserializer):
    try:
        key = msg.key().decode("utf-8") if msg.key() else None
    except UnicodeDecodeError:
        key = None

    raw_value = msg.value()

    try:
        value = json.loads(raw_value.decode("utf-8"))
        return value, key, None
    except UnicodeDecodeError:
        pass
    except json.JSONDecodeError as e:
        return None, key, f"JSON decode error: {e}"

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
        "schema.registry.basic.auth.user.info to client.properties."
    )


def assign_quadrant(row):
    hi_income = row.get("is_high_income") == 1
    hi_tax = row.get("is_high_tax") == 1
    if hi_income and hi_tax:
        return "High Income & High Tax"
    if hi_income:
        return "High Income Only"
    if hi_tax:
        return "High Tax Only"
    return "Baseline"


st.title("Tax Applications — Analytics Dashboard")

st.sidebar.header("Settings")
refresh_seconds = st.sidebar.number_input("Auto-refresh interval (seconds)", min_value=1, value=3)
poll_batch_size = st.sidebar.number_input("Max messages to drain per refresh", min_value=1, value=100)
window_limit = st.sidebar.number_input("Messages to retain per topic", min_value=50, value=2000)

# --- Consumer group: ONE stable group by default. Resetting is a
# deliberate button press, not something that happens automatically every
# session (that auto-fresh-group behavior was the original clutter cause).
if "group_override" not in st.session_state:
    st.session_state.group_override = None

base_group_id = st.sidebar.text_input("Consumer group id", value="dashboard_group")
if st.sidebar.button("Reset & re-read from earliest"):
    st.session_state.group_override = f"{base_group_id}_reset_{int(time.time())}"
    st.rerun()

group_id = st.session_state.group_override or base_group_id

config = load_config()
kafka_conf, sr_conf = split_kafka_and_schema_registry_config(config)
consumer = get_consumer(kafka_conf, group_id, ALL_TOPICS)
avro_deserializer = get_avro_deserializer(sr_conf)

st.sidebar.write(f"Active group id: `{group_id}`")
st.sidebar.caption(
    "✅ Schema Registry configured" if avro_deserializer is not None
    else "⚠️ No Schema Registry configured — Avro topics will show a decode warning."
)

if "buffers" not in st.session_state:
    st.session_state.buffers = {t: [] for t in ALL_TOPICS}
if "topic_errors" not in st.session_state:
    st.session_state.topic_errors = {t: None for t in ALL_TOPICS}


@st.fragment(run_every=refresh_seconds)
def live_dashboard():
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
        value["_received_at"] = datetime.utcnow().isoformat()
        st.session_state.buffers.setdefault(topic, []).append(value)

        buf = st.session_state.buffers[topic]
        if len(buf) > window_limit:
            st.session_state.buffers[topic] = buf[-window_limit:]

    for topic in ALL_TOPICS:
        err = st.session_state.topic_errors.get(topic)
        if err:
            st.warning(f"`{topic}`: {err}")

    df_analytics = pd.DataFrame(st.session_state.buffers.get(ANALYTICS_TOPIC, []))
    df_risk = pd.DataFrame(st.session_state.buffers.get(RISK_TOPIC, []))

    if df_analytics.empty:
        st.write("No data yet on `application_analytics` — waiting for messages.")
        return

    # --- Global filters, sourced from whatever values have shown up so far ---
    with st.expander("Filters", expanded=True):
        fcol1, fcol2, fcol3, fcol4 = st.columns(4)
        status_opts = sorted(df_analytics["status"].dropna().unique().tolist()) if "status" in df_analytics else []
        year_opts = sorted(df_analytics["tax_year"].dropna().unique().tolist()) if "tax_year" in df_analytics else []
        emp_opts = sorted(df_analytics["employment_type"].dropna().unique().tolist()) if "employment_type" in df_analytics else []
        prov_opts = sorted(df_analytics["province"].dropna().unique().tolist()) if "province" in df_analytics else []

        sel_status = fcol1.multiselect("Status", status_opts, default=[])
        sel_year = fcol2.multiselect("Tax Year", year_opts, default=[])
        sel_emp = fcol3.multiselect("Employment Type", emp_opts, default=[])
        sel_prov = fcol4.multiselect("Province", prov_opts, default=[])

    df = df_analytics.copy()
    if sel_status:
        df = df[df["status"].isin(sel_status)]
    if sel_year:
        df = df[df["tax_year"].isin(sel_year)]
    if sel_emp:
        df = df[df["employment_type"].isin(sel_emp)]
    if sel_prov:
        df = df[df["province"].isin(sel_prov)]

    if df.empty:
        st.info("No records match the current filters.")
        return

    tab_overview, tab_tax, tab_ops, tab_risk = st.tabs(
        ["Overview", "Tax & Income Analysis", "Operations & Status", "Customer Risk"]
    )

    with tab_overview:
        render_overview(df)
    with tab_tax:
        render_tax_income(df)
    with tab_ops:
        render_operations(df)
    with tab_risk:
        render_customer_risk(df_risk)


def render_overview(df):
    total = len(df)
    total_tax = pd.to_numeric(df.get("tax_due"), errors="coerce").sum()
    avg_income = pd.to_numeric(df.get("income"), errors="coerce").mean()
    n_customers = df["customer_id"].nunique() if "customer_id" in df.columns else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Applications", total)
    c2.metric("Total tax due", f"R{total_tax:,.0f}" if pd.notna(total_tax) else "—")
    c3.metric("Avg income", f"R{avg_income:,.0f}" if pd.notna(avg_income) else "—")
    c4.metric("Distinct customers", n_customers)

    col1, col2 = st.columns(2)
    if "status" in df.columns:
        by_status = df["status"].value_counts().rename_axis("status").reset_index(name="count")
        fig = px.bar(
            by_status.sort_values("count"), x="count", y="status", orientation="h",
            title="Applications by Status", color="status",
            color_discrete_map=STATUS_COLORS,
        )
        fig.update_traces(marker_line_width=0)
        col1.plotly_chart(style(fig), use_container_width=True)

    if "province" in df.columns:
        by_prov = df["province"].value_counts().rename_axis("province").reset_index(name="count")
        fig = px.bar(
            by_prov.sort_values("count"), x="count", y="province", orientation="h",
            title="Applications by Province", color="province",
            color_discrete_map=PROVINCE_COLORS,
        )
        fig.update_traces(marker_line_width=0)
        col2.plotly_chart(style(fig), use_container_width=True)

    st.subheader("Recent Applications")
    sort_col = "submitted_date" if "submitted_date" in df.columns else "_received_at"
    st.dataframe(df.sort_values(sort_col, ascending=False).head(50), use_container_width=True)


def render_tax_income(df):
    d = df.copy()
    d["income"] = pd.to_numeric(d.get("income"), errors="coerce")
    d["tax_due"] = pd.to_numeric(d.get("tax_due"), errors="coerce")
    d["effective_tax_rate"] = pd.to_numeric(d.get("effective_tax_rate"), errors="coerce")

    st.caption(
        "Quadrant view: is high income paired with high tax (expected, "
        "progressive tax working) or not (worth a closer look)?"
    )
    if {"income", "effective_tax_rate"}.issubset(d.columns):
        d["quadrant"] = d.apply(assign_quadrant, axis=1)
        fig = px.scatter(
            d, x="income", y="effective_tax_rate", color="quadrant",
            color_discrete_map=QUADRANT_COLORS,
            hover_data=["application_id", "customer_id", "province", "employment_type"],
            title="Income vs Effective Tax Rate",
        )
        fig.add_hline(y=0.27, line_dash="dash", line_color=GRID, annotation_text="27% threshold")
        fig.add_vline(x=95000, line_dash="dash", line_color=GRID, annotation_text="R95k threshold")
        st.plotly_chart(style(fig), use_container_width=True)

    col1, col2 = st.columns(2)
    if {"province", "tax_due"}.issubset(d.columns):
        by_prov = d.groupby("province", as_index=False)["tax_due"].sum().sort_values("tax_due")
        fig = px.bar(
            by_prov, x="tax_due", y="province", orientation="h",
            title="Total Tax Due by Province", color="province",
            color_discrete_map=PROVINCE_COLORS,
        )
        fig.update_traces(marker_line_width=0)
        col1.plotly_chart(style(fig), use_container_width=True)

    if {"employment_type", "income"}.issubset(d.columns):
        fig = px.box(
            d, x="employment_type", y="income", color="employment_type",
            color_discrete_map=EMPLOYMENT_COLORS,
            title="Income Distribution by Employment Type",
        )
        col2.plotly_chart(style(fig), use_container_width=True)


def render_operations(df):
    d = df.copy()
    col1, col2 = st.columns(2)

    if {"province", "status"}.issubset(d.columns):
        pivot = d.groupby(["province", "status"]).size().reset_index(name="count")
        fig = px.bar(
            pivot, x="province", y="count", color="status",
            color_discrete_map=STATUS_COLORS,
            title="Status Breakdown by Province", barmode="stack",
        )
        col1.plotly_chart(style(fig), use_container_width=True)

    if "gender" in d.columns:
        by_gender = d["gender"].value_counts().rename_axis("gender").reset_index(name="count")
        fig = px.pie(
            by_gender, names="gender", values="count", hole=0.55,
            color="gender", color_discrete_map=GENDER_COLORS,
            title="Applicant Gender Mix",
        )
        fig.update_traces(marker=dict(line=dict(color=BG, width=2)))
        col2.plotly_chart(style(fig), use_container_width=True)

    if {"submitted_date", "application_id"}.issubset(d.columns):
        trend = d.copy()
        trend["submitted_date"] = pd.to_datetime(trend["submitted_date"], errors="coerce")
        trend = trend.dropna(subset=["submitted_date"])
        daily = trend.groupby(trend["submitted_date"].dt.date).size().reset_index(name="count")
        daily.columns = ["date", "count"]
        if not daily.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["count"], mode="lines+markers"))
            fig.update_layout(title="Applications Submitted Over Time")
            st.plotly_chart(style(fig), use_container_width=True)


def render_customer_risk(df_risk):
    if df_risk.empty:
        st.write("No data yet on `customer_risk_profile` — waiting for messages.")
        return

    d = df_risk.copy()
    for col in ["total_applications", "high_tax_count", "high_income_count",
                "avg_effective_tax_rate", "max_effective_tax_rate", "lifetime_tax_paid"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    # customer_risk_profile is keyed by customer_id with continuous
    # updates — keep only the latest snapshot per customer for display.
    if "_received_at" in d.columns and "customer_id" in d.columns:
        d = d.sort_values("_received_at").drop_duplicates("customer_id", keep="last")

    col1, col2 = st.columns(2)
    if {"customer_id", "lifetime_tax_paid"}.issubset(d.columns):
        top = d.sort_values("lifetime_tax_paid", ascending=False).head(15)
        fig = px.bar(
            top.sort_values("lifetime_tax_paid"), x="lifetime_tax_paid", y="customer_id",
            orientation="h", title="Top Customers by Lifetime Tax Paid",
        )
        col1.plotly_chart(style(fig), use_container_width=True)

    if {"total_applications", "high_tax_count", "avg_effective_tax_rate"}.issubset(d.columns):
        fig = px.scatter(
            d, x="total_applications", y="high_tax_count",
            color="avg_effective_tax_rate", color_continuous_scale="Viridis",
            hover_data=["customer_id"],
            title="Application Volume vs High-Tax Frequency",
        )
        col2.plotly_chart(style(fig), use_container_width=True)

    st.subheader("Customer Risk Table")
    st.dataframe(d, use_container_width=True)


live_dashboard()

st.sidebar.divider()
for topic in ALL_TOPICS:
    n = len(st.session_state.buffers.get(topic, []))
    st.sidebar.caption(f"`{topic}`: {n} / {window_limit} buffered")

if st.sidebar.button("Clear all buffers"):
    st.session_state.buffers = {t: [] for t in ALL_TOPICS}
    st.session_state.topic_errors = {t: None for t in ALL_TOPICS}
    st.rerun()