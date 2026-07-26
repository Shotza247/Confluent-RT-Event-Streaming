# Tax Applications — Streaming Analytics Pipeline

A real-time data pipeline simulating siloed enterprise source systems (CRM,
employment/payroll, tax declarations, application workflow) feeding into
Confluent Cloud, transformed with Flink SQL, and visualized in a live
Streamlit dashboard.

## Architecture

```
client.py (producer)
    |
    +--> crm_customers
    +--> employment_income_records
    +--> tax_declaration_records
    +--> tax_application_events
    |
    | (Flink SQL: dedup + join, see flink_sql/)
    v
application_analytics  ---> high_tax_due
    |                       (Flink filter)
    +--> customer_risk_profile
    |    (Flink per-customer rollup)
    v
dashboard.py (Streamlit)
    - consumes application_analytics + customer_risk_profile
    - 4 tabs: Overview / Tax & Income Analysis / Operations & Status / Customer Risk
    - global filters: status, tax year, employment type, province

client.py (siloed_ingestion_group consumer)
    - separately reads the same 4 raw topics back down into
      applications.db (SQLite) for local historical storage
```

The dashboard does **not** read the 4 raw siloed topics directly — it only
reads the Flink-derived `application_analytics` and `customer_risk_profile`
topics, which already carry every dimension needed (province, status,
employment_type, tax_year, gender, income, tax_due, effective_tax_rate,
is_high_tax, is_high_income).

## Prerequisites

Python 3.12+ (tested against 3.12.5). A Confluent Cloud cluster with:
- A Kafka cluster (bootstrap servers + SASL credentials)
- Schema Registry enabled (separate credentials from the Kafka cluster)
- Flink compute pool, with the SQL statements in `flink_sql/` run against it

## Installation

```shell
py -m venv venv
venv\Scripts\activate.ps1        # PowerShell; use venv\Scripts\activate.bat for cmd
pip install -r requirements.txt
```

### `client.properties`

Create this file in the project root (never commit it — see `.gitignore`):

```
bootstrap.servers=pkc-xxxxx.region.provider.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=YOUR_KAFKA_API_KEY
sasl.password=YOUR_KAFKA_API_SECRET
session.timeout.ms=45000
client.id=ccloud-python-client-your-id

# Schema Registry — separate key/secret pair, found under
# Environment -> Schema Registry -> API keys
schema.registry.url=https://psrc-xxxxx.region.provider.confluent.cloud
schema.registry.basic.auth.user.info=YOUR_SR_API_KEY:YOUR_SR_API_SECRET
```

For Streamlit Community Cloud deployment, use `secrets.toml.example` as a
template instead (quoted dotted keys, TOML `[kafka]` table).

## Running the pipeline

**1. Produce + store raw siloed data:**

```shell
python client.py
```

This does three things in order:
- `ensure_topics_exist()` — auto-creates the 4 raw topics if missing (idempotent)
- Produces 100 simulated events across `crm_customers`, `employment_income_records`,
  `tax_declaration_records`, `tax_application_events`
- Consumes those same 4 topics back down into `applications.db` (SQLite),
  one table per stream, for local historical storage

**2. Run the Flink SQL** (in Confluent Cloud's Flink SQL workspace, in order):

See `flink_sql/00_full_pipeline.sql` for the combined script, or run
individually:
1. `01_crm_latest_view.sql` — dedup view (CRM stream is append-only, not upsert)
2. `02_application_analytics.sql` — the wide joined fact table, with
   `is_high_tax` (effective tax rate > 27%) and `is_high_income`
   (income >= 95000) flags baked in
3. `03_high_tax_due.sql` / `04_high_income_applications.sql` — trivial filters

Also run the `customer_risk_profile` aggregation (per-customer rollup) —
see the state-TTL note below before running it.

**Important:** verify each output topic's registered schema type in
Schema Registry's **Data Contracts** page (Confluent Cloud sometimes
registers JSON Schema for one topic and Avro for another, seemingly
inconsistently) — a mismatch here causes
`Error while deserializing value... declared value schema doesn't match
your records` in Flink, not a data problem.

**3. Run the dashboard:**

```shell
streamlit run dashboard.py
```

## Dashboard notes

- **Consumer group:** stable `dashboard_group` by default — does *not*
  auto-generate a new group every session (that was an earlier design that
  cluttered Confluent Cloud's consumer group list). Use the sidebar
  **"Reset & re-read from earliest"** button only when you deliberately
  want to replay all data from the start.
- **Filters:** status, tax year, employment type, and province multiselects
  apply across the Overview, Tax & Income, and Operations tabs.
- **Colors:** province, status, employment type, and gender each have a
  fixed color mapping, consistent across every chart.
- **Client IDs:** each component (producer, siloed-ingestion consumer,
  dashboard consumer) appends its own suffix to the shared `client.id` from
  `client.properties`, so they're distinguishable in Confluent Cloud's UI
  (Cluster -> Consumers) instead of all showing the identical ID.

## Known design notes / things to revisit

- `employment_income_records` and `tax_declaration_records` are, like
  `crm_customers`, append-only streams (not upsert) — the same
  "latest value per key" dedup pattern used for CRM should be applied to
  these two before joining, to avoid duplicate rows fanning out in
  `application_analytics` as data volume grows.
- `customer_risk_profile`'s `MAX(effective_tax_rate)` aggregation needs
  `SET 'sql.state-ttl' = '2 days';` (or similar) to avoid Confluent's
  `STATE_INTENSIVE_AGG_FUNCTION` warning — but note this means truly
  "lifetime" metrics silently reset if a customer goes inactive longer
  than the TTL. Fine for this demo's volume; worth a proper fix (e.g.
  computing true lifetime stats from the historical store instead of
  live Flink state) if this becomes a real system.
- Longer-term architecture idea under consideration: swap SQLite for
  PostgreSQL and use a Confluent Cloud CDC source connector instead of
  `client.py`'s own consumer — Postgres becomes the system of record,
  Kafka topics are derived via CDC automatically, and
  `siloed_ingestion_group` goes away entirely. Also enables proper
  upsert semantics (via `INSERT ... ON CONFLICT`), which would let Flink
  handle "latest value" natively instead of via `ROW_NUMBER()` dedup views.
- Planned downstream addition: an ML fraud/anomaly-detection model on top
  of `application_analytics` / `customer_risk_profile`, once the dashboard
  and Flink layer are stable.

## Troubleshooting

### `pip install -r requirements.txt` fails on `confluent-kafka`

Check you're using a Python version with a
[built distribution](https://pypi.org/project/confluent-kafka/2.3.0/#files)
available; building from source needs `librdkafka`.

### `KafkaException: Unknown topic`

The 4 raw topics don't exist yet in Confluent Cloud — `ensure_topics_exist()`
in `client.py` should create them automatically. If it fails with a
permissions error, your API key needs the `CloudClusterAdmin` role (or
create the topics manually: Cluster -> Topics -> Create topic).

### Dashboard shows "No Schema Registry configured"

Add the two `schema.registry.*` lines to `client.properties` (see above).

### Flink `SELECT * FROM <topic>` fails with a deserialization error

Check the topic's registered schema type in Schema Registry's Data
Contracts page — if it says Avro but the data is actually plain JSON (or
vice versa), delete that schema subject and re-infer it, explicitly
choosing the correct format this time.

## Learn more

- [confluent-kafka Python client docs](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [Confluent getting started tutorial](https://developer.confluent.io/get-started/python/)