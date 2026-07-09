# Python Client

This project contains a Python 3 application that subscribes to a topic on a Confluent Cloud Kafka cluster and sends a sample message, then consumes it and prints the consumed record to the console.

## Prerequisites

We assume that you already have Python 3 installed. The template was last tested against Python 3.12.5.

The instructions use `virtualenv` but you may use other virtual environment managers like `venv` if you prefer.

```shell 
py -m venv venv   
```

## Installation

Create and activate a Python environment, so that you have an isolated workspace:

```shell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
venv\Scripts\activate.ps1
pip install -r requirements.txt
```

Install the dependencies of this application:

```shell
pip3 install -r requirements.txt
```

## Usage

You can execute the consumer script by running:

```shell
python3 client.py
```

## Troubleshooting

### Running `pip3 install -r requirements.txt` fails

If the execution of `pip3 install -r requirements.txt` fails with an error message indicating that librdkafka cannot be
found, please check if you are using a Python version for which a
[built distribution](https://pypi.org/project/confluent-kafka/2.3.0/#files) of `confluent-kafka` is available.


## Learn more

- For the Python client API, check out the [kafka-clients documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- Check out the full [getting started tutorial](https://developer.confluent.io/get-started/python/)

## Streamlit Dashboard

The Streamlit dashboard now consumes from **multiple** Kafka topics in real
time, using a single Kafka consumer subscribed to all of them at once:

- `tax-evaluation-applications` — the raw producer topic (rich KPI/chart view)
- The Flink SQL-derived topics from the Confluent Cloud pipeline:
  - `high_income_applications`
  - `status_summary`
  - `tax_year_summary_table`
  - `regional_summary_table`
  - `province_tax_revenue`
  - `income_by_employment`
  - `high_tax_due`
  - `employment_summary_table`

Each topic gets its own tab in the dashboard. The raw topic has a
hand-tuned view; the Flink-derived topics use an auto-detecting chart
(category vs. numeric column) until their exact schemas are confirmed and
tailored.

**Before running:** open `dashboard.py` and double-check the `FLINK_TOPICS`
dict against the actual topic names in Confluent Cloud — two names were
ambiguous from a truncated screenshot (`high_income_applications` and
`employment_summary_table`) and may need correcting.

**Also worth checking:** Confluent Cloud Flink SQL sink topics default to
Avro (with Schema Registry), not JSON. If a tab shows a warning about
non-UTF-8 payloads, add `WITH ('value.format' = 'json-registry')` to that
Flink `CREATE TABLE` statement, or switch to Avro deserialization in the
dashboard.

Install dependencies (if you haven't already):

```shell
pip install -r requirements.txt
```

Run the dashboard:

```shell
streamlit run dashboard.py
```

Notes:
- The dashboard reads Kafka configuration from `client.properties` locally,
  or `st.secrets["kafka"]` when deployed on Streamlit Community Cloud.
- It uses a single consumer with `auto.offset.reset=earliest` and
  `group.id=dashboard_consumer_group` by default, subscribed to all topics
  above.
- `client.py`'s consumer is unchanged — it still writes raw applications
  from `tax-evaluation-applications` into `applications.db` (SQLite) for
  historical storage, independent of what the dashboard displays.
- Restart Streamlit to clear the in-memory buffers, or change the consumer
  group id to re-read all topics from the earliest offset.