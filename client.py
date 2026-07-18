from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import sqlite3
import json
import random
import time
from datetime import datetime, timedelta, timezone

# ------------------------------------------------------
# Read Kafka Configuration
# ------------------------------------------------------

def read_config(path="client.properties"):
    config = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.split("=", 1)
                # Skip schema.registry.* settings — those are only used by
                # the dashboard to decode Avro from Flink-derived topics.
                # The plain Producer/Consumer here don't understand them.
                if parameter.startswith("schema.registry."):
                    continue
                config[parameter] = value.strip()
    return config


# ------------------------------------------------------
# Sample Source Data (siloed: CRM / Employment / Tax / Applications)
# ------------------------------------------------------

CUSTOMERS = [
    {"customer_id": "CUST-1001", "customer_name": "John Nkosi", "gender": "Male", "email": "john.cust-1001@example.com", "province": "Gauteng"},
    {"customer_id": "CUST-1002", "customer_name": "Sarah Johnson", "gender": "Female", "email": "sarah.cust-1002@example.com", "province": "Western Cape"},
    {"customer_id": "CUST-1003", "customer_name": "Lerato Eva Brown", "gender": "Female", "email": "lerato.cust-1003@example.com", "province": "KwaZulu-Natal"},
    {"customer_id": "CUST-1004", "customer_name": "Emma Williams", "gender": "Female", "email": "emma.cust-1004@example.com", "province": "Eastern Cape"},
    {"customer_id": "CUST-1005", "customer_name": "Daniel Van Jones", "gender": "Male", "email": "daniel.cust-1005@example.com", "province": "Free State"},
    {"customer_id": "CUST-1006", "customer_name": "Thabo Miller", "gender": "Male", "email": "thabo.cust-1006@example.com", "province": "Mpumalanga"},
    {"customer_id": "CUST-1007", "customer_name": "David Wilson", "gender": "Male", "email": "david.cust-1007@example.com", "province": "Limpopo"},
    {"customer_id": "CUST-1008", "customer_name": "Sophia Mvambo", "gender": "Female", "email": "sophia.cust-1008@example.com", "province": "Northern Cape"},
    {"customer_id": "CUST-1010", "customer_name": "Emily Mabatho Thomas", "gender": "Female", "email": "emily.cust-1010@example.com", "province": "Gauteng"},
]

EMPLOYMENT_TYPES = ["Full-Time", "Part-Time", "Self-Employed", "Contractor", "Retired"]
APPLICATION_STATUSES = ["Submitted", "Pending Review", "Under Audit", "Approved", "Rejected", "Processing", "Refund Issued", "Payment Outstanding"]

TOPICS = {
    "crm": "crm-customers",
    "employment": "employment-income-records",
    "tax": "tax-declaration-records",
    "applications": "tax-application-events",
}
# Reverse lookup: topic name -> friendly key, used by the consumer to route
# each incoming message to the right SQLite table.
TOPIC_TO_KEY = {v: k for k, v in TOPICS.items()}

application_counter = 10001


# ------------------------------------------------------
# Topic setup — Confluent Cloud does NOT auto-create topics on produce,
# so ensure they exist before we try to write to them. Idempotent: safe
# to call on every run.
# ------------------------------------------------------

def ensure_topics_exist(config, topics, num_partitions=6, replication_factor=3):
    admin_conf = {
        k: v for k, v in config.items()
        if k not in ("group.id", "auto.offset.reset")
    }
    admin_client = AdminClient(admin_conf)

    existing = set(admin_client.list_topics(timeout=10).topics.keys())
    missing = [t for t in topics if t not in existing]

    if not missing:
        print("All required topics already exist.")
        return

    print(f"Creating missing topics: {missing}")
    new_topics = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=replication_factor)
        for t in missing
    ]
    futures = admin_client.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"  Created topic: {topic}")
        except Exception as e:
            print(f"  Could not create topic '{topic}': {e}")
            print(
                "  If this is a permissions error, your Confluent Cloud API "
                "key needs the 'CloudClusterAdmin' role (or equivalent "
                "topic-create ACL), or create the topic manually in the "
                "Confluent Cloud UI: Cluster -> Topics -> Create topic."
            )


# ------------------------------------------------------
# Helper Functions
# ------------------------------------------------------

def now_utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def random_date(year):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")


def produce_json(producer, topic, key, value):
    producer.produce(topic, key=key, value=json.dumps(value))
    producer.poll(0)


def generate_crm_customer(customer):
    return {
        "customer_id": customer["customer_id"],
        "customer_name": customer["customer_name"],
        "gender": customer["gender"],
        "email": customer["email"],
        "country": "South Africa",
        "province": customer["province"],
        "crm_status": random.choice(["Active", "Active", "Active", "Dormant"]),
        "customer_segment": random.choice(["Individual", "Small Business", "High Net Worth"]),
        "updated_at": now_utc(),
    }


def generate_employment_income(customer_id, tax_year):
    employment_type = random.choice(EMPLOYMENT_TYPES)
    income = random.randint(20000, 100000)
    return {
        "customer_id": customer_id,
        "tax_year": tax_year,
        "employment_type": employment_type,
        "income": income,
        "income_source": random.choice(["Payroll", "Self Declared", "Employer Submission", "Manual Capture"]),
        "updated_at": now_utc(),
    }


def generate_tax_declaration(customer_id, tax_year, income):
    return {
        "customer_id": customer_id,
        "tax_year": tax_year,
        "tax_due": round(income * random.uniform(0.15, 0.375), 2),
        "currency": "ZAR",
        "declaration_channel": random.choice(["eFiling", "Branch", "Mobile App", "Call Centre"]),
        "updated_at": now_utc(),
    }


def generate_application_event(customer_id, tax_year):
    global application_counter
    application = {
        "application_id": f"APP-{application_counter}",
        "customer_id": customer_id,
        "tax_year": tax_year,
        "status": random.choice(APPLICATION_STATUSES),
        "submitted_date": random_date(tax_year),
        "event_time": now_utc(),
    }
    application_counter += 1
    return application


# ------------------------------------------------------
# Producer — siloed streams (CRM, employment, tax, applications)
# ------------------------------------------------------

def produce_siloed_streams(config, interval_seconds=2, total_events=100):
    producer = Producer(config)
    print("Producing CRM, employment, tax declaration, and application workflow events...")
    for _ in range(total_events):
        customer = random.choice(CUSTOMERS)
        customer_id = customer["customer_id"]
        tax_year = random.randint(2021, 2025)

        crm_event = generate_crm_customer(customer)
        employment_event = generate_employment_income(customer_id, tax_year)
        tax_event = generate_tax_declaration(customer_id, tax_year, employment_event["income"])
        app_event = generate_application_event(customer_id, tax_year)

        produce_json(producer, TOPICS["crm"], customer_id, crm_event)
        produce_json(producer, TOPICS["employment"], f"{customer_id}-{tax_year}", employment_event)
        produce_json(producer, TOPICS["tax"], f"{customer_id}-{tax_year}", tax_event)
        produce_json(producer, TOPICS["applications"], app_event["application_id"], app_event)

        print(
            f"Produced -> CRM={customer_id} | "
            f"Employment={customer_id}/{tax_year} | "
            f"TaxDue={tax_event['tax_due']} | "
            f"Application={app_event['application_id']} | "
            f"Status={app_event['status']}"
        )
        time.sleep(interval_seconds)

    producer.flush()
    print("Done producing siloed stream events.")


# ------------------------------------------------------
# Consumer — stores each siloed stream into its OWN SQLite table
# ------------------------------------------------------

CREATE_TABLE_SQL = {
    "crm": """
        CREATE TABLE IF NOT EXISTS crm_customers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          customer_id TEXT,
          customer_name TEXT,
          gender TEXT,
          email TEXT,
          country TEXT,
          province TEXT,
          crm_status TEXT,
          customer_segment TEXT,
          updated_at TEXT,
          raw_json TEXT,
          received_at TEXT
        )
    """,
    "employment": """
        CREATE TABLE IF NOT EXISTS employment_income_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          customer_id TEXT,
          tax_year INTEGER,
          employment_type TEXT,
          income REAL,
          income_source TEXT,
          updated_at TEXT,
          raw_json TEXT,
          received_at TEXT
        )
    """,
    "tax": """
        CREATE TABLE IF NOT EXISTS tax_declaration_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          customer_id TEXT,
          tax_year INTEGER,
          tax_due REAL,
          currency TEXT,
          declaration_channel TEXT,
          updated_at TEXT,
          raw_json TEXT,
          received_at TEXT
        )
    """,
    "applications": """
        CREATE TABLE IF NOT EXISTS tax_application_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          application_id TEXT,
          customer_id TEXT,
          tax_year INTEGER,
          status TEXT,
          submitted_date TEXT,
          event_time TEXT,
          raw_json TEXT,
          received_at TEXT
        )
    """,
}

INSERT_SQL = {
    "crm": """
        INSERT INTO crm_customers
          (customer_id, customer_name, gender, email, country, province, crm_status, customer_segment, updated_at, raw_json, received_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """,
    "employment": """
        INSERT INTO employment_income_records
          (customer_id, tax_year, employment_type, income, income_source, updated_at, raw_json, received_at)
        VALUES (?,?,?,?,?,?,?,?)
    """,
    "tax": """
        INSERT INTO tax_declaration_records
          (customer_id, tax_year, tax_due, currency, declaration_channel, updated_at, raw_json, received_at)
        VALUES (?,?,?,?,?,?,?,?)
    """,
    "applications": """
        INSERT INTO tax_application_events
          (application_id, customer_id, tax_year, status, submitted_date, event_time, raw_json, received_at)
        VALUES (?,?,?,?,?,?,?,?)
    """,
}


def params_for(key, value, raw, received_at):
    """Build the INSERT params tuple for a given stream key, using
    value.get(...) throughout so a missing/malformed field never crashes
    the consumer — it just stores NULL for that column."""
    if key == "crm":
        return (
            value.get("customer_id"), value.get("customer_name"), value.get("gender"),
            value.get("email"), value.get("country"), value.get("province"),
            value.get("crm_status"), value.get("customer_segment"), value.get("updated_at"),
            raw, received_at,
        )
    if key == "employment":
        return (
            value.get("customer_id"), value.get("tax_year"), value.get("employment_type"),
            value.get("income"), value.get("income_source"), value.get("updated_at"),
            raw, received_at,
        )
    if key == "tax":
        return (
            value.get("customer_id"), value.get("tax_year"), value.get("tax_due"),
            value.get("currency"), value.get("declaration_channel"), value.get("updated_at"),
            raw, received_at,
        )
    if key == "applications":
        return (
            value.get("application_id"), value.get("customer_id"), value.get("tax_year"),
            value.get("status"), value.get("submitted_date"), value.get("event_time"),
            raw, received_at,
        )
    raise ValueError(f"Unknown stream key: {key}")


def consume_siloed_streams(config, group_id="siloed_ingestion_group", db_path="applications.db"):
    conf = config.copy()
    conf["group.id"] = group_id
    conf["auto.offset.reset"] = "earliest"

    consumer = Consumer(conf)
    consumer.subscribe(list(TOPICS.values()))

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for sql in CREATE_TABLE_SQL.values():
        cur.execute(sql)
    conn.commit()

    print(f"Consuming {list(TOPICS.values())} into '{db_path}' "
          f"(crm_customers, employment_income_records, "
          f"tax_declaration_records, tax_application_events)...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            topic = msg.topic()
            key = TOPIC_TO_KEY.get(topic)
            if key is None:
                print(f"Skipping message from unexpected topic: {topic}")
                continue

            raw = msg.value().decode("utf-8")
            try:
                value = json.loads(raw)
            except Exception as e:
                print("JSON decode error:", e)
                continue

            received_at = now_utc()
            params = params_for(key, value, raw, received_at)

            try:
                cur.execute(INSERT_SQL[key], params)
                conn.commit()
            except Exception as e:
                print("DB insert error:", e)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


# ------------------------------------------------------
# Main
# ------------------------------------------------------

def main():
    config = read_config()

    ensure_topics_exist(config, list(TOPICS.values()))
    produce_siloed_streams(config, interval_seconds=2, total_events=100)
    consume_siloed_streams(config)


if __name__ == "__main__":
    main()