from confluent_kafka import Producer, Consumer
import sqlite3
import json
import random
import time
from datetime import datetime, timedelta

# ------------------------------------------------------
# Read Kafka Configuration
# ------------------------------------------------------

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()

            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()

    return config


# ------------------------------------------------------
# Sample Data
# ------------------------------------------------------

customers = [
    {"customer_id": "CUST-1001", "name": "John Nkosi"},
    {"customer_id": "CUST-1002", "name": "Sarah Johnson"},
    {"customer_id": "CUST-1003", "name": "Lerato Eva Brown"},
    {"customer_id": "CUST-1004", "name": "Emma Williams"},
    {"customer_id": "CUST-1005", "name": "Daniel Van Jones"},
    {"customer_id": "CUST-1006", "name": "Thabo Miller"},
    {"customer_id": "CUST-1007", "name": "David Wilson"},
    {"customer_id": "CUST-1008", "name": "Sophia Mvambo"},
    {"customer_id": "CUST-1009", "name": "James Kutlwano Anderson"},
    {"customer_id": "CUST-1010", "name": "Emily Mabatho Thomas"},
]

"""countries = [
    "South Africa",
    "United Kingdom",
    "United States",
    "Canada",
    "Australia",
    "Germany",
    "France",
    "India",
    "Brazil",
    "Japan"
]"""

provinces = [
    "Gauteng",
    "Free State",
    "KwaZulu-Natal",
    "Mpumalanga",
    "Western Cape",
    "Eastern Cape",
    "Limpopo",
    "North West",
    "Northern Cape"
]

statuses = [
    "Submitted",
    "Pending Review",
    "Under Audit",
    "Approved",
    "Rejected",
    "Processing",
    "Refund Issued",
    "Payment Outstanding"
]

employment_types = [
    "Full-Time",
    "Part-Time",
    "Self-Employed",
    "Contractor",
    "Retired"
]

#currencies = ["ZAR"]

application_counter = 10001


# ------------------------------------------------------
# Helper Functions
# ------------------------------------------------------

def random_date(year):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)

    delta = end - start

    random_days = random.randint(0, delta.days)

    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def generate_application():

    global application_counter

    customer = random.choice(customers)

    customer_id = customer["customer_id"]
    name = customer["name"]

    first_name = name.split()[0].lower()

    email = f"{first_name}.{customer_id.lower()}@example.com"

    country = "South Africa"
    
    province = random.choice(provinces)

    tax_year = random.randint(2010, 2025)

    income = random.randint(20000, 100000)

    tax_due = round(income * random.uniform(0.15,0.375), 2)

    status = random.choice(statuses)

    employment = random.choice(employment_types)

    currency = "ZAR"

    application_id = f"APP-{application_counter}"

    application_counter += 1

    submitted_date = random_date(tax_year)
    #event_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    application = {

        "application_id": application_id,

        "customer_id": customer_id,

        "customer_name": name,

        "email": email,

        "country": country,

        "province": province,

        "tax_year": tax_year,

        "income": income,

        "tax_due": tax_due,

        "currency": currency,

        "employment_type": employment,

        "status": status,

        "submitted_date": submitted_date

    }

    return application


# ------------------------------------------------------
# Producer
# ------------------------------------------------------

def produce(topic, config):

    producer = Producer(config)

    for _ in range(50):

        application = generate_application()

        key = application["customer_id"]

        value = json.dumps(application)

        producer.produce(
            topic,
            key=key,
            value=value
        )

        producer.poll(0)

        print(
            f"Produced -> "
            f"Application={application['application_id']} "
            f"Customer={application['customer_id']} "
            f"Status={application['status']} "
        )

        time.sleep(2)

    producer.flush()


# ------------------------------------------------------
# Consumer
# ------------------------------------------------------

def consume(topic, config):
    config["group.id"] = "tax_evaluation_group"
    config["auto.offset.reset"] = "earliest"

    consumer = Consumer(config)
    consumer.subscribe([topic])

    conn = sqlite3.connect("applications.db")
    cur = conn.cursor()

    create_sql = """
    CREATE TABLE IF NOT EXISTS applications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      application_id TEXT,
      customer_id TEXT,
      customer_name TEXT,
      email TEXT,
      country TEXT,
      province TEXT,
      tax_year INTEGER,
      income REAL,
      tax_due REAL,
      currency TEXT,
      employment_type TEXT,
      status TEXT,
      submitted_date TEXT,
      raw_json TEXT,
      received_at TEXT
    )
    """
    cur.execute(create_sql)
    conn.commit()

    insert_sql = """
    INSERT INTO applications
      (application_id, customer_id, customer_name, email, country, province, tax_year, income, tax_due, currency, employment_type, status, submitted_date, raw_json, received_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            raw = msg.value().decode("utf-8")
            try:
                value = json.loads(raw)
            except Exception as e:
                print("JSON decode error:", e)
                value = {}

            params = (
                value.get("application_id"),
                value.get("customer_id") or (msg.key().decode("utf-8") if msg.key() else None),
                value.get("customer_name"),
                value.get("email"),
                value.get("country"),
                value.get("province"),
                value.get("tax_year"),
                value.get("income"),
                value.get("tax_due"),
                value.get("currency"),
                value.get("employment_type"),
                value.get("status"),
                value.get("submitted_date"),
                raw,
                datetime.utcnow().isoformat() + "Z"
            )

            try:
                cur.execute(insert_sql, params)
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

    topic = "tax-evaluation-applications"

    produce(topic, config)

    consume(topic, config)


main()