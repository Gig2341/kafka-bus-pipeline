import pandas as pd
import json
import time
import random
import os
from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
TOPIC_NAME = os.getenv("TOPIC_NAME")

# Kafka config (Azure Event Hubs)
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
    'client.id': 'bus-producer'
}

producer = Producer(conf)

# Load dataset
df = pd.read_csv('../data/full_bus_schedule.csv')

# 🔧 Custom time parser (handles 24:xx:xx)
def parse_time(time_str):
    try:
        h, m, s = map(int, time_str.split(":"))

        extra_days = h // 24
        h = h % 24

        base_time = datetime(2024, 1, 1, h, m, s)
        return base_time + timedelta(days=extra_days)
    except:
        return None

# Apply parsing
df["scheduled_dt"] = df["arrival_time"].apply(parse_time)

# Remove bad rows
df = df.dropna(subset=["scheduled_dt"])

# 🔥 Global chronological ordering
df = df.sort_values(by="scheduled_dt")

# Track delay per trip
trip_delays = {}

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent to {msg.topic()}")

print("🚍 Starting Real-Time Bus Producer...\n")

for _, row in df.iterrows():

    trip_id = row["trip_id"]

    # Initialize delay for new trip
    if trip_id not in trip_delays:
        trip_delays[trip_id] = 0

    # Increment delay (simulate traffic)
    delay_increment = random.randint(0, 3)
    trip_delays[trip_id] += delay_increment

    scheduled_time = row["scheduled_dt"]
    actual_time = scheduled_time + timedelta(minutes=trip_delays[trip_id])

    event = {
        "trip_id": trip_id,
        "route_id": row["route_id"],
        "stop_id": row["stop_id"],
        "stop_name": row["stop_name"],
        "stop_sequence": int(row["stop_sequence"]),
        "scheduled_arrival": scheduled_time.strftime("%H:%M:%S"),
        "actual_arrival": actual_time.strftime("%H:%M:%S"),
        "delay_minutes": trip_delays[trip_id],
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.produce(
        TOPIC_NAME,
        key=str(trip_id),
        value=json.dumps(event),
        callback=delivery_report
    )

    producer.poll(0)

    # ⏳ Realistic streaming delay
    sleep_time = random.uniform(1, 7)
    print(f"⏳ Waiting {sleep_time:.2f}s...")
    time.sleep(sleep_time)

producer.flush()
print("\n🎉 Finished streaming all events!")