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

def get_traffic_delay(hour):
    """
    Simulates realistic city traffic conditions
    """

    #  Late night / early morning
    if 0 <= hour < 6:
        return random.randint(0, 1)

    #  Morning rush hour
    elif 6 <= hour < 10:
        return random.randint(2, 8)

    #  Midday traffic
    elif 10 <= hour < 16:
        return random.randint(1, 4)

    #  Evening rush hour
    elif 16 <= hour < 20:
        return random.randint(3, 10)

    #  Night traffic
    else:
        return random.randint(0, 3)
    


for _, row in df.iterrows():

    trip_id = row["trip_id"]

    # Initialize delay tracking
    if trip_id not in trip_delays:
        trip_delays[trip_id] = 0

    # ✅ Get scheduled time FIRST
    scheduled_time = row["scheduled_dt"]

    # ✅ Extract hour
    current_hour = scheduled_time.hour

    # ✅ Traffic-aware delay
    delay_increment = get_traffic_delay(current_hour)

    # 🚨 Random accident simulation
    if random.random() < 0.03:
        delay_increment += random.randint(10, 20)
        print("🚨 Accident causing heavy congestion!")

    # Add cumulative delay
    trip_delays[trip_id] += delay_increment

    # Optional recovery
    if random.random() < 0.15:
        trip_delays[trip_id] = max(0, trip_delays[trip_id] - 2)

    # Compute actual arrival
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

    sleep_time = random.uniform(1, 7)
    print(
        f"🚌 {trip_id} | "
        f"{scheduled_time.strftime('%H:%M:%S')} | "
        f"Delay: {trip_delays[trip_id]} mins | "
        f"Sleeping {sleep_time:.2f}s"
    )

    time.sleep(sleep_time)