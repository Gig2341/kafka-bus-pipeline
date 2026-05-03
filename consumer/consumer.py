import json
import os
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load env variables
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
INPUT_TOPIC = os.getenv("TOPIC_NAME")      # bus-events
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC")  # bus-delays

# Common Kafka config
common_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD
}

# Consumer config
consumer_conf = {
    **common_conf,
    'group.id': 'bus-delay-group',
    'auto.offset.reset': 'earliest'
}

# Producer config (to send processed data)
producer_conf = {
    **common_conf,
    'client.id': 'delay-producer'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe([INPUT_TOPIC])

DELAY_THRESHOLD = 5  # minutes

def delivery_report(err, msg):
    if err:
        print(f"❌ Failed to send: {err}")
    else:
        print(f"📤 Sent to {msg.topic()}")

print("🧠 Delay Detection Consumer Started...\n")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"⚠️ Error: {msg.error()}")
            continue

        # Parse message
        event = json.loads(msg.value().decode('utf-8'))

        delay = event.get("delay_minutes", 0)

        status = "ON_TIME"
        if delay > DELAY_THRESHOLD:
            status = "DELAYED"

        result = {
            "trip_id": event["trip_id"],
            "route_id": event["route_id"],
            "stop_name": event["stop_name"],
            "scheduled_arrival": event["scheduled_arrival"],
            "actual_arrival": event["actual_arrival"],
            "delay_minutes": delay,
            "status": status,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }

        # Print result (for demo)
        if status == "DELAYED":
            print(f"🚨 DELAY ALERT: {result}")
        else:
            print(f"✅ On time: {result['trip_id']} at {result['stop_name']}")

        # Send to output topic
        producer.produce(
            OUTPUT_TOPIC,
            key=str(result["trip_id"]),
            value=json.dumps(result),
            callback=delivery_report
        )

        producer.poll(0)

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")

finally:
    consumer.close()
    producer.flush()