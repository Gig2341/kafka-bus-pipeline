import streamlit as st
import json
import os
import pandas as pd
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Load env
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
TOPIC = os.getenv("OUTPUT_TOPIC")

# Kafka config
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
    'group.id': 'dashboard-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# Streamlit UI
st.set_page_config(page_title="Bus Delay Dashboard", layout="wide")

st.title("🚍 Real-Time Bus Delay Monitoring")

# Placeholder containers
delay_box = st.empty()
table_box = st.empty()

data = []

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        continue

    event = json.loads(msg.value().decode('utf-8'))
    data.append(event)

    df = pd.DataFrame(data)

    # Metrics
    total = len(df)
    delayed = len(df[df["status"] == "DELAYED"])
    on_time = len(df[df["status"] == "ON_TIME"])

    with delay_box.container():
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", total)
        col2.metric("Delayed Buses 🚨", delayed)
        col3.metric("On Time ✅", on_time)

    # Show latest data
    with table_box.container():
        st.subheader("Live Bus Status")
        st.dataframe(df.tail(20), use_container_width=True)