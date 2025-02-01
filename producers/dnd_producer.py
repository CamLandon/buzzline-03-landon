import time
import pandas as pd
from confluent_kafka import Producer

# Kafka Configuration (update as needed)
kafka_config = {
    'bootstrap.servers': 'localhost:9092'  # Change this if your Kafka server is different
}

topic = 'dnd_events'
producer = Producer(kafka_config)

# Load CSV File
df = pd.read_csv('dnd.csv')

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Send Messages to Kafka
for _, row in df.iterrows():
    message = f"{row['timestamp']},{row['character_name']},{row['action']}"
    producer.produce(topic, key=row['character_name'], value=message, callback=delivery_report)
    time.sleep(0.5)  # Small delay to simulate real-time events

producer.flush()
print("All events have been sent!")
