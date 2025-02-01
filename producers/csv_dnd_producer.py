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
df = pd.read_csv(r"C:\Projects\buzzline-03-landon\data\dnd.csv")

print("Sending D&D events in order...")

# Send messages in order, looping back to the beginning
while True:
    for _, row in df.iterrows():
        message = f"{row['timestamp']},{row['character_name']},{row['action']}"
        producer.produce(topic, key=row['character_name'], value=message)
        producer.flush()
        print(f"Sent: {message}")
        time.sleep(3)  # Wait 3 seconds before sending the next message
