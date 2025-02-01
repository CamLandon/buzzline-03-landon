from kafka import KafkaProducer
import time
import pandas as pd
import json

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Change if using a different broker
TOPIC = 'brisket_temp'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load CSV file
df = pd.read_csv('brisket_temp_simulation.csv')

# Send each row as a Kafka message every 2 seconds
for index, row in df.iterrows():
    message = {
        'timestamp': row['Timestamp'],
        'temperature': row['Temperature']
    }
    producer.send(TOPIC, value=message)
    print(f"Sent: {message}")
    time.sleep(2)  # Wait 2 seconds before sending the next row

# Close the producer
producer.close()
