from confluent_kafka import Producer
import pandas as pd
import random
import time

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'temperature_producer'
}

# Create a producer instance
producer = Producer(conf)

# Read the CSV file into a DataFrame
df = pd.read_csv('city_temp_30.csv')

# Function to send messages
def send_message(row):
    # Format the message
    message = f"The Temperature in {row['location']}: {row['temperature']}°F. Expect {row['weather']} later this week."
    
    # Send the message to the Kafka topic
    producer.produce('temperature_topic', value=message)
    producer.flush()

# Loop to send a random line every few seconds
try:
    while True:
        # Choose a random row from the DataFrame
        random_row = random.choice(df.itertuples(index=False))
        
        # Send the selected row as a message
        send_message(random_row)
        
        # Sleep for 3 seconds before sending the next message
        time.sleep(3)

except KeyboardInterrupt:
    print("Producer interrupted")
