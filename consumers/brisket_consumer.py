from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Change if needed
TOPIC = 'brisket_temp'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

previous_temp = None

# Consume messages
for message in consumer:
    data = message.value
    current_temp = data['temperature']
    timestamp = data['timestamp']
    
    if previous_temp is not None:
        if current_temp < previous_temp:
            alert = "Warning! Temperature is dropping"
        elif current_temp - previous_temp < 1:
            alert = "Cooking progress has slowed. May take longer to get to proper temperature."
        else:
            alert = "Brisket getting only perfectly."
    else:
        alert = "Starting temperature monitoring."
    
    print(f"Timestamp: {timestamp}, Temperature: {current_temp}°F, Alert: {alert}")
    
    previous_temp = current_temp
