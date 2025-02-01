from confluent_kafka import Consumer

# Kafka Configuration (update as needed)
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dnd_group',
    'auto.offset.reset': 'earliest'
}

topic = 'dnd_events'
consumer = Consumer(kafka_config)
consumer.subscribe([topic])

# Action check mapping
checks = {
    "Dodge": "Roll a Dexterity check.",
    "Sneak": "Roll a Dexterity check.",
    "Dash": "Roll a Dexterity check.",
    "Attack": "Roll a Strength check.",
    "Defend": "Roll a Strength check.",
    "Cast Spell": "Roll a Magic check.",
    "Heal": "Roll a Magic check.",
    "Investigate": "Roll a Wisdom check.",
    "Persuade": "Roll a Charisma check."
}

print("Listening for D&D events...")

while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    
    # Parse message (CSV format: timestamp,character_name,action)
    try:
        _, character_name, action = msg.value().decode('utf-8').split(',')
        response = f"{character_name} chose to {action}!"
        
        # Add corresponding check message
        if action in checks:
            response += f" {checks[action]}"
        
        print(response)
    except Exception as e:
        print(f"Error processing message: {e}")

consumer.close()
