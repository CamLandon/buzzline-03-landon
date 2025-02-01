#####################################
# Import Modules
#####################################

import os
import json
from collections import defaultdict
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    topic = os.getenv("DND_TOPIC", "dnd_encounters")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> int:
    group_id: str = os.getenv("DND_CONSUMER_GROUP_ID", "dnd_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Set up Data Store to hold encounter counts
#####################################

encounter_counts = defaultdict(int)

#####################################
# Function to process a single message
#####################################

def process_message(message: str) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        message_dict: dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            event = message_dict.get("event", "unknown_event")
            logger.info(f"Encounter received: {event}")

            encounter_counts[event] += 1
            logger.info(f"Updated encounter counts: {dict(encounter_counts)}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    logger.info("START DnD Encounter Consumer.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
