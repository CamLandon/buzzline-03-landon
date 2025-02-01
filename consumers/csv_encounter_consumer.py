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

def get_kafka_topic() -> str:
    topic = os.getenv("DND_TOPIC", "dnd_encounters")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("DND_CONSUMER_GROUP_ID", "dnd_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Set up Data Store to Track Encounters
#####################################

encounter_counts = defaultdict(int)

def process_message(message: str) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        message_dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            event = message_dict.get("event", "Unknown Event")
            location = message_dict.get("location", "Unknown Location")
            difficulty = message_dict.get("difficulty", "Unknown Difficulty")
            creatures = message_dict.get("creatures", "None")
            treasure = message_dict.get("treasure", "None")

            logger.info(f"Encounter Detected: {event} at {location}")
            logger.info(f"Difficulty: {difficulty} | Creatures: {creatures} | Treasure: {treasure}")

            encounter_counts[event] += 1
            logger.info(f"Updated encounter counts: {dict(encounter_counts)}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function
#####################################

def main() -> None:
    logger.info("START DnD Encounter Consumer.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

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

    logger.info("END DnD Encounter Consumer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
