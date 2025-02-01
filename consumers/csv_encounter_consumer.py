#####################################
# Import Modules
#####################################

import os
import json
from collections import defaultdict, deque
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

def get_danger_threshold() -> int:
    return int(os.getenv("DND_DANGER_THRESHOLD", 3))  # Number of consecutive hard/deadly encounters

#####################################
# Set up Data Stores
#####################################

dangerous_encounters = deque(maxlen=get_danger_threshold())  # Tracks last N encounters
monster_counts = defaultdict(int)  # Tracks number of times each monster appears
collected_treasure = defaultdict(int)  # Tracks amount of different treasures collected

def process_message(message: str) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        message_dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            event = message_dict.get("event", "Unknown Event")
            location = message_dict.get("location", "Unknown Location")
            difficulty = message_dict.get("difficulty", "Unknown Difficulty").lower()
            creatures = message_dict.get("creatures", "None").split(",")
            treasure = message_dict.get("treasure", "None").split(",")

            logger.info(f"Encounter: {event} at {location} (Difficulty: {difficulty.capitalize()})")
            
            # Danger Level Detector
            if difficulty in ["hard", "deadly"]:
                dangerous_encounters.append(difficulty)
                if len(dangerous_encounters) >= get_danger_threshold():
                    logger.warning(f"WARNING! {get_danger_threshold()} tough encounters in a row! Adventurers should be cautious!")
            
            # Monster Popularity Counter
            for monster in creatures:
                monster = monster.strip()
                if monster:
                    monster_counts[monster] += 1
            logger.info(f"Monster counts so far: {dict(monster_counts)}")
            
            # Treasure Collector
            for item in treasure:
                item = item.strip()
                if item:
                    collected_treasure[item] += 1
            logger.info(f"Treasure collected so far: {dict(collected_treasure)}")
        
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
