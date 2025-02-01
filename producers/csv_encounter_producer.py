#####################################
# Import Modules
#####################################

import os
import sys
import time
import pathlib
import csv
import json
from datetime import datetime
from dotenv import load_dotenv
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("DND_TOPIC", "dnd_encounters")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("DND_INTERVAL_SECONDS", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE = DATA_FOLDER.joinpath("dnd_encounters.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################

def generate_messages(file_path: pathlib.Path):
    while True:
        try:
            logger.info(f"Opening data file: {DATA_FILE}")
            with open(DATA_FILE, "r") as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    if "event" not in row or "location" not in row:
                        logger.error(f"Invalid row format: {row}")
                        continue
                    
                    message = {
                        "timestamp": row.get("timestamp", datetime.utcnow().isoformat()),
                        "location": row["location"],
                        "event": row["event"],
                        "difficulty": row.get("difficulty", "Unknown"),
                        "creatures": row.get("creatures", "None"),
                        "treasure": row.get("treasure", "None"),
                    }
                    logger.debug(f"Generated message: {message}")
                    yield message
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

#####################################
# Main Function
#####################################

def main():
    logger.info("START DnD Encounter Producer.")
    verify_services()
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message in generate_messages(DATA_FILE):
            producer.send(topic, value=message)
            logger.info(f"Sent message: {message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")
    
    logger.info("END DnD Encounter Producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
