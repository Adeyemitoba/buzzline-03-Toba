"""
json_producer_Adeyemi.py

Stream dynamically generated JSON data to a Kafka topic related to transportation updates.

Example JSON message:
{"ride_id": "12345", "service": "Uber", "status": "Completed", "duration": 15, "pickup_location": "Downtown", "dropoff_location": "Airport"}

Example serialized to Kafka message:
"{\"ride_id\": \"12345\", \"service\": \"Uber\", \"status\": \"Completed\", \"duration\": 15, \"pickup_location\": \"Downtown\", \"dropoff_location\": \"Airport\"}"
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import json  # work with JSON data
from dotenv import load_dotenv

# Import functions from local modules
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

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("TRANSPORT_TOPIC", "transportation_updates")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("TRANSPORT_INTERVAL_SECONDS", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Generate Dynamic Messages
#####################################

def generate_messages():
    """
    Generate sample transportation data messages dynamically.

    Yields:
        dict: A dictionary containing transportation data.
    """
    ride_id_counter = 1  # Start ride ID count

    while True:
        # Sample data to simulate Uber/Lyft rides
        message = {
            "ride_id": str(ride_id_counter),
            "service": "Uber",  # or "Lyft"
            "status": "Completed" if ride_id_counter % 2 == 0 else "In Progress",
            "duration": ride_id_counter * 10,  # Just an example duration in minutes
            "pickup_location": "Downtown",
            "dropoff_location": "Airport" if ride_id_counter % 2 == 0 else "Hotel",
        }
        logger.debug(f"Generated JSON: {message}")
        yield message

        ride_id_counter += 1
        time.sleep(2)  # Simulate message interval


#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams dynamically generated JSON messages to the Kafka topic.
    """

    logger.info("START producer.")
    verify_services()

    # Fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in generate_messages():
            # Send message directly as a dictionary (producer handles serialization)
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()