"""
csv_consumer_Adeyemi.py

Consume JSON messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2023-10-01T12:00:00Z", "temperature": 25.3, "humidity": 60, "pressure": 1013.2}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
from collections import deque  # For rolling window calculations

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("WEATHER_TOPIC", "weather_data_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("WEATHER_CONSUMER_GROUP_ID", "weather_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("WEATHER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Function to process a single message
#####################################


def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON-transferred CSV message and log weather data.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        timestamp = data.get("timestamp")
        temperature = data.get("temperature")
        humidity = data.get("humidity")
        pressure = data.get("pressure")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if None in (timestamp, temperature, humidity, pressure):
            logger.error(f"Invalid message format: {message}")
            return

        # Append the temperature reading to the rolling window
        rolling_window.append(temperature)

        # Log the weather data
        logger.info(
            f"Weather Data at {timestamp}: "
            f"Temperature={temperature}°C, Humidity={humidity}%, Pressure={pressure}hPa"
        )

        # Check for temperature stability (optional)
        if len(rolling_window) == window_size:
            temp_range = max(rolling_window) - min(rolling_window)
            logger.debug(f"Temperature range over last {window_size} readings: {temp_range}°C")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # Fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    # Initialize a rolling window for temperature readings
    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
