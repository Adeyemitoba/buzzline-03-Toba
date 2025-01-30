# buzzline-03-Toba 

## Overview
`buzzline-03-Toba` focuses on real-time data streaming using Apache Kafka, handling JSON and CSV data formats with custom producers and consumers. The project includes separate setups for transportation updates and weather data analytics.

## Project Components

### Custom JSON Kafka Producer and Consumer

#### JSON Producer
- **Script**: `json_producer_Adeyemi.py`

  - Navigate to the producers folder.
  - Activate your virtual environment:
    - Windows: `.venv\Scripts\activate`
    - Mac/Linux: `source .venv/bin/activate`
  - Run the script:
    - Windows: `py -m producers.json_producer_Adeyemi`
    - Mac/Linux: `python3 -m producers.json_producer_Adeyemi`

#### JSON Consumer
- **Script**: `json_consumer_Adeyemi.py`

  - Navigate to the consumers folder.
  - Activate your virtual environment.
  - Run the script:
    - Windows: `py -m consumers.json_consumer_Adeyemi`
    - Mac/Linux: `python3 -m consumers.json_consumer_Adeyemi`

### Custom CSV Kafka Producer and Consumer

#### CSV Producer
- **Script**: `csv_producer_Adeyemi.py`

  - Navigate to the producers folder.
  - Activate your virtual environment.
  - Run the script:
    - Windows: `py -m producers.csv_producer_Adeyemi`
    - Mac/Linux: `python3 -m producers.csv_producer_Adeyemi`

#### CSV Consumer
- **Script**: `csv_consumer_Adeyemi.py`

  - Navigate to the consumers folder.
  - Activate your virtual environment.
  - Run the script:
    - Windows: `py -m consumers.csv_consumer_Adeyemi`
    - Mac/Linux: `python3 -m consumers.csv_consumer_Adeyemi`

## How to Run the Project
1. Start Zookeeper and Kafka as per the instructions in `SETUP-KAFKA.md`.
2. Activate your virtual environment:
   - Windows: `.venv\Scripts\activate`
   - Mac/Linux: `source .venv/bin/activate`
3. Run the producers and consumers in separate terminals:
   - Start the JSON producer and consumer.
   - Start the CSV producer and consumer.

## Real-Time Analytics
- **Transportation Services**: Analyzes the frequency and distribution of services like Uber and Lyft.
- **Weather Data**: Maintains a rolling window for weather metrics, detecting significant changes and trends.

## License
This project is licensed under the MIT License - see the LICENSE file for details. You are encouraged to fork, copy, explore, and modify the code as you like.

