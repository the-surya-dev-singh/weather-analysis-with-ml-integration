import mysql.connector
import os
from dotenv import load_dotenv
from confluent_kafka import Producer
import json
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()


def batch_producer(bootstrap_servers='localhost:9092', topic='weather_data_batch', batch_size=5):
    try:
        mysql_config = {
        'user': 'root',  # Default username for XAMPP MySQL
        'password': '',  # Default password is empty (unless you set one in XAMPP)
        'host': 'localhost',
        'port': '3306',  # Default MySQL port
        'database': 'weather',  # Replace with your database name
        'raise_on_warnings': True
        }

        # Configure the Kafka producer
        kafka_conf = {'bootstrap.servers': bootstrap_servers}
        producer = Producer(kafka_conf)

        # Configure MySQL connection
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(buffered=True)

        # Fetch data from MySQL in batches
        mysql_cursor.execute("SELECT * FROM weather.weather_data")
        rows = mysql_cursor.fetchmany(batch_size)

        while rows:
            # Prepare batch data as a list of dictionaries
            batch_data = []
            for row in rows:
                data = {
                    'Formatted Date': row[0],
                    'Summary': row[1],
                    'Precip Type': row[2],
                    'Temperature (C)': row[3],
                    'Apparent Temperature (C)': row[4],
                    'Humidity': row[5],
                    'Wind Speed (km/h)': row[6],
                    'Wind Bearing (degrees)': row[7],
                    'Visibility (km)': row[8],
                    'Loud Cover': row[9],
                    'Pressure (millibars)': row[10],
                    'Daily Summary': row[11]
                }
                batch_data.append(data)

            # Send batch data to Kafka as JSON
            # Note: This sends the entire batch as a single message
            producer.produce(topic, value=json.dumps(batch_data).encode('utf-8'))
            producer.flush()
            logging.info(f"Batch of {len(batch_data)} rows sent to Kafka topic '{topic}'")

            # Fetch the next batch
            rows = mysql_cursor.fetchmany(batch_size)
            time.sleep(10)

        # Close connections
        mysql_cursor.close()
        mysql_conn.close()

        # Close Kafka producer
        producer.flush()
        logging.info("Producer closed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        # Attempt to close connections and producer in case of an error
        try:
            mysql_cursor.close()
            mysql_conn.close()
            producer.flush()
        except Exception as e:
            logging.error(f"Failed to close connections or producer: {e}")

# Example usage
batch_producer()
