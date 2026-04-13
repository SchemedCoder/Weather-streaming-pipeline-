from snowflake.snowpark import Session
from kafka import KafkaConsumer
import json
import os
import time
from dotenv import load_dotenv

# Load credentials
load_dotenv()

# Snowflake Connection Parameters
connection_parameters = {
    "account": os.getenv("SF_ACCOUNT"),
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": os.getenv("SF_DATABASE"),
    "schema": "BRONZE"
}

def start_bridge():
    while True:
        try:
            print("--- Initializing Snowflake Bridge ---")
            
            # Connect to Snowflake
            session = Session.builder.configs(connection_parameters).create()
            print("Connected to Snowflake.")

            # Connect to Kafka
            consumer = KafkaConsumer(
                'weather-data',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # Add a short timeout so the loop can check for connection issues
                consumer_timeout_ms=10000 
            )
            print("Connected to Kafka. Listening for weather data...")

            # The Ingestion Loop
            for message in consumer:
                city_data = message.value
                raw_json = json.dumps(city_data)
                
                # Insert into the VARIANT column in Bronze
                session.sql(f"INSERT INTO RAW_WEATHER (raw_content) SELECT PARSE_JSON('{raw_json}')").collect()
                print(f"Ingested to Snowflake: {city_data['city']}")

        except Exception as e:
            print(f"\n[ERROR] Connection lost or Bridge failed: {e}")
            print("Attempting to reconnect in 10 seconds...\n")
            time.sleep(10)
            # The 'continue' happens automatically at the end of the loop
        
        finally:
            try:
                session.close()
            except:
                pass

if __name__ == "__main__":
    start_bridge()
