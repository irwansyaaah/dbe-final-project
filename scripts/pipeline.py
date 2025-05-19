import psycopg2
from confluent_kafka import Producer
import json
import time
from datetime import datetime

# PostgreSQL connection configuration
PG_CONFIG = {
    'dbname': 'smoking_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'smoking-producer'
}

def create_kafka_producer():
    return Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def fetch_and_stream_data():
    # Connect to PostgreSQL
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    try:
        # Query to fetch data from smoking table
        cur.execute("SELECT * FROM smoking")
        
        # Fetch all rows
        rows = cur.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cur.description]
        
        # Stream each row to Kafka
        for row in rows:
            # Create dictionary from row data
            data = dict(zip(columns, row))
            
            # Add timestamp
            data['timestamp'] = datetime.now().isoformat()
            
            # Convert to JSON
            message = json.dumps(data)
            
            # Produce message to Kafka
            producer.produce(
                'smoking-data',
                message.encode('utf-8'),
                callback=delivery_report
            )
            
            # Flush to ensure message is sent
            producer.flush()
            
            # Small delay to prevent overwhelming the system
            time.sleep(0.1)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print("Starting data pipeline...")
    fetch_and_stream_data()
    print("Data pipeline completed.") 