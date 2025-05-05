import pandas as pd
from confluent_kafka import Producer
import json

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'kafka:9092',
}

# Producer setup
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Read CSV and publish chunks
csv_file = '/opt/airflow/dags/data/accidents_2005_2020_cleaned.csv'
chunk_size = 10000

for chunk in pd.read_csv(csv_file, sep=';', encoding='latin1', chunksize=chunk_size, quotechar='"'):
    # Convert chunk to JSON
    records = chunk.to_dict(orient='records')
    for record in records:
        producer.produce('accidents', value=json.dumps(record).encode('utf-8'), callback=delivery_report)
    producer.flush()
    print(f"Published chunk of {len(records)} records")

print("Producer finished")