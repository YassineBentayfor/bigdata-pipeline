from confluent_kafka import Producer
import pandas as pd
import json
import csv

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    # Kafka configuration
    conf = {
        'bootstrap.servers': 'projet-kafka-1:9092',
        'client.id': 'accident-producer'
    }
    producer = Producer(conf)

    # Read CSV in chunks with latin1 encoding and semicolon delimiter
    csv_path = '/opt/airflow/dags/data/accidents_2005_2020_cleaned.csv'
    chunk_size = 10000
    for chunk in pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        encoding='latin1',
        sep=';',
        quoting=csv.QUOTE_MINIMAL
    ):
        for _, row in chunk.iterrows():
            # Convert row to JSON
            message = row.to_json()
            producer.produce(
                topic='accidents',
                value=message.encode('utf-8'),
                callback=delivery_report
            )
        producer.poll(0)
    producer.flush()

if __name__ == '__main__':
    main()


    