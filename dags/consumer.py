import pandas as pd
from confluent_kafka import Consumer, KafkaError
from minio import Minio
import io
import json
import os

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'accidents_consumer',
    'auto.offset.reset': 'earliest',
}

# MinIO configuration
minio_client = Minio(
    'minio:9000',
    access_key='admin',
    secret_key='password',
    secure=False,
)

# Ensure bucket exists
bucket_name = 'accidents'
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Consumer setup
consumer = Consumer(kafka_config)
consumer.subscribe(['accidents'])

# Collect records
records = []
batch_size = 10000
year_counts = {}

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        break
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            break
    
    # Parse message
    record = json.loads(msg.value().decode('utf-8'))
    records.append(record)
    
    # Track year for partitioning
    year = record.get('an', 'unknown')
    year_counts[year] = year_counts.get(year, 0) + 1
    
    # Write batch to MinIO
    if len(records) >= batch_size:
        df = pd.DataFrame(records)
        year = df['an'].mode()[0]  # Most common year in batch
        output_path = f"raw/year={year}/part-{len(year_counts[year])}.parquet"
        
        # Convert to Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        # Upload to MinIO
        minio_client.put_object(
            bucket_name,
            output_path,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/octet-stream',
        )
        print(f"Wrote {len(records)} records to s3://accidents/{output_path}")
        records = []

# Write remaining records
if records:
    df = pd.DataFrame(records)
    year = df['an'].mode()[0]
    output_path = f"raw/year={year}/part-{len(year_counts.get(year, [0]))}.parquet"
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    minio_client.put_object(
        bucket_name,
        output_path,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type='application/octet-stream',
    )
    print(f"Wrote {len(records)} records to s3://accidents/{output_path}")

consumer.close()
print("Consumer finished")