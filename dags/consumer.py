from confluent_kafka import Consumer, KafkaError
from minio import Minio
import json
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io
import time

def main():
    # Kafka configuration
    conf = {
        'bootstrap.servers': 'projet-kafka-1:9092',
        'group.id': 'accident-consumer',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['accidents'])

    # MinIO configuration
    minio_client = Minio(
        'projet-minio-1:9000',
        access_key='admin',
        secret_key='password',
        secure=False
    )
    bucket_name = 'accidents'

    # Ensure bucket exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Process messages
    rows = []
    batch_count = 0
    start_time = time.time()
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of partition")
                break
            else:
                print(f"Consumer error: {msg.error()}")
                break
        row = json.loads(msg.value().decode('utf-8'))
        rows.append(row)
        if len(rows) >= 5000:  # Batch size
            df = pd.DataFrame(rows)
            # Fix hrmn column: strip non-digits, zero-pad to 4 digits
            if 'hrmn' in df.columns:
                df['hrmn'] = (
                    df['hrmn']
                    .fillna('0000')
                    .astype(str)
                    .str.replace(r'\D', '', regex=True)
                    .str.zfill(4)
                )
            # Fix com column: string with leading zeros
            if 'com' in df.columns:
                df['com'] = df['com'].fillna('0000').astype(str).str.zfill(4)
            # Ensure all columns are strings to avoid type errors
            for col in df.columns:
                df[col] = df[col].astype(str)
            year = df['an'].iloc[0]
            output = io.BytesIO()
            df.to_parquet(output, engine='pyarrow', index=False)
            output.seek(0)
            object_name = f'raw/year={year}/part-{batch_count}.parquet'
            minio_client.put_object(
                bucket_name,
                object_name,
                output,
                length=output.getbuffer().nbytes
            )
            batch_count += 1
            print(f"Wrote batch {batch_count} ({len(rows)} rows) to {object_name} in {time.time() - start_time:.2f} seconds")
            rows = []
            start_time = time.time()
    # Handle final partial batch
    if rows:
        df = pd.DataFrame(rows)
        if 'hrmn' in df.columns:
            df['hrmn'] = (
                df['hrmn']
                .fillna('0000')
                .astype(str)
                .str.replace(r'\D', '', regex=True)
                .str.zfill(4)
            )
        if 'com' in df.columns:
            df['com'] = df['com'].fillna('0000').astype(str).str.zfill(4)
        for col in df.columns:
            df[col] = df[col].astype(str)
        year = df['an'].iloc[0]
        output = io.BytesIO()
        df.to_parquet(output, engine='pyarrow', index=False)
        output.seek(0)
        object_name = f'raw/year={year}/part-{batch_count}.parquet'
        minio_client.put_object(
            bucket_name,
            object_name,
            output,
            length=output.getbuffer().nbytes
        )
        print(f"Wrote final batch {batch_count + 1} ({len(rows)} rows) to {object_name} in {time.time() - start_time:.2f} seconds")
    consumer.close()

if __name__ == '__main__':
    main()