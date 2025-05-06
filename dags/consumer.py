from confluent_kafka import Consumer, KafkaError
from minio import Minio
import json
import pandas as pd
import io
import time

def main():
    # Kafka configuration
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'accident-consumer',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['accidents'])

    # MinIO configuration
    minio_client = Minio(
        'minio:9000',
        access_key='admin',
        secret_key='password',
        secure=False
    )
    bucket_name = 'accidents'
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    rows = []
    batch_count = 0
    start_time = time.time()
    idle_counter = 0
    max_idle = 5      # stop after 5s of inactivity once we’ve read at least one message
    seen_any = False

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            if seen_any:
                idle_counter += 1
                if idle_counter >= max_idle:
                    print(f"No messages for {max_idle} seconds → stopping consumer")
                    break
            continue

        idle_counter = 0
        seen_any = True

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # Parse and collect
        row = json.loads(msg.value().decode('utf-8'))
        rows.append(row)

        # Once we have a full batch, convert to Parquet & upload
        if len(rows) >= 5000:
            df = pd.DataFrame(rows)

            # ── Fill missing 'an' (year) with 0 ──────────────────────────────
            if 'an' in df.columns:
                df['an'] = (
                    df['an']
                      .fillna(0)                 # replace null with 0
                      .astype(float)
                      .astype(int)
                      .astype(str)
                )
            # ────────────────────────────────────────────────────────────────

            # ── Clean 'hrmn' (time) column ─────────────────────────────────
            if 'hrmn' in df.columns:
                df['hrmn'] = (
                    df['hrmn']
                      .fillna('0000')
                      .astype(str)
                      .str.replace(r'\D', '', regex=True)
                      .str.zfill(4)
                )
            # ────────────────────────────────────────────────────────────────

            # ── Clean 'com' (commune) column ───────────────────────────────
            if 'com' in df.columns:
                df['com'] = (
                    df['com']
                      .fillna('0000')
                      .astype(str)
                      .str.zfill(4)
                )
            # ────────────────────────────────────────────────────────────────

            # Ensure everything is string to avoid mixed-type Parquet errors
            for col in df.columns:
                df[col] = df[col].astype(str)

            year = df['an'].iloc[0]
            output = io.BytesIO()
            df.to_parquet(output, engine='pyarrow', index=False)
            output.seek(0)

            object_name = f"raw/year={year}/part-{batch_count}.parquet"
            minio_client.put_object(
                bucket_name,
                object_name,
                output,
                length=output.getbuffer().nbytes
            )
            elapsed = time.time() - start_time
            print(f"Wrote batch {batch_count} ({len(df)} rows) to {object_name} in {elapsed:.2f} seconds")
            batch_count += 1

            # reset for next batch
            rows = []
            start_time = time.time()

    # ── Final partial batch if any remains ─────────────────────────────────
    if rows:
        df = pd.DataFrame(rows)

        if 'an' in df.columns:
            df['an'] = (
                df['an']
                  .fillna(0)
                  .astype(float)
                  .astype(int)
                  .astype(str)
            )
        if 'hrmn' in df.columns:
            df['hrmn'] = (
                df['hrmn']
                  .fillna('0000')
                  .astype(str)
                  .str.replace(r'\D', '', regex=True)
                  .str.zfill(4)
            )
        if 'com' in df.columns:
            df['com'] = (
                df['com']
                  .fillna('0000')
                  .astype(str)
                  .str.zfill(4)
            )

        for col in df.columns:
            df[col] = df[col].astype(str)

        year = df['an'].iloc[0]
        output = io.BytesIO()
        df.to_parquet(output, engine='pyarrow', index=False)
        output.seek(0)
        object_name = f"raw/year={year}/part-{batch_count}.parquet"
        minio_client.put_object(
            bucket_name,
            object_name,
            output,
            length=output.getbuffer().nbytes
        )
        elapsed = time.time() - start_time
        print(f"Wrote final batch {batch_count} ({len(df)} rows) to {object_name} in {elapsed:.2f} seconds")
    # ─────────────────────────────────────────────────────────────────────────

    consumer.close()

if __name__ == '__main__':
    main()
