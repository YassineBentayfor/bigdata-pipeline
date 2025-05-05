from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

def run_producer():
    subprocess.run(["python3", "/opt/airflow/dags/producer.py"], check=True)

def run_consumer():
    subprocess.run(["python3", "/opt/airflow/dags/consumer.py"], check=True)

with DAG(
    'ingest_accidents',
    start_date=datetime(2025, 5, 6),
    schedule_interval=None,  # Run manually for now
    catchup=False,
) as dag:
    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=run_producer,
    )

    consume_task = PythonOperator(
        task_id='consume_to_minio',
        python_callable=run_consumer,
    )

    produce_task >> consume_task