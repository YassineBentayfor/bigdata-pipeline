# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.docker.operators.docker import DockerOperator
# from docker.types import Mount
# import subprocess

# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2025, 5, 6),
# }

# with DAG(
#     "full_accidents_pipeline",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     def run_producer():
#         subprocess.run(
#             ["python3", "/opt/airflow/dags/producer.py"],
#             check=True,
#         )

#     produce_to_kafka = PythonOperator(
#         task_id="produce_to_kafka",
#         python_callable=run_producer,
#     )

#     def run_consumer():
#         subprocess.run(
#             ["python3", "/opt/airflow/dags/consumer.py"],
#             check=True,
#         )

#     consume_to_minio = PythonOperator(
#         task_id="consume_to_minio",
#         python_callable=run_consumer,
#     )

#     # preprocess_data = SparkSubmitOperator(
#     #     task_id="preprocess_data",
#     #     conn_id="spark_default",
#     #     application="/opt/spark_jobs/spark_preprocess_accidents.py",
#     #     jars=(
#     #         "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
#     #         "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
#     #     ),
#     #     deploy_mode="client",
#     #     packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",

#     #     conf={
#     #         "spark.hadoop.fs.s3a.endpoint":             "http://minio:9000",
#     #         "spark.hadoop.fs.s3a.access.key":           "admin",
#     #         "spark.hadoop.fs.s3a.secret.key":           "password",
#     #         "spark.hadoop.fs.s3a.path.style.access":    "true",
#     #         "spark.hadoop.fs.s3a.connection.ssl.enabled":"false",
#     #         "spark.sql.shuffle.partitions":             "50",
#     #         "spark.driver.memory":                      "2g",
#     #         "spark.executor.memory":                    "4g",
#     #     },
#     # )

#     # preprocess_data = DockerOperator(
#     #     task_id="preprocess_data",
#     #     image="projet-spark",           # your custom Spark image from Dockerfile.spark
#     #     network_mode="projet_default",
#     #     mounts=[  # bind your spark_jobs folder into the container
#     #         Mount(source="/full/path/to/your/project/spark_jobs",
#     #             target="/opt/spark_jobs", type="bind")
#     #     ],
#     #     command=(
#     #     "spark-submit --master spark://spark:7077 "
#     #     "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
#     #     "--conf spark.hadoop.fs.s3a.access.key=admin "
#     #     "--conf spark.hadoop.fs.s3a.secret.key=password "
#     #     "--conf spark.hadoop.fs.s3a.path.style.access=true "
#     #     "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
#     #     "--conf spark.sql.shuffle.partitions=50 "
#     #     "--conf spark.driver.memory=2g "
#     #     "--conf spark.executor.memory=4g "
#     #     "/opt/spark_jobs/spark_preprocess_accidents.py"
#     #     ),
#     #     do_xcom_push=False,
#     # )

#     preprocess_data = SparkSubmitOperator(
#         task_id="preprocess_data",
#         application="/opt/spark_jobs/spark_preprocess_accidents.py",  # Path in Airflow container
#         conn_id="spark_default",
#         verbose=True,
#         conf={
#             "spark.master": "spark://spark:7077",
#             "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
#             "spark.hadoop.fs.s3a.access.key": "admin",
#             "spark.hadoop.fs.s3a.secret.key": "password",
#             "spark.hadoop.fs.s3a.path.style.access": "true",
#             "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
#             "spark.submit.deployMode": "client",
#             "spark.driver.host": "airflow"  # Critical for networking
#         },
#         jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
#     )

#     produce_to_kafka >> consume_to_minio >> preprocess_data




# ####################################

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import subprocess

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 6),
}

def run_producer():
    subprocess.run(
        ["python3", "/opt/airflow/dags/producer.py"],
        check=True,
    )

def run_consumer():
    subprocess.run(
        ["python3", "/opt/airflow/dags/consumer.py"],
        check=True,
    )

with DAG(
    "full_accidents_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    produce_to_kafka = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=run_producer,
    )

    consume_to_minio = PythonOperator(
        task_id="consume_to_minio",
        python_callable=run_consumer,
    )

    preprocess_data = SparkSubmitOperator(
        task_id="preprocess_data",
        conn_id="spark_default",               # points to spark://spark:7077, deploy-mode=client
        application="/opt/spark_jobs/spark_preprocess_accidents.py",
        jars=(
            "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
        ),
        driver_class_path=(
            "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:"
            "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
        ),
        conf={
            # S3A / MinIO settings
            "spark.hadoop.fs.s3a.endpoint":               "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key":             "admin",
            "spark.hadoop.fs.s3a.secret.key":             "password",
            "spark.hadoop.fs.s3a.path.style.access":      "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            # Spark performance / memory
            "spark.sql.shuffle.partitions": "50",
            "spark.driver.memory":          "2g",
            "spark.executor.memory":        "4g",
        },
        name="arrow-spark",
        verbose=True,
    )

    produce_to_kafka >> consume_to_minio >> preprocess_data