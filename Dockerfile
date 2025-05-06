FROM apache/airflow:2.9.2-python3.12

USER root
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-standard==1.0.0 \
    confluent-kafka==2.5.0 \
    minio==7.2.8 \
    pandas==2.2.3 \
    pyarrow==17.0.0