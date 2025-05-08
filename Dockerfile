# FROM apache/airflow:2.9.2-python3.12

# USER root
# RUN apt-get update && apt-get install -y wget && \
#     mkdir -p /opt/spark/jars && \
#     wget -q -P /opt/spark/jars \
#       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
#       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*

# USER airflow
# RUN pip install --no-cache-dir \
#     apache-airflow-providers-standard==1.0.0 \
#     apache-airflow-providers-apache-spark==4.9.\
#     confluent-kafka==2.5.0 \
#     minio==7.2.8 \
#     pandas==2.2.3 \
#     pyarrow==17.0.0 \
#     pyspark==3.4.1

# Dockerfile (airflow)




FROM apache/airflow:2.9.2-python3.12

USER root
RUN apt-get update && apt-get install -y wget && \
    mkdir -p /opt/spark/jars && \
    wget -q -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-standard==1.0.0 \
    apache-airflow-providers-apache-spark==4.9.\
    confluent-kafka==2.5.0 \
    minio==7.2.8 \
    pandas==2.2.3 \
    pyarrow==17.0.0 \
    pyspark==3.4.1
