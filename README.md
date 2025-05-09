# Big Data Pipeline Tutorial: Accident Data Processing

This tutorial provides a step-by-step guide to replicate the big data pipeline project for processing accident data, from cloning the repository to running the pipeline and creating visualizations. The pipeline ingests CSV data, processes it through Kafka, MinIO, Spark, Trino, and OpenSearch, and supports machine learning with Spark ML. It can be run manually (executing scripts in each container) or orchestrated with Airflow. The tutorial includes setup commands, debugging tips, visualization creation, and details our challenges with Spark-Airflow integration.

## Prerequisites
- **System**: Linux machine (e.g., Ubuntu 20.04) with 16GB RAM, 8 CPU cores.
- **Software**: Docker, Docker Compose, Git.
- **Repository**: Clone from https://github.com/YassineBentayfor/bigdata-pipeline.
- **Data**: Accident CSV files (~1,065,052 rows) downloaded to `data/`.

## Project Overview
The pipeline:
1. **Ingests** CSV data (`data/*.csv`) into Kafka (`accidents` topic).
2. **Stores** raw data in MinIO (`raw/accidents/`).
3. **Processes** data with Spark, saving to MinIO (`processed/accidents_processed/`).
4. **Creates** Trino tables for SQL querying.
5. **Indexes** data in OpenSearch for visualization.
6. **Optional**: Applies Spark ML to classify accidents (e.g., by lighting conditions).

**Components**:
- **Kafka**: Streams data.
- **MinIO**: Stores raw and processed data.
- **Spark**: Processes data.
- **Trino**: Enables SQL queries.
- **OpenSearch**: Hosts visualizations.
- **Airflow**: Orchestrates the pipeline.
- **Jupyter**: Prototypes ML and scripts.

## Setup Instructions

### 1. Clone the Repository
Clone the project and navigate to the directory.

```bash
git clone https://github.com/YassineBentayfor/bigdata-pipeline.git
cd bigdata-pipeline
```

### 2. Download Accident Data
Download accident data CSV files to `data/` using the provided script.

```bash
chmod +x data/download_accident_data.sh
./data/download_accident_data.sh
ls -l data/*.csv
```

**Debugging**:
- **No files downloaded**: Check internet connection or script URLs. Run `cat data/download_accident_data.sh` to verify URLs.
- **Partial files**: Re-run the script or download manually from the source (e.g., French government accident datasets).

### 3. Set System Parameters
Increase virtual memory for OpenSearch.

```bash
sudo sysctl -w vm.max_map_count=512000
```

**Debugging**:
- **Permission denied**: Run as root (`sudo`) or check user permissions.
- **Setting not applied**: Verify with `sysctl vm.max_map_count`.

### 4. Start Docker Containers
Build and start all services (Airflow, Kafka, MinIO, Spark, Trino, OpenSearch, Jupyter).

```bash
docker-compose down
docker volume rm bigdata-pipeline_postgres_data bigdata-pipeline_opensearch_data bigdata-pipeline_minio_data
docker-compose up -d --build
sleep 60
```

**Services**:
- Airflow: `http://localhost:8080` (admin/admin)
- Kafka: `kafka:9092`
- MinIO: `http://localhost:9001` (admin/password)
- Spark: `http://localhost:8081`
- Trino: `http://localhost:8082`
- OpenSearch: `http://localhost:9200`
- OpenSearch Dashboards: `http://localhost:5601`
- Jupyter: `http://localhost:8888`

**Debugging**:
- **Container not running**: Check `docker ps -a | grep bigdata-pipeline`.
- **Build errors**: Verify Dockerfiles (`Dockerfile`, `Dockerfile.spark`, `Dockerfile.jupyter`) and dependencies.
- **Logs**: Run `docker logs bigdata-pipeline-<service>-1` (e.g., `airflow-1`, `opensearch-1`).
- **Port conflicts**: Check `netstat -tuln | grep 8080` and kill conflicting processes or change ports in `docker-compose.yml`.

### 5. Verify Services
Confirm all services are running.

```bash
# Airflow
curl http://localhost:8080
docker logs bigdata-pipeline-airflow-1

# Kafka
docker exec -it bigdata-pipeline-kafka-1 kafka-topics.sh --list --bootstrap-server kafka:9092

# MinIO
docker exec -it bigdata-pipeline-minio-1 mc ls myminio/
curl http://localhost:9001

# Spark
curl http://localhost:8081
docker logs bigdata-pipeline-spark-1

# Trino
curl http://localhost:8082
docker exec -it bigdata-pipeline-trino-1 trino --execute "SHOW CATALOGS"

# OpenSearch
curl http://localhost:9200/?pretty
docker logs bigdata-pipeline-opensearch-1

# OpenSearch Dashboards
curl http://localhost:5601
docker logs bigdata-pipeline-opensearch-dashboards-1

# Jupyter
curl http://localhost:8888
docker logs bigdata-pipeline-jupyter-1
```

**Debugging**:
- **Airflow not responding**: Check `docker logs bigdata-pipeline-airflow-1` for database errors. Re-run `airflow db init`.
- **Kafka errors**: Verify Zookeeper (`docker logs bigdata-pipeline-zookeeper-1`).
- **MinIO buckets missing**: Check `minio-init` logs (`docker logs bigdata-pipeline-minio-init-1`).
- **Spark UI not loading**: Ensure `spark-master` is running (`docker ps | grep spark`).
- **Trino catalog errors**: Verify `trino_catalog/hive.properties` and MinIO connectivity.
- **OpenSearch yellow status**: Normal for single-node setup; check `curl http://localhost:9200/_cat/indices?v`.

## Pipeline Execution

The pipeline can be run in two ways:
1. **Manual Execution**: Run each script in its respective container.
2. **Airflow Orchestration**: Trigger the `full_accidents_pipeline` DAG.

### Option 1: Manual Execution
Execute each step in the corresponding container.

#### 1. Produce to Kafka
Run the producer to send CSV data to Kafka.

```bash
docker cp data/ bigdata-pipeline-airflow-1:/opt/airflow/dags/data/
docker exec -it bigdata-pipeline-airflow-1 python /opt/airflow/dags/producer.py
```

**Verify**:
```bash
docker exec -it bigdata-pipeline-kafka-1 kafka-console-consumer.sh --topic accidents --from-beginning --bootstrap-server kafka:9092 --max-messages 5
```

**Debugging**:
- **No data in topic**: Check `producer.py` logs (`docker logs bigdata-pipeline-airflow-1`). Verify CSV files in `/opt/airflow/dags/data/`.
- **Kafka connection error**: Ensure `kafka:9092` is accessible (`docker exec -it bigdata-pipeline-airflow-1 ping kafka`).

#### 2. Consume from Kafka to MinIO
Run the consumer to save data to MinIO (`raw/accidents/`).

```bash
docker exec -it bigdata-pipeline-airflow-1 python /opt/airflow/dags/consumer.py
```

**Note**: Stop the consumer after ~1 minute with `Ctrl+C` to avoid indefinite running.

**Verify**:
```bash
docker exec -it bigdata-pipeline-minio-1 mc ls myminio/raw/accidents/
```

**Debugging**:
- **No files in MinIO**: Check consumer logs (`docker logs bigdata-pipeline-airflow-1`). Verify Kafka topic has data.
- **MinIO errors**: Ensure buckets exist (`docker exec -it bigdata-pipeline-minio-1 mc ls myminio/`).

#### 3. Process with Spark
Run the Spark job to process data and save to `processed/accidents_processed/`.

```bash
docker exec -it bigdata-pipeline-spark-1 spark-submit /opt/spark_jobs/spark_preprocess_accidents.py
```

**Verify**:
```bash
docker exec -it bigdata-pipeline-minio-1 mc ls myminio/processed/accidents_processed/
```

**Debugging**:
- **Job fails**: Check logs (`docker logs bigdata-pipeline-spark-1`). Verify `spark_conf/spark-defaults.conf` and MinIO connectivity.
- **No data read**: Ensure `raw/accidents/` has Parquet files.
- **Dependency errors**: Confirm JARs in `/opt/bitnami/spark/jars/` (`hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`).

#### 4. Create Trino Table
Create a Trino table for querying processed data.

```bash
docker exec -it bigdata-pipeline-jupyter-1 python /home/jovyan/work/scripts/create_trino_tables.py
```

**Verify**:
```bash
docker exec -it bigdata-pipeline-trino-1 trino --execute "SELECT count(*) FROM hive.default.accidents"
docker exec -it bigdata-pipeline-trino-1 trino --execute "SELECT * FROM hive.default.accidents LIMIT 5"
```

**Debugging**:
- **Table not created**: Check logs (`docker logs bigdata-pipeline-jupyter-1`). Verify `trino_catalog/hive.properties`.
- **Query fails**: Ensure `processed/accidents_processed/` has Parquet files. Check Trino catalog (`SHOW CATALOGS`).

#### 5. Index to OpenSearch
Index processed data to OpenSearch.

```bash
docker exec -it bigdata-pipeline-jupyter-1 python /home/jovyan/work/scripts/index_to_opensearch.py
```

**Verify**:
```bash
curl http://localhost:9200/_cat/indices?v
curl http://localhost:9200/accidents/_search?pretty -H 'Content-Type: application/json' -d '{"query": {"match_all": {}}, "size": 5}'
```

**Debugging**:
- **No documents indexed**: Check script logs (`docker logs bigdata-pipeline-jupyter-1`). Verify Parquet files in `processed/accidents_processed/`.
- **OpenSearch errors**: Ensure `http://opensearch:9200` is accessible (`curl http://opensearch:9200`).

### Option 2: Airflow Orchestration
Trigger the Airflow DAG to run the pipeline automatically.

#### 1. Configure Spark Connection
Set up the Spark connection in Airflow.

- Open `http://localhost:8080` (admin/admin).
- Go to `Admin > Connections`.
- Add/edit connection:
  - Conn ID: `spark_default`
  - Conn Type: Spark
  - Host: `spark://spark:7077`
  - Port: `7077`
- Save.

#### 2. Trigger the DAG
Run the `full_accidents_pipeline` DAG.

```bash
docker exec -it bigdata-pipeline-airflow-1 airflow dags trigger -e "2025-05-09T00:00:00" full_accidents_pipeline
```

**Verify**:
- Check DAG status at `http://localhost:8080`.
- Verify MinIO:
  ```bash
  docker exec -it bigdata-pipeline-minio-1 mc ls myminio/raw/accidents/
  docker exec -it bigdata-pipeline-minio-1 mc ls myminio/processed/accidents_processed/
  ```
- Verify Trino:
  ```bash
  docker exec -it bigdata-pipeline-trino-1 trino --execute "SELECT count(*) FROM hive.default.accidents"
  ```
- Verify OpenSearch:
  ```bash
  curl http://localhost:9200/accidents/_search?pretty -H 'Content-Type: application/json' -d '{"query": {"match_all": {}}, "size": 5}'
  ```

**Debugging**:
- **DAG fails**: Check Airflow logs (`docker logs bigdata-pipeline-airflow-1` or `logs/` directory). View task logs in Airflow UI.
- **Spark task fails**: See “Spark-Airflow Orchestration Challenges” below. Verify `spark_default` connection and `spark_conf/spark-defaults.conf`.
- **Consumer runs indefinitely**: Modify `consumer.py` to limit messages or run manually.
- **Dependency errors**: Ensure Airflow container has all dependencies (`docker exec -it bigdata-pipeline-airflow-1 pip list`).

## Spark-Airflow Orchestration Challenges
Integrating the Spark job into Airflow was challenging. We tried multiple approaches:

1. **Running Spark Job in Airflow Container**:
   - **Attempt**: Executed `spark-submit` inside the Airflow container using `BashOperator`.
   - **Issue**: Defeated the purpose of a separate Spark container. Missing dependencies (e.g., Hadoop JARs) caused failures.
   - **Outcome**: Abandoned due to redundancy and dependency issues.

2. **BashOperator with Docker Command**:
   - **Attempt**: Used `BashOperator` to run `docker exec bigdata-pipeline-spark-1 spark-submit ...`.
   - **Issue**: Required Airflow to have Docker CLI access, which was brittle and failed due to missing JARs or file paths.
   - **Outcome**: Unreliable and not scalable.

3. **DockerOperator**:
   - **Attempt**: Used `DockerOperator` to run the Spark container’s `spark-submit`.
   - **Issue**: Required root access for Airflow to control Docker, posing security risks. Complex configuration and still faced dependency issues.
   - **Outcome**: Not ideal for production.

4. **REST API Approach (Final Solution)**:
   - **Solution**: Used `SparkSubmitOperator` to submit jobs to the Spark master via REST API (`spark://spark:7077`).
   - **Implementation**: Configured `spark_default` connection in Airflow. Mounted `spark_jobs/` and `spark_conf/` to Spark containers. Ensured JARs and configurations in `spark-defaults.conf`.
   - **Outcome**: Reliable, leverages Spark’s native cluster, and aligns with distributed architecture.
   - **Debugging**:
     - **Job not submitted**: Check Spark master UI (`http://localhost:8081`). Verify `spark_default` connection.
     - **Dependency errors**: Ensure JARs in `/opt/bitnami/spark/jars/`. Check `spark_conf/spark-defaults.conf`.
     - **File access issues**: Verify `spark_jobs/spark_preprocess_accidents.py` is mounted (`docker exec -it bigdata-pipeline-spark-1 ls /opt/spark_jobs/`).

**Commands to Fix Spark-Airflow**:
```bash
# Verify Spark configuration
docker exec -it bigdata-pipeline-spark-1 cat /opt/bitnami/spark/conf/spark-defaults.conf

# Check Spark JARs
docker exec -it bigdata-pipeline-spark-1 ls /opt/bitnami/spark/jars/

# Test Spark job manually
docker exec -it bigdata-pipeline-spark-1 spark-submit /opt/spark_jobs/spark_preprocess_accidents.py

# Check Airflow Spark connection
docker exec -it bigdata-pipeline-airflow-1 airflow connections list | grep spark_default

# Re-trigger DAG
docker exec -it bigdata-pipeline-airflow-1 airflow dags trigger -e "2025-05-09T00:00:00" full_accidents_pipeline
```

## Machine Learning Component
An optional Spark ML model classifies accidents by lighting conditions (`lum`) using a small data chunk in Jupyter (`notebooks/eda_accidents.ipynb`). Improvements include:

- **Preprocessing**: Handle missing values in `accidents_2005_2020_cleaned.csv` (e.g., impute `lat`, `long` with mean or `0.0`). Encode categorical features (`dep`, `gps`, `com`) using StringIndexer.
- **Features**: Add `hour`, `atm` (weather), `col` (collision type) for better prediction.
- **Models**: Test Random Forest or Gradient Boosted Trees instead of the current model (likely Logistic Regression).
- **Evaluation**: Use F1-score, confusion matrix, and feature importance to assess informativeness.

**Commands**:
```bash
# Copy cleaned CSV to Spark
docker cp data/accidents_2005_2020_cleaned.csv bigdata-pipeline-spark-1:/opt/spark_jobs/

# Run ML script (create in Spark container)
docker exec -it bigdata-pipeline-spark-1 spark-submit /opt/spark_jobs/ml_accidents.py

# Prototype in Jupyter
docker exec -it bigdata-pipeline-jupyter-1 jupyter notebook --ip=0.0.0.0 --port=8888
```

**Debugging**:
- **Poor model performance**: Check feature correlations (`notebooks/eda_accidents.ipynb`). Add more features or tune hyperparameters.
- **Out of memory**: Increase Spark worker memory in `docker-compose.yml` (`SPARK_WORKER_MEMORY=6g`).
- **Missing values**: Verify preprocessing steps in ML script.

## Create Visualizations
Set up visualizations in OpenSearch Dashboards.

1. **Open Dashboards**:
   ```bash
   curl http://localhost:5601
   ```
   - Open `http://localhost:5601`.

2. **Create Index Pattern**:
   - Go to `Stack Management > Index Patterns`.
   - Click `Create Index Pattern`.
   - Enter `accidents`, select it.
   - Click `Create Index Pattern` (no time field).

3. **Create Visualizations**:
   - Go to `Visualize > Create Visualization`.
   - **Heatmap**:
     - Choose `Coordinate Map`.
     - Select `accidents` index.
     - Use `location` field for geopoints.
     - Save as `Accident Heatmap`.
   - **Histogram**:
     - Choose `Histogram`.
     - Select `accidents` index.
     - X-axis: `hour` (or `an` for year).
     - Save as `Accidents by Hour`.
   - **Pie Chart**:
     - Choose `Pie`.
     - Select `accidents` index.
     - Add bucket: Terms on `dep` (or `lum`).
     - Save as `Accidents by Department`.

4. **Create Dashboard**:
   - Go to `Dashboard > Create Dashboard`.
   - Click `Add`, select `Accident Heatmap`, `Accidents by Hour`, `Accidents by Department`.
   - Save as `Accident Analytics Dashboard`.

5. **Export PDF**:
   - Click `Share > PDF Reports > Generate PDF`.
   - Download and save.

**Verify**:
```bash
ls *.pdf
```

**Debugging**:
- **No data in visualizations**: Verify OpenSearch index (`curl http://localhost:9200/accidents/_search?pretty`). Re-run `index_to_opensearch.py`.
- **Dashboards not loading**: Check logs (`docker logs bigdata-pipeline-opensearch-dashboards-1`). Test `curl http://opensearch:9200`.

## Documentation and Report
The report (`docs/project_tutorial.md`) is this document. Additional steps:

- Update `README.md`:
  ```bash
  cat <<EOL > README.md
  # Big Data Pipeline for Accident Data

  This project processes accident data using Kafka, MinIO, Spark, Trino, OpenSearch, and Airflow. It includes a Spark ML model for accident classification.

  ## Setup
  1. Clone: \`git clone https://github.com/YassineBentayfor/bigdata-pipeline.git\`
  2. Install Docker, Docker Compose.
  3. Run: \`docker-compose up -d --build\`
  4. Follow \`docs/project_tutorial.md\`.

  ## Components
  - **Kafka**: Streams data.
  - **MinIO**: Stores data.
  - **Spark**: Processes data.
  - **Trino**: SQL queries.
  - **OpenSearch**: Visualizations.
  - **Airflow**: Orchestration.

  ## Documentation
  See \`docs/project_tutorial.md\` for detailed setup, execution, and debugging.
  EOL
  ```

- Commit changes:
  ```bash
  git add .
  git commit -m "Final project setup and documentation"
  git push origin main
  ```

**Debugging**:
- **Git push fails**: Check credentials or repository permissions (`git remote -v`).
- **README incomplete**: Ensure all steps are clear and tested.

## Results
- **Pipeline**: Runs manually or via Airflow, processing ~1,065,052 rows.
- **MinIO**: `raw/accidents/` and `processed/accidents_processed/` contain Parquet files.
- **Trino**: `hive.default.accidents` table queryable.
- **OpenSearch**: `accidents` index with documents.
- **Dashboard**: `Accident Analytics Dashboard` with heatmap, histogram, pie chart.
- **ML**: Improved Spark ML model classifying accidents.
- **Report**: `docs/project_tutorial.md` and `README.md` guide replication.

## Troubleshooting Summary
- **General**:
  - Check container status: `docker ps -a | grep bigdata-pipeline`
  - View logs: `docker logs bigdata-pipeline-<service>-1`
  - Restart: `docker-compose down && docker-compose up -d`
- **Airflow**:
  - Logs: `docker logs bigdata-pipeline-airflow-1`
  - UI: `http://localhost:8080`
  - Reset: `docker exec -it bigdata-pipeline-airflow-1 airflow db reset`
- **Kafka**:
  - Topic: `docker exec -it bigdata-pipeline-kafka-1 kafka-topics.sh --list --bootstrap-server kafka:9092`
  - Consumer: `kafka-console-consumer.sh --topic accidents --bootstrap-server kafka:9092`
- **MinIO**:
  - Buckets: `docker exec -it bigdata-pipeline-minio-1 mc ls myminio/`
  - Inspect file: `docker exec -it bigdata-pipeline-minio-1 mc cp myminio/processed/accidents_processed/an=2019/sample.parquet /tmp/sample.parquet && docker exec -it bigdata-pipeline-jupyter-1 python3 -c "import pandas as pd; df = pd.read_parquet('/tmp/sample.parquet'); print(df.head())"`
- **Spark**:
  - UI: `http://localhost:8081`
  - Logs: `docker logs bigdata-pipeline-spark-1`
  - Test: `docker exec -it bigdata-pipeline-spark-1 spark-submit /opt/spark_jobs/spark_preprocess_accidents.py`
- **Trino**:
  - Catalog: `docker exec -it bigdata-pipeline-trino-1 trino --execute "SHOW CATALOGS"`
  - Schema: `docker exec -it bigdata-pipeline-trino-1 trino --execute "SHOW SCHEMAS IN hive"`
- **OpenSearch**:
  - Index: `curl http://localhost:9200/_cat/indices?v`
  - Data: `curl http://localhost:9200/accidents/_search?pretty`