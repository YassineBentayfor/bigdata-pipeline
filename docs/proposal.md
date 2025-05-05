Big Data Platform Project Proposal
Group Members

Yassine Bentayfor

Dataset
We will use the French Road Accident Dataset (2005–2022) from data.gouv.fr (~1.2GB, CSV). It includes accident locations, severity, weather, and driver demographics, suitable for a robust Big Data pipeline with minimal analysis.
Use Case
The platform will process French road accident data to enable efficient storage, querying, and basic analytics (e.g., accident hotspots, severity prediction), with a focus on production-grade infrastructure.
Pipeline Architecture

Ingestion: Apache Airflow for batch CSV downloads from data.gouv.fr; Apache Kafka for simulated real-time streaming.
Raw Storage: MinIO (S3-compatible) with Parquet files, partitioned by year and department.
Processing: Apache Spark for data cleaning, transformation, and a Random Forest model for severity prediction.
Curated Storage: Apache Iceberg for ACID-compliant, partitioned tables with time travel.
Exposure: Trino for distributed SQL querying; OpenSearch Dashboards for minimal visualizations.

Advanced Techniques

Apache Iceberg: ACID transactions, partitioning, and time travel for curated storage, not covered in our course.
Trino: Distributed SQL querying for fast analytics, also not covered.

Deliverables

GitHub repository with pipeline code and configurations.
Dockerized infrastructure (Airflow, Kafka, MinIO, Spark, Iceberg, Trino).
Jupyter notebook with basic analyses (e.g., hotspots, ML predictions).
Final report detailing the pipeline and advanced tools.

Timeline

- Setup, dataset exploration.
- Ingestion, raw storage.
- Processing, ML, curated storage.
- Exposure, analyses, documentation.

Resource Requirements

VM (4 cores, 14GB RAM, 95GB disk).
Docker, Docker Compose, Python 3.9, Git.

