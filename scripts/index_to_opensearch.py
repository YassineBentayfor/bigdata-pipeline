from opensearchpy import OpenSearch
import pandas as pd
import pyarrow.parquet as pq
from minio import Minio

# Connexion à MinIO
minio_client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

# Connexion à OpenSearch
os_client = OpenSearch(
    hosts=[{"host": "opensearch", "port": 9200}],
    http_auth=None,
    use_ssl=False,
    verify_certs=False,
    ssl_show_warn=False
)

# Lire les fichiers Parquet depuis MinIO
bucket_name = "processed"
prefix = "accidents_processed/"
objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]

# Créer l'index OpenSearch
index_name = "accidents"
if not os_client.indices.exists(index_name):
    os_client.indices.create(
        index=index_name,
        body={
            "mappings": {
                "properties": {
                    "an": {"type": "integer"},
                    "dep": {"type": "keyword"},
                    "lat": {"type": "float"},
                    "long": {"type": "float"},
                    "gps": {"type": "keyword"},
                    "adr": {"type": "text"},
                    "com": {"type": "keyword"},
                    "hrmn": {"type": "keyword"},
                    "hour": {"type": "integer"},
                    "lum_ohe": {"type": "float", "index": False},
                    "agg_ohe": {"type": "float", "index": False},
                    "int_ohe": {"type": "float", "index": False},
                    "atm_ohe": {"type": "float", "index": False},
                    "col_ohe": {"type": "float", "index": False},
                    "location": {"type": "geo_point"}
                }
            }
        }
    )

# Indexer les données
for file in parquet_files:
    # Télécharger le fichier Parquet
    local_file = f"/tmp/{file.split('/')[-1]}"
    minio_client.fget_object(bucket_name, file, local_file)
    
    # Lire le Parquet
    df = pd.read_parquet(local_file)
    
    # Ajouter un champ geo_point pour la carte
    df["location"] = df.apply(lambda row: {"lat": row["lat"], "lon": row["long"]}, axis=1)
    
    # Indexer chaque ligne
    for _, row in df.iterrows():
        doc = row.to_dict()
        os_client.index(index=index_name, body=doc)

print(f"Indexation terminée dans l'index {index_name}")