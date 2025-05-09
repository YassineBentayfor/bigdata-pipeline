from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
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

# Connexion à OpenSearch sans SSL
os_client = OpenSearch(
    hosts=[{"host": "opensearch", "port": 9200, "scheme": "http"}],
    http_auth=None
)

# Lire les fichiers Parquet depuis MinIO
bucket_name = "processed"
prefix = "accidents_processed/"
objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]
print(f"Found {len(parquet_files)} Parquet files: {parquet_files}")

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
else:
    print(f"Index {index_name} already exists")

# Indexer les données en bulk
actions = []
for file in parquet_files:
    print(f"Processing file: {file}")
    # Télécharger le fichier Parquet
    local_file = f"/tmp/{file.split('/')[-1]}"
    minio_client.fget_object(bucket_name, file, local_file)
    
    # Lire le Parquet
    df = pd.read_parquet(local_file)
    print(f"File {file} has {len(df)} rows")
    
    # Filtrer les lignes avec lat/long valides
    df = df[df['lat'].notnull() & df['long'].notnull()]
    df = df[df['lat'].apply(lambda x: isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '', 1).isdigit()))]
    df = df[df['long'].apply(lambda x: isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '', 1).isdigit()))]
    print(f"File {file} after filtering: {len(df)} valid rows")
    
    # Préparer les actions pour le bulk
    for _, row in df.iterrows():
        doc = row.to_dict()
        doc["location"] = {"lat": float(row["lat"]), "lon": float(row["long"])}
        actions.append({
            "_index": index_name,
            "_source": doc
        })
    
    # Indexer par lots de 1000
    if len(actions) >= 1000:
        success, failed = bulk(os_client, actions)
        print(f"Indexed {success} documents, {failed} failed")
        actions = []

# Indexer les actions restantes
if actions:
    success, failed = bulk(os_client, actions)
    print(f"Indexed {success} documents, {failed} failed")
else:
    print("No documents to index")

print(f"Indexation terminée dans l'index {index_name}")