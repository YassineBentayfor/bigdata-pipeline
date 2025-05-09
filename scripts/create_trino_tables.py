import logging
from trino.dbapi import connect
from trino.exceptions import TrinoUserError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Trino connection parameters
TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "admin"
TRINO_CATALOG = "hive"
TRINO_SCHEMA = "default"

# Table definition
TABLE_NAME = "accidents_processed"
TABLE_LOCATION = "s3a://processed/accidents_processed/"
TABLE_FORMAT = "PARQUET"
PARTITION_COLUMNS = ["an", "dep"]
TABLE_SCHEMA = """
    Num_Acc VARCHAR,
    mois VARCHAR,
    jour VARCHAR,
    hrmn VARCHAR,
    lum VARCHAR,
    agg VARCHAR,
    int VARCHAR,
    atm VARCHAR,
    col VARCHAR,
    com VARCHAR,
    adr VARCHAR,
    gps VARCHAR,
    lat VARCHAR,
    long VARCHAR,
    year INTEGER,
    hour INTEGER,
    lum_ohe ARRAY(DOUBLE),
    agg_ohe ARRAY(DOUBLE),
    int_ohe ARRAY(DOUBLE),
    atm_ohe ARRAY(DOUBLE),
    col_ohe ARRAY(DOUBLE),
    an INTEGER,
    dep VARCHAR
"""

def create_trino_connection():
    """Create and return a Trino connection."""
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA
        )
        logger.info("Connected to Trino")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Trino: {e}")
        raise

def execute_query(cursor, query, description):
    """Execute a SQL query and log the result."""
    try:
        cursor.execute(query)
        logger.info(f"Successfully executed: {description}")
    except TrinoUserError as e:
        if "already exists" in str(e).lower():
            logger.warning(f"{description} already exists, skipping")
        else:
            logger.error(f"Failed to execute {description}: {e}")
            raise
    except Exception as e:
        logger.error(f"Failed to execute {description}: {e}")
        raise

def create_schema(cursor):
    """Create the Hive schema if it doesn't exist."""
    schema_query = f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA} WITH (location = '{TABLE_LOCATION}')"
    execute_query(cursor, schema_query, f"Schema {TRINO_CATALOG}.{TRINO_SCHEMA}")

def create_table(cursor):
    """Create the accidents_processed table if it doesn't exist."""
    table_query = f"""
        CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TABLE_NAME} (
            {TABLE_SCHEMA}
        )
        WITH (
            external_location = '{TABLE_LOCATION}',
            format = '{TABLE_FORMAT}',
            partitioned_by = ARRAY[{', '.join(f"'{col}'" for col in PARTITION_COLUMNS)}]
        )
    """
    execute_query(cursor, table_query, f"Table {TABLE_NAME}")

def main():
    """Main function to create Trino schema and table."""
    conn = create_trino_connection()
    cursor = conn.cursor()
    try:
        create_schema(cursor)
        create_table(cursor)
        logger.info("Trino schema and table creation completed successfully")
    except Exception as e:
        logger.error(f"Failed to complete Trino setup: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()