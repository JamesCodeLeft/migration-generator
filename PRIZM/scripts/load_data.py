import os
import pandas as pd
import re
import json
from pathlib import Path
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
DB_URL = os.getenv("DB_URL")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID", "PRIZM")

# Using relative path from 'scripts' folder to 'metadata' folder
BASE_METADATA_DIR = os.path.join(os.path.dirname(__file__), "..", "metadata")

# SQL Server to BigQuery Type Mapping (Synced with migration generator)
TYPE_MAPPING = {
    "uniqueidentifier": "STRING", "nvarchar": "STRING", "ntext": "STRING", "varchar": "STRING",
    "text": "STRING", "int": "INT64", "bigint": "INT64", "smallint": "INT64", "tinyint": "INT64",
    "bit": "BOOL", "datetime": "DATETIME", "datetime2": "DATETIME", "date": "DATE",
    "float": "FLOAT64", "decimal": "NUMERIC", "numeric": "NUMERIC", "money": "NUMERIC",
    "char": "STRING", "nchar": "STRING", "binary": "BYTES", "varbinary": "BYTES", "image": "BYTES"
}

def sanitize_name(name):
    """Replaces any character that is NOT a letter, number, or underscore with an underscore."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def get_latest_schema(table_name):
    """
    Locates the most recent JSON metadata for a table and returns a BigQuery schema.
    """
    base_path = Path(BASE_METADATA_DIR)
    if not base_path.exists():
        return None

    # Find the latest timestamped directory
    subdirs = sorted([d for d in base_path.iterdir() if d.is_dir()])
    if not subdirs:
        return None
    
    latest_dir = subdirs[-1]
    json_path = latest_dir / f"{table_name}.json"
    
    if not json_path.exists():
        return None

    with open(json_path, 'r') as f:
        metadata = json.load(f)
    
    schema = []
    for col in metadata["columns"]:
        sanitized_col_name = sanitize_name(col["name"])
        bq_type = TYPE_MAPPING.get(col["type"].lower(), "STRING")
        schema.append(bigquery.SchemaField(sanitized_col_name, bq_type))
    
    return schema

def apply_schema_types(df, schema):
    """
    Uses the BigQuery schema to force correct Pandas types.
    This prevents Pyarrow conversion errors for Dates and Nullable Integers.
    """
    if not schema:
        # Fallback if no schema is provided: use generic nullable conversion
        return df.convert_dtypes()

    for field in schema:
        col = field.name
        if col not in df.columns:
            continue
            
        if field.field_type in ["DATETIME", "DATE", "TIMESTAMP"]:
            # Ensure dates are actual datetime objects, not strings
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif field.field_type == "INT64":
            # Ensure nullable integers are handled correctly
            df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")
        elif field.field_type == "BOOL":
            # Handle bits/bools
            df[col] = df[col].astype("boolean")
        elif field.field_type == "STRING":
            # Ensure string columns are actual strings
            df[col] = df[col].astype("string")
            
    return df

def load_table_to_bq(client, table_name, truncate=False):
    """Extracts data from SQL Server and loads it into an EXISTING BigQuery table."""
    print(f"\n--- Processing: {table_name} ---")
    
    engine = create_engine(DB_URL)
    
    # 1. Fast check for empty table
    try:
        count_query = f"SELECT COUNT(*) as TotalRows FROM [{table_name}]"
        total_rows = int(pd.read_sql(count_query, engine).iloc[0]['TotalRows'])
        if total_rows == 0:
            print(f"  Skipping {table_name} (0 rows).")
            return
    except Exception as e:
        print(f"  Error checking row count for {table_name}: {e}")
        return
    
    sanitized_table_name = sanitize_name(table_name)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{sanitized_table_name}"
    
    # 2. Verify table exists (Should be created by dbmate migrations)
    try:
        client.get_table(table_ref)
    except NotFound:
        print(f"  Error: Table {table_ref} not found.")
        print(f"  Please run migrations (dbmate up) before loading data.")
        return

    # 3. Fetch schema from metadata
    bq_schema = get_latest_schema(table_name)
    if bq_schema:
        print(f"  Using explicit schema from metadata for {table_name}.")
    else:
        print(f"  Warning: No metadata found for {table_name}. Falling back to inference.")

    query = f"SELECT * FROM [{table_name}]"
    
    try:
        # 4. Read data from SQL Server in chunks
        # Set chunk_size to 20,000. This allows for up to 80 million rows per table 
        # per day before hitting BigQuery's partition modification quota (4,000/day).
        chunk_size = 20000
        print(f"  Fetching data from SQL Server in chunks of {chunk_size}...")
        
        total_rows_loaded = 0
        batch_count = 0
        first_chunk = True
        
        for df_chunk in pd.read_sql(query, engine, chunksize=chunk_size):
            batch_count += 1
            print(f"    [Batch {batch_count}] Read {len(df_chunk)} rows from SQL. Preparing upload...")
            
            # 5. Sanitize column names to match migration-generated schema
            df_chunk.columns = [sanitize_name(col) for col in df_chunk.columns]
            
            # 6. Apply strict typing based on the BigQuery schema
            df_chunk = apply_schema_types(df_chunk, bq_schema)
            
            # Determine write disposition: truncate only on the first chunk if requested
            if first_chunk and truncate:
                current_write_disposition = "WRITE_TRUNCATE"
            else:
                current_write_disposition = "WRITE_APPEND"
            
            # 7. Prepare BigQuery Load Job
            job_config = bigquery.LoadJobConfig(
                schema=bq_schema,
                create_disposition="CREATE_NEVER",
                write_disposition=current_write_disposition,
                source_format=bigquery.SourceFormat.PARQUET,
            )
            
            # 8. Execute Load Job
            print(f"    [Batch {batch_count}] Uploading to BigQuery...")
            job = client.load_table_from_dataframe(df_chunk, table_ref, job_config=job_config)
            job.result()  # Wait for the job to complete
            
            total_rows_loaded += len(df_chunk)
            print(f"    [Batch {batch_count}] Success! Total rows uploaded so far: {total_rows_loaded}")
            
            first_chunk = False
            
        print(f"  Successfully loaded {total_rows_loaded} total rows for {table_name} to BigQuery.")
        
    except Exception as e:
        print(f"  Error processing {table_name}: {e}")

def run_ingestion(tables=None, truncate=False):
    """Runs the ingestion process."""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Verify dataset exists
    try:
        client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")
    except NotFound:
        print(f"Error: Dataset {DATASET_ID} not found in project {PROJECT_ID}.")
        return

    if not tables:
        engine = create_engine(DB_URL)
        list_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME ASC"
        tables = pd.read_sql(list_query, engine)['TABLE_NAME'].tolist()

    for table in tables:
        load_table_to_bq(client, table, truncate=truncate)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load data from SQL Server to BigQuery.")
    parser.add_argument("--tables", nargs="+", help="Specific tables to load.")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before loading.")
    
    args = parser.parse_args()
    run_ingestion(tables=args.tables, truncate=args.truncate)
