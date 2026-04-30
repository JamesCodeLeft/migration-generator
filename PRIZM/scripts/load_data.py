import os
import pandas as pd
import re
import json
import decimal
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

def get_latest_metadata(table_name):
    """
    Locates the most recent JSON metadata for a table.
    """
    base_path = Path(BASE_METADATA_DIR)
    if not base_path.exists():
        return None

    subdirs = sorted([d for d in base_path.iterdir() if d.is_dir()])
    if not subdirs:
        return None
    
    latest_dir = subdirs[-1]
    json_path = latest_dir / f"{table_name}.json"
    
    if not json_path.exists():
        return None

    with open(json_path, 'r') as f:
        return json.load(f)

def get_bq_schema(metadata):
    """Converts metadata columns to BigQuery SchemaField objects."""
    if not metadata:
        return None
    schema = []
    for col in metadata["columns"]:
        sanitized_col_name = sanitize_name(col["name"])
        bq_type = TYPE_MAPPING.get(col["type"].lower(), "STRING")
        schema.append(bigquery.SchemaField(sanitized_col_name, bq_type))
    return schema

def apply_schema_types(df, schema):
    """
    Uses the BigQuery schema to force correct Pandas types.
    This prevents Pyarrow conversion errors for Dates, Nullable Integers, 
    and 16-byte NUMERIC/GUID mismatches.
    """
    if not schema:
        return df.convert_dtypes()

    for field in schema:
        col = field.name
        if col not in df.columns:
            continue
            
        if field.field_type in ["DATETIME", "DATE", "TIMESTAMP"]:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif field.field_type == "INT64":
            df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")
        elif field.field_type in ["NUMERIC", "BIGNUMERIC"]:
            df[col] = df[col].apply(lambda x: decimal.Decimal(str(x)) if pd.notnull(x) else None)
        elif field.field_type == "BOOL":
            df[col] = df[col].astype("boolean")
        elif field.field_type == "STRING":
            df[col] = df[col].astype("string")
            
    return df

def load_table_to_bq(client, table_name, truncate=False):
    """Extracts data from SQL Server and loads it into BigQuery via a staging table."""
    print(f"\n--- Processing: {table_name} ---")
    
    engine = create_engine(DB_URL)
    
    # 1. Fast check for empty source table
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
    staging_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{sanitized_table_name}_stg"
    
    # 2. Verify target table exists
    try:
        client.get_table(table_ref)
    except NotFound:
        print(f"  Error: Table {table_ref} not found. Run migrations first.")
        return

    # 3. Fetch metadata and schema
    metadata = get_latest_metadata(table_name)
    bq_schema = get_bq_schema(metadata)
    
    if bq_schema:
        print(f"  Using explicit schema from metadata for {table_name}.")
    else:
        print(f"  Warning: No metadata found for {table_name}. Falling back to inference.")

    query = f"SELECT * FROM [{table_name}]"
    
    try:
        # 4. Read data and load to STAGING table
        # We use a non-partitioned staging table to avoid 403 Quota errors.
        chunk_size = 20000 
        print(f"  Loading to staging table: {staging_table_ref}")
        
        total_rows_loaded = 0
        batch_count = 0
        first_chunk = True
        
        for df_chunk in pd.read_sql(query, engine, chunksize=chunk_size):
            batch_count += 1
            print(f"    [Batch {batch_count}] Read {len(df_chunk)} rows. Preparing staging upload...")
            
            df_chunk.columns = [sanitize_name(col) for col in df_chunk.columns]
            df_chunk = apply_schema_types(df_chunk, bq_schema)
            
            # Staging table is ALWAYS truncated on the first chunk to ensure a fresh start
            write_disp = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
            
            job_config = bigquery.LoadJobConfig(
                schema=bq_schema,
                write_disposition=write_disp,
                create_disposition="CREATE_IF_NEEDED", # Create the stg table if it doesn't exist
                source_format=bigquery.SourceFormat.PARQUET,
            )
            
            job = client.load_table_from_dataframe(df_chunk, staging_table_ref, job_config=job_config)
            job.result()
            
            total_rows_loaded += len(df_chunk)
            first_chunk = False
            
        print(f"  Successfully loaded {total_rows_loaded} rows into staging.")

        # 5. Move data from Staging to Final Table (ONE modification job)
        print(f"  Moving data to final table: {table_ref}...")
        
        if truncate:
            # For truncate, we use a Copy Job which is fast and preserves partitioning
            copy_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
            copy_job = client.copy_table(staging_table_ref, table_ref, job_config=copy_config)
            copy_job.result()
        else:
            # For append, we use a simple SQL INSERT to be safe
            insert_query = f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{sanitized_table_name}` SELECT * FROM `{staging_table_ref}`"
            query_job = client.query(insert_query)
            query_job.result()

        # 6. Cleanup
        client.delete_table(staging_table_ref, not_found_ok=True)
        print(f"  Success! Total rows synced: {total_rows_loaded}")
        
    except Exception as e:
        print(f"  Error processing {table_name}: {e}")
        # Attempt cleanup even on failure
        client.delete_table(staging_table_ref, not_found_ok=True)

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
