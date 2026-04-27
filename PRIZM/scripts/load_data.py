import os
import pandas as pd
import re
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
# SQL Server connection (reused from generate_metadata.py)
DB_URL = os.getenv("DB_URL")

# BigQuery Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID", "PRIZM")

def sanitize_name(name):
    """Replaces any character that is NOT a letter, number, or underscore with an underscore."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def load_table_to_bq(client, table_name, truncate=False):
    """Extracts data from SQL Server and loads it into BigQuery."""
    print(f"\n--- Processing: {table_name} ---")
    
    engine = create_engine(DB_URL)
    query = f"SELECT * FROM [{table_name}]"
    
    try:
        # 1. Read data from SQL Server
        print(f"  Fetching data from SQL Server...")
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print(f"  Table {table_name} is empty. Skipping load.")
            return

        # 2. Sanitize column names for BigQuery
        df.columns = [sanitize_name(col) for col in df.columns]
        
        # 3. Prepare BigQuery Load Job
        sanitized_table_name = sanitize_name(table_name)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{sanitized_table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" if truncate else "WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET, # Efficient format
        )
        
        # 4. Execute Load Job
        print(f"  Uploading to BigQuery ({len(df)} rows) -> {table_ref}...")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        print(f"  Successfully loaded {table_name} to BigQuery.")
        
    except Exception as e:
        print(f"  Error processing {table_name}: {e}")

def run_ingestion(tables=None, truncate=False):
    """Runs the ingestion process for specified tables or all tables."""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Ensure dataset exists
    try:
        client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")
    except NotFound:
        print(f"Dataset {DATASET_ID} not found. Creating it...")
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        dataset.location = "US"  # Adjust as needed
        client.create_dataset(dataset)
        print(f"Dataset {DATASET_ID} created.")

    if not tables:
        # Fetch all tables if none specified
        engine = create_engine(DB_URL)
        list_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME ASC"
        tables = pd.read_sql(list_query, engine)['TABLE_NAME'].tolist()

    for table in tables:
        load_table_to_bq(client, table, truncate=truncate)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load data from SQL Server to BigQuery.")
    parser.add_argument("--tables", nargs="+", help="Specific tables to load (space-separated).")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before loading (Full Refresh).")
    
    args = parser.parse_args()
    
    run_ingestion(tables=args.tables, truncate=args.truncate)
