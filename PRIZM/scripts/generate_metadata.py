import pandas as pd
import json
import os
from sqlalchemy import create_engine
from datetime import date, datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
DB_URL = os.getenv("DB_URL")
# Using relative path from 'scripts' folder to 'metadata' folder
BASE_METADATA_DIR = os.path.join(os.path.dirname(__file__), "..", "metadata")
SAMPLE_PCT = 100 

# Helper to handle non-JSON-serializable types (dates, decimals)
def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def generate_metadata():
    try:
        # Create timestamped folder name (e.g., 20260420_1000)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        metadata_run_dir = os.path.join(BASE_METADATA_DIR, timestamp)
        
        # Ensure directory exists
        os.makedirs(metadata_run_dir, exist_ok=True)
        print(f"Starting extraction to: {metadata_run_dir}")
        
        engine = create_engine(DB_URL)
        print("Fetching table list...")
        list_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME ASC"
        all_tables = pd.read_sql(list_query, engine)['TABLE_NAME'].tolist()
        
        for table in all_tables:
            # 1. Get Total Row Count
            row_count_query = f"SELECT COUNT(*) as TotalRows FROM [{table}]"
            try:
                total_rows = int(pd.read_sql(row_count_query, engine).iloc[0]['TotalRows'])
            except Exception as e:
                print(f"  Error getting row count for {table}: {e}")
                continue

            if total_rows == 0:
                print(f"  Skipping {table} (0 rows)")
                continue

            print(f"\n--- Processing: {table} ({total_rows} rows) ---")

            # 2. Get Keys (PK/FK)
            pk_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{table}'"
            pk_cols = set(pd.read_sql(pk_query, engine)['COLUMN_NAME'].tolist())

            fk_query = f"""
            SELECT parent_col.name AS LocalCol, ref_table.name AS RefTable, ref_col.name AS RefCol
            FROM sys.foreign_key_columns AS fkc
            INNER JOIN sys.tables AS parent_table ON fkc.parent_object_id = parent_table.object_id
            INNER JOIN sys.columns AS parent_col ON fkc.parent_object_id = parent_col.object_id AND fkc.parent_column_id = parent_col.column_id
            INNER JOIN sys.tables AS ref_table ON fkc.referenced_object_id = ref_table.object_id
            INNER JOIN sys.columns AS ref_col ON fkc.referenced_object_id = ref_col.object_id AND fkc.referenced_column_id = ref_col.column_id
            WHERE parent_table.name = '{table}'
            """
            fk_df = pd.read_sql(fk_query, engine)
            fk_map = {row['LocalCol']: {"ref_table": row['RefTable'], "ref_column": row['RefCol']} for _, row in fk_df.iterrows()}

            # 3. Get Column Data
            meta_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
            cols_df = pd.read_sql(meta_query, engine)
            
            table_metadata = {
                "table_name": table,
                "total_rows": total_rows,
                "columns": []
            }

            for _, row in cols_df.iterrows():
                col = row['COLUMN_NAME']
                dtype = row['DATA_TYPE'].lower()
                
                col_info = {
                    "name": col,
                    "type": dtype,
                    "is_primary_key": col in pk_cols,
                    "foreign_key": fk_map.get(col, None),
                    "null_count": 0,
                    "unique_count": 0,
                    "min": None,
                    "max": None
                }
                table_metadata["columns"].append(col_info)

            # 4. Save to JSON
            json_path = os.path.join(metadata_run_dir, f"{table}.json")
            with open(json_path, 'w') as f:
                json.dump(table_metadata, f, indent=4, default=json_serial)
            
            print(f"  Successfully saved {table}.json")

        print(f"\nExtraction complete. Metadata saved in: {metadata_run_dir}")

    except Exception as e:
        print(f"Critical Error: {e}")

if __name__ == "__main__":
    generate_metadata()

