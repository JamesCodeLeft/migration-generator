import json
import os
import datetime
import re
from pathlib import Path

# SQL Server to BigQuery Type Mapping
TYPE_MAPPING = {
    "uniqueidentifier": "STRING", "nvarchar": "STRING", "ntext": "STRING", "varchar": "STRING",
    "text": "STRING", "int": "INT64", "bigint": "INT64", "smallint": "INT64", "tinyint": "INT64",
    "bit": "BOOL", "datetime": "DATETIME", "datetime2": "DATETIME", "date": "DATE",
    "float": "FLOAT64", "decimal": "NUMERIC", "numeric": "NUMERIC", "money": "NUMERIC",
    "char": "STRING", "nchar": "STRING", "binary": "BYTES", "varbinary": "BYTES", "image": "BYTES"
}

# --- PARTITION CONFIGURATION ---
# Key: Table Name, Value: Column to partition by
PARTITION_CONFIG = {
    "tblMemPayment": "post_date"
}

def get_bq_type(sql_server_type):
    """Maps SQL Server types to BigQuery types, defaulting to STRING if unknown."""
    return TYPE_MAPPING.get(sql_server_type.lower(), "STRING")

def sanitize_name(name):
    """Replaces any character that is NOT a letter, number, or underscore with an underscore."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def quote(name):
    """Wraps names in backticks for BigQuery compatibility."""
    return f"`{name}`"

def generate_create_table(table_data):
    """Generates a CREATE TABLE statement for BigQuery with optional partitioning."""
    table_name = table_data["table_name"]
    columns = []
    pk_columns = []
    fk_constraints = []
    
    for col in table_data["columns"]:
        # Sanitize column name for SQL output
        raw_col_name = col["name"]
        sanitized_col_name = sanitize_name(raw_col_name)
        
        col_name_quoted = quote(sanitized_col_name)
        col_type = get_bq_type(col["type"])
        
        definition = f"{col_name_quoted} {col_type}"
        if col.get("is_primary_key"):
            pk_columns.append(col_name_quoted)
            
        # Collect foreign keys
        fk = col.get("foreign_key")
        if fk:
            ref_table = quote(sanitize_name(fk["ref_table"]))
            ref_col = quote(sanitize_name(fk["ref_column"]))
            fk_constraints.append(f"FOREIGN KEY ({col_name_quoted}) REFERENCES {ref_table}({ref_col}) NOT ENFORCED")
            
        columns.append(definition)
    
    if pk_columns:
        pk_sql = f"PRIMARY KEY ({', '.join(pk_columns)}) NOT ENFORCED"
        columns.append(pk_sql)

    if fk_constraints:
        columns.extend(fk_constraints)

    cols_sql = ",\n    ".join(columns)
    sql = f"CREATE TABLE {quote(table_name)} (\n    {cols_sql}\n)"
    
    # Handle Partitioning (BigQuery specific)
    if table_name in PARTITION_CONFIG:
        # Note: Partition column name should also be sanitized if it comes from metadata
        partition_col = sanitize_name(PARTITION_CONFIG[table_name])
        sql += f"\nPARTITION BY DATE({quote(partition_col)})"
        
    return sql + ";"

def generate_add_column(table_name, column_data):
    """Generates an ALTER TABLE ... ADD COLUMN statement."""
    col_name = quote(sanitize_name(column_data["name"]))
    col_type = get_bq_type(column_data["type"])
    return f"ALTER TABLE {quote(table_name)} ADD COLUMN {col_name} {col_type};"

def get_metadata_dirs(base_dir):
    """
    Scans the base directory for timestamped folders.
    Returns (old_dir, new_dir) as Path objects.
    """
    base_path = Path(base_dir)
    if not base_path.exists():
        return None, None

    # Get all subdirectories and sort them alphabetically
    subdirs = sorted([d for d in base_path.iterdir() if d.is_dir()])
    
    if not subdirs:
        return None, None
    
    if len(subdirs) == 1:
        return None, subdirs[0]
    
    return subdirs[-2], subdirs[-1]

def run_migration_generator(base_metadata_dir, output_dir):
    old_path, new_path = get_metadata_dirs(base_metadata_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    if not new_path:
        print(f"No metadata directories found in {base_metadata_dir}")
        return

    print(f"Comparing: {old_path.name if old_path else '(Initial run)'} -> {new_path.name}")

    # Load old state
    old_tables = {}
    if old_path and old_path.exists():
        for f in old_path.glob("*.json"):
            with open(f, 'r') as j:
                data = json.load(j)
                old_tables[data["table_name"]] = data

    # Collect all changes first
    pending_migrations = []
    
    for f in sorted(new_path.glob("*.json")):
        if f.name == "migration_marker_log.txt":
            continue
            
        with open(f, 'r') as j:
            new_data = json.load(j)
            table_name = new_data["table_name"]
            
            if table_name not in old_tables:
                # New Table
                pending_migrations.append((f"create_{table_name}", generate_create_table(new_data)))
            else:
                # Compare Columns
                old_data = old_tables[table_name]
                old_col_names = {c["name"] for c in old_data["columns"]}
                
                for col in new_data["columns"]:
                    if col["name"] not in old_col_names:
                        # New Column
                        pending_migrations.append((f"add_{col['name']}_to_{table_name}", generate_add_column(table_name, col)))

    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = new_path / "migration_marker_log.txt"

    if not pending_migrations:
        print("No changes detected.")
        if new_path.exists():
            with open(log_path, 'w') as log_file:
                log_file.write(f"[{timestamp_str}] No changes\n")
        return

    # Generate separate files with incrementing timestamps
    base_time = datetime.datetime.now()
    generated_sql_commands = []

    for i, (desc, sql) in enumerate(pending_migrations):
        # Increment timestamp by 'i' seconds for strict ordering
        timestamp = (base_time + datetime.timedelta(seconds=i)).strftime("%Y%m%d%H%M%S")
        filename = f"{timestamp}__auto_{desc}.sql"
        file_path = output_path / filename
        
        with open(file_path, 'w') as f:
            f.write("-- migrate:up\n\n")
            f.write(sql)
            f.write("\n\n-- migrate:down\n")
        
        generated_sql_commands.append(sql)
        print(f"Generated: {filename}")

    # Write to marker log
    with open(log_path, 'w') as log_file:
        log_file.write(f"[{timestamp_str}] We generated {len(pending_migrations)} migration files:\n")
        log_file.write("\n".join(generated_sql_commands))
        log_file.write("\n")

    print(f"Log written to: {log_path}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up one level to reach the project root (~/datasets/PRIZM)
    project_root = os.path.dirname(script_dir)
    BASE_METADATA_DIR = os.path.join(project_root, "metadata")
    OUTPUT_DIR = os.path.join(project_root, "migrations")
    
    run_migration_generator(BASE_METADATA_DIR, OUTPUT_DIR)
