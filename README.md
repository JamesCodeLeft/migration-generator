# Migration Generator

A toolset for extracting database schema metadata from SQL Server and generating BigQuery-compatible SQL migrations.

## Project Structure

- `PRIZM/scripts/`: Contains the core Python scripts.
  - `generate_metadata.py`: Extracts schema information (tables, columns, keys) from SQL Server.
  - `generate_migrations.py`: Compares metadata versions and generates `.sql` migration files.
- `PRIZM/metadata/`: (Generated) Stores timestamped JSON snapshots of the database schema.
- `PRIZM/migrations/`: (Generated) Stores the generated SQL migration files.

## Prerequisites

- Python 3.x
- `pandas`
- `sqlalchemy`
- `pyodbc`
- ODBC Driver for SQL Server

## Usage

### 1. Generate Metadata
Run this script to capture the current state of the SQL Server database. It will create a new timestamped folder in `PRIZM/metadata/`.

```bash
python PRIZM/scripts/generate_metadata.py
```

### 2. Generate Migrations
Run this script to compare the latest two metadata snapshots and generate BigQuery migration scripts for any new tables or columns.

```bash
python PRIZM/scripts/generate_migrations.py
```

The generated migrations follow the `dbmate` format (using `-- migrate:up` and `-- migrate:down`) and are saved in `PRIZM/migrations/`.

### 3. Load Data to BigQuery
Run this script to extract data from SQL Server and upload it directly to BigQuery.

```bash
# Set your GCP Project ID and BigQuery Dataset ID
export GCP_PROJECT_ID="your-project-id"
export BQ_DATASET_ID="prizm_data"

# Optional: Provide GOOGLE_APPLICATION_CREDENTIALS if not using gcloud auth
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-file.json"

# Load all tables
python PRIZM/scripts/load_data.py

# Load specific tables with full refresh (truncate)
python PRIZM/scripts/load_data.py --tables tblMemPayment tblMembers --truncate
```

## Configuration

- **Database Connection**: Update `DB_URL` in `PRIZM/scripts/generate_metadata.py` with your SQL Server credentials.
- **Type Mapping**: Modify `TYPE_MAPPING` in `PRIZM/scripts/generate_migrations.py` to adjust how SQL Server types are mapped to BigQuery types.
- **Partitioning**: Update `PARTITION_CONFIG` in `PRIZM/scripts/generate_migrations.py` to specify BigQuery partition columns for specific tables.
