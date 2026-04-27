# PRIZM Migration Scripts

This directory contains scripts specific to the PRIZM data migration project.

- **scripts/**: Python logic for metadata extraction, migration generation, and data loading.
  - `generate_metadata.py`: Extracts schema.
  - `generate_migrations.py`: Generates SQL migrations.
  - `load_data.py`: Loads data from SQL Server to BigQuery.
- **metadata/**: (Auto-generated) JSON schema snapshots.
- **migrations/**: (Auto-generated) BigQuery SQL files.
