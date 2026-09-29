# AtmoSync — Snowflake Data Warehouse

**Module Owner:** Member 2 — Snowflake & dbt

This directory contains database DDL, schemas, tables, seed scripts, and ingestion utilities for the Snowflake Cloud Data Warehouse.

---

## Architecture

The Snowflake data warehouse is partitioned into four distinct schemas:
1. `RAW`: Direct ingestion target for streaming telemetry and static reference tables.
2. `STAGING`: Standardized, typed, and deduplicated views created by dbt.
3. `INTERMEDIATE`: Business transformation logic, rolling thermal excursion counters, and spoilage kinetics indices.
4. `ANALYTICS`: Production marts consumed by Apache Superset (`fct_spoilage_arbitrage`, `fct_container_health`, `dim_container`, `dim_market`).

---

## Setup Steps

### 1. Execute DDL Scripts in Snowflake Worksheets
Run the SQL scripts in this exact order:
1. `database.sql` — Creates `ATMOSYNC_WH` warehouse and `ATMOSYNC_DB` database.
2. `schema.sql` — Provisions `RAW`, `STAGING`, `INTERMEDIATE`, and `ANALYTICS` schemas.
3. `tables.sql` — Provisions `SENSOR_TELEMETRY`, `COMMODITY_PRICES`, `MARKETS`, and `ROUTES`.
4. `seed_data.sql` — Seeds reference data and baseline telemetry records.

### 2. Configure Local Credentials
Copy `.env.example` to `.env` and set your credentials:
```bash
SNOWFLAKE_ACCOUNT=your-account-id.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ATMOSYNC_ROLE
SNOWFLAKE_WAREHOUSE=ATMOSYNC_WH
SNOWFLAKE_DATABASE=ATMOSYNC_DB
SNOWFLAKE_SCHEMA=RAW
```

### 3. Ingest Data via Python
Load CSV reference data and streaming telemetry batches:
```bash
python snowflake/load_data.py
```
To run a local validation dry-run without credentials:
```bash
python snowflake/load_data.py --dry-run
```
