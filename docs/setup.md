# AtmoSync — Local Setup & Installation Guide

**Author:** Member 3 — Superset & Documentation

Follow this step-by-step guide to run the entire AtmoSync pipeline locally or connect it to your Snowflake cloud warehouse.

---

## 1. Prerequisites

- **Python**: 3.9+ (Python 3.10–3.14 supported)
- **Docker & Docker Compose**: Installed and running
- **Git**: Installed
- *(Optional)* Snowflake Account: Required only for production cloud warehouse execution; a local offline test mode is included out of the box.

---

## 2. Environment Setup

### Step 1: Clone and Navigate
```bash
git clone https://github.com/your-org/AtmoSync.git
cd AtmoSync
```

### Step 2: Configure Environment Variables
Copy the template configuration:
```bash
cp .env.example .env
```
Open `.env` in your editor and configure Snowflake credentials if available:
```env
SNOWFLAKE_ACCOUNT=xy12345.ap-south-1.aws
SNOWFLAKE_USER=atmosync_svc
SNOWFLAKE_PASSWORD=YourSecurePasswordHere123!
SNOWFLAKE_ROLE=ATMOSYNC_ROLE
SNOWFLAKE_WAREHOUSE=ATMOSYNC_WH
SNOWFLAKE_DATABASE=ATMOSYNC_DB
SNOWFLAKE_SCHEMA=RAW

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

---

## 3. Launch Streaming Infrastructure (Docker Kafka)

### Step 1: Start Kafka & Kafka UI
```bash
docker compose -f docker/docker-compose.yml up -d
```
Verify containers are healthy:
```bash
docker compose -f docker/docker-compose.yml ps
```

### Step 2: Provision Topics
```bash
python kafka/create_topics.py
```
This idempotently creates `sensor-data` and `spoilage-alerts`.

### Step 3: Inspect Kafka UI
Open [http://localhost:8080](http://localhost:8080) in your browser.

---

## 4. Execute Streaming Pipeline

### Step 1: Run Producer
In Terminal 1, start generating container telemetry:
```bash
python kafka/producer.py --num-containers 5 --iterations 20 --interval 1.5
```

### Step 2: Run Consumer
In Terminal 2, start consuming and staging records:
```bash
python kafka/consumer.py --batch-size 20
```

*(Note: If running without Docker, add `--mock` to both producer and consumer commands).*

---

## 5. Snowflake & dbt Setup

### Step 1: Provision Snowflake Tables
Log in to your Snowflake Web Worksheet and run:
1. `snowflake/database.sql`
2. `snowflake/schema.sql`
3. `snowflake/tables.sql`
4. `snowflake/seed_data.sql`

### Step 2: Load Telemetry to Snowflake
```bash
python snowflake/load_data.py
```
*(Or `python snowflake/load_data.py --dry-run` for offline local verification).*

### Step 3: Run dbt Transformations
```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
dbt debug --project-dir dbt
dbt run --project-dir dbt
dbt test --project-dir dbt
```

---

## 6. View Visualizations & Web Preview

Open the interactive dashboard preview in any browser:
```bash
python -m http.server 3000 --directory superset/web_preview
```
Visit [http://localhost:3000](http://localhost:3000) to inspect the live map, time series, and arbitrage matrix.

---

## 7. Run Unit & Integration Tests

Execute the automated test suite:
```bash
pytest tests/ -v
```