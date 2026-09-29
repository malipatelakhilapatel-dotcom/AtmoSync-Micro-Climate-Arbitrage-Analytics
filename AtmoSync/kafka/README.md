# AtmoSync — Kafka Streaming Pipeline

## Overview
This module handles real-time message streaming for container telematics using **Apache Kafka** (Member 1 responsibility).

## Topics
- `sensor-data`: Main stream receiving raw IoT telemetry from active containers (3 partitions).
- `alerts`: Filtered high-priority stream receiving critical threshold anomaly alerts.

## Scripts
- `create_topics.py`: Idempotent topic provisioning via Kafka Admin Client.
- `producer.py`: High-throughput, resilient producer with backoff retries and JSON serialization.
- `consumer.py`: Consumer with Pydantic schema validation, threshold alerting, and micro-batch staging to Snowflake.

## Usage

### 1. Provision Topics
Ensure Kafka is running (e.g., via `docker compose up -d` in `docker/`), then run:
```bash
python create_topics.py
```

### 2. Start Kafka Producer
```bash
# Stream 5 containers continuously every 2 seconds
python producer.py --containers 5 --interval 2.0

# Dry-run mode (runs without active broker)
python producer.py --dry-run --containers 3 --iterations 5
```

### 3. Start Kafka Consumer
```bash
python consumer.py --batch-size 10
```

The consumer will automatically detect if Snowflake credentials are configured in `.env`. If present, it executes batch SQL `INSERT` statements into `ATMOSYNC_DB.RAW.SENSOR_TELEMETRY`. If absent, it stages sanitized events to `data/staged_telemetry.json` for offline inspection and testing.
