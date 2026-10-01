AtmoSync — Data Pipeline Operations & Runbook
This document describes the operational lifecycle, data ingestion mechanisms, batching policies, and alerting triggers of the AtmoSync pipeline.
1. Streaming Lifecycle
[IoT Telematics Sensor]
        │ (2-sec intervals)
        ▼
[Kafka Producer: topic 'sensor-data']
        │ (Partitioned by container_id)
        ▼
[Kafka Consumer & Pydantic Validator]
        │
        ├── Critical Anomaly Detected ────► [Kafka Topic: 'alerts'] ──► (Slack/Email Webhook)
        │
        └── Clean Telematics Batch ───────► [Snowflake RAW.SENSOR_TELEMETRY]
                                                    │
                                                    ▼
                                            [dbt Scheduled Run]
                                                    │
                                                    ▼
                                            [Superset BI Refresh]
2. Ingestion & Batching Strategy
Micro-Batching: Telematics packets are buffered in memory by the Kafka consumer until either:
batch_size (default: 10–20 records) is reached, OR
flush_interval (default: 5.0 seconds) expires.
Deduplication: Ingestion queries and dbt staging models use windowing (ROW_NUMBER() OVER (PARTITION BY container_id, timestamp ORDER BY ingested_at DESC)) to eliminate duplicate readings during network retries.
Dead-Letter Handling: Records failing Pydantic validation are logged with root-cause diagnostics and written to an error staging queue.
3. Transformation Cadence
Staging Layer: Materialized as lightweight SQL views for zero-latency queries.
Intermediate Layer: Ephemeral CTE pipelines computing moving averages and spoilage kinetics on demand.
Analytics Marts: Materialized as physical tables refreshed on a 1-to-5 minute schedule or triggered via Snowflake Tasks/Airflow.
4. Alerting & Incident Response
When risk_level transitions to Critical or spoilage_risk_score >= 75:
The consumer posts an alert event containing container coordinates, thermal readings, and root cause reasons to the alerts Kafka topic.
The Spoilage Arbitrage mart calculates alternative routes immediately.
If an alternative market offers positive arbitrage and is reachable within remaining $TTS$, a dispatch notification (EXECUTE_REROUTE) is presented on the Superset dashboard.