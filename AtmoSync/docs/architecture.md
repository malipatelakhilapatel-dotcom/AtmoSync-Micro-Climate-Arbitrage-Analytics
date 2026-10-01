# AtmoSync System Architecture

**Author:** Member 3 — Superset & Documentation  
**Reviewers:** Member 1 (IoT & Kafka), Member 2 (Snowflake & dbt)

---

## 1. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph Edge["1. Edge & Telemetry Simulation (Member 1)"]
        A1["Reefer Container Sensors<br/>(Temp, Hum, Vib, GPS)"]
        A2["IoT Telemetry Simulator<br/>(simulator/iot_simulator.py)"]
        A3["Pydantic Telemetry Validation"]
        A1 --> A2 --> A3
    end

    subgraph Streaming["2. Streaming Infrastructure (Member 1)"]
        B1["Kafka Producer<br/>(kafka/producer.py)"]
        B2["Kafka Broker (Docker KRaft)<br/>Topic: sensor-data"]
        B3["Kafka Broker (Docker KRaft)<br/>Topic: spoilage-alerts"]
        B4["Kafka Consumer<br/>(kafka/consumer.py)"]
        B5["Dead-Letter Queue (DLQ)<br/>(data/dead_letter_records.json)"]

        A3 --> B1
        B1 -->|All Telemetry| B2
        B1 -->|Critical Excursions| B3
        B2 --> B4
        B4 -->|Corrupted Schema| B5
    end

    subgraph DW["3. Snowflake Data Warehouse (Member 2)"]
        C1["Snowflake Ingestion Loader<br/>(snowflake/load_data.py)"]
        C2[("RAW.SENSOR_TELEMETRY")]
        C3[("RAW.COMMODITY_PRICES")]
        C4[("RAW.MARKETS")]
        C5[("RAW.ROUTES")]

        B4 -->|Batch Staging| C1
        C1 --> C2
        C3 -.-> C1
        C4 -.-> C1
        C5 -.-> C1
    end

    subgraph Transformation["4. dbt Transformation Layer (Member 2)"]
        D1["dbt Staging Models<br/>(stg_sensor_telemetry, stg_markets, etc.)"]
        D2["int_container_health<br/>(Thermal/Vibration Excursions)"]
        D3["int_spoilage_risk<br/>(Risk Score 0-100 & Time to Spoilage)"]
        D4["int_market_options<br/>(Haversine Distance & Transit Feasibility)"]
        D5[("fct_spoilage_arbitrage")]
        D6[("fct_container_health")]
        D7[("dim_container")]
        D8[("dim_market")]

        C2 --> D1
        C3 --> D1
        C4 --> D1
        C5 --> D1

        D1 --> D2 --> D3 --> D4 --> D5
        D1 --> D6
        D1 --> D7
        D1 --> D8
    end

    subgraph Analytics["5. Operations & Decision Support (Member 3)"]
        E1["Apache Superset BI Console"]
        E2["Trader Spoilage Arbitrage Matrix"]
        E3["Interactive Web Preview (Local GIS & KPIs)"]
        E4["Fleet Dispatch Order Notification"]

        D5 --> E1
        D6 --> E1
        D7 --> E1
        D8 --> E1

        E1 --> E2
        D5 --> E3
        E2 --> E4
    end
```

---

## 2. Component Responsibilities by Team Member

### Member 1 — IoT & Kafka
- **IoT Simulator (`simulator/`)**: Models thermodynamic decay curves, refrigerant leakage, compressor failures, and vibration shock.
- **Kafka Streaming (`kafka/`, `docker/`)**: Docker Compose KRaft cluster, topic provisioning, reliable producer with exponential backoff, schema-validating consumer with dead-letter queue routing.

### Member 2 — Snowflake & dbt
- **Warehouse DDL (`snowflake/`)**: Scalable table structures across `RAW`, `STAGING`, `INTERMEDIATE`, and `ANALYTICS`.
- **dbt Transformation (`dbt/`)**: Clean staging, intermediate kinetics logic (Arrhenius-type shelf-life collapse), multi-market transit evaluation, and the final `fct_spoilage_arbitrage` mart.

### Member 3 — Superset & Documentation
- **Visualization (`superset/`)**: 7 KPI cards, 7 charts (Deck.gl map, time-series, leaderboard, matrix), native filter architecture, Superset dashboard export bundle, and local web preview.
- **Documentation & Verification (`docs/`, `tests/`)**: System architecture, setup guides, data dictionary, mathematical derivations, end-to-end demo walkthroughs.

---

## 3. Data Flow Latency & Resilience

1. **Ingestion Latency**: Sub-second from simulator to Kafka topic.
2. **Buffer Staging**: Batches are buffered into micro-batches (every 5–10 seconds or 50 records) before bulk insertion into Snowflake `RAW.SENSOR_TELEMETRY`.
3. **Idempotency & Deduplication**: Telemetry records are keyed by `container_id` and unique `record_id`. dbt models deduplicate rows via window functions (`ROW_NUMBER() OVER (PARTITION BY container_id ORDER BY telemetry_timestamp DESC)`).
4. **Resilience**: If Kafka is temporarily down, the producer triggers exponential retry (1s, 2s, 4s, 8s, 16s). If an invalid payload is received, it is routed to `data/dead_letter_records.json` without crashing the ingestion consumer.