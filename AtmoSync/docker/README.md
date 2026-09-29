# AtmoSync — Docker Infrastructure

This directory contains the local streaming infrastructure orchestration files for running Apache Kafka and the Kafka Web UI.

## Services
1. **Kafka (KRaft Mode)**: Single-node Apache Kafka cluster running without ZooKeeper dependency on port `9092`.
2. **Kafka UI**: Web console running on port `8080` for visual topic inspection, throughput monitoring, and message tracing.

## Quickstart

### Start Services
```bash
docker compose up -d
```

### Verify Running Containers
```bash
docker compose ps
```

### Access Kafka UI
Navigate to [http://localhost:8080](http://localhost:8080) in your web browser.

### Stop Infrastructure
```bash
docker compose down -v
```
