# AtmoSync — dbt Core Transformation Project

**Module Owner:** Member 2 — Snowflake & dbt

This directory contains the **dbt Core** project that structures raw streaming IoT telemetry into business-ready dimensional and fact analytics models within Snowflake.

---

## Model Lineage & Architecture

```mermaid
flowchart TD
    subgraph RAW["Snowflake RAW Landing"]
        RAW_ST["RAW.SENSOR_TELEMETRY"]
        RAW_CP["RAW.COMMODITY_PRICES"]
        RAW_MK["RAW.MARKETS"]
        RAW_RT["RAW.ROUTES"]
    end

    subgraph STG["Staging Layer (Views)"]
        STG_ST["stg_sensor_telemetry"]
        STG_CP["stg_commodity_prices"]
        STG_MK["stg_markets"]
        STG_RT["stg_routes"]
    end

    subgraph INT["Intermediate Business Layer (Views)"]
        INT_H["int_container_health<br/>(Thermal/Vibration Excursions)"]
        INT_S["int_spoilage_risk<br/>(Risk Score 0-100 & Time to Spoilage)"]
        INT_M["int_market_options<br/>(Transit Feasibility & Realizable Value)"]
    end

    subgraph MARTS["Analytics Marts (Tables)"]
        FCT_ARB["fct_spoilage_arbitrage<br/>(Core Arbitrage Opportunity Matrix)"]
        FCT_HLT["fct_container_health<br/>(Time-Series Metrics)"]
        DIM_C["dim_container<br/>(Active Fleet Dimension)"]
        DIM_M["dim_market<br/>(Wholesale Terminals)"]
    end

    RAW_ST --> STG_ST
    RAW_CP --> STG_CP
    RAW_MK --> STG_MK
    RAW_RT --> STG_RT

    STG_ST --> INT_H
    INT_H --> INT_S
    INT_S --> INT_M
    STG_MK --> INT_M
    STG_CP --> INT_M

    INT_M --> FCT_ARB
    STG_ST --> FCT_HLT
    STG_ST --> DIM_C
    STG_MK --> DIM_M
```

---

## Model Summaries

| Model | Layer | Materialization | Description |
|---|---|---|---|
| `stg_sensor_telemetry` | Staging | View | Cleans, typecasts, and tags physical envelope alert flags |
| `stg_commodity_prices` | Staging | View | Normalizes wholesale commodity spot prices |
| `stg_markets` | Staging | View | Standardizes terminal market coordinates and fee rates |
| `stg_routes` | Staging | View | Standardizes logistics corridors and transit times |
| `int_container_health` | Intermediate | View | Tracks container recency and thermal deviations |
| `int_spoilage_risk` | Intermediate | View | Computes 0–100 Spoilage Risk Score and Hours to Spoilage |
| `int_market_options` | Intermediate | View | Evaluates candidate destinations against shelf life |
| `fct_spoilage_arbitrage` | Marts | Table | **Primary Mart**: Computes Net Arbitrage Benefit & Reroute Recommendations |
| `fct_container_health` | Marts | Table | Time-series container environmental health log |
| `dim_container` | Marts | Table | Master container fleet dimension |
| `dim_market` | Marts | Table | Master wholesale terminal dimension |

---

## Execution Instructions

### 1. Setup Profile
Copy `profiles.yml.example` to your local `~/.dbt/profiles.yml`:
```bash
cp profiles.yml.example ~/.dbt/profiles.yml
```

### 2. Test Connection
```bash
dbt debug --project-dir dbt
```

### 3. Run Models
```bash
dbt run --project-dir dbt
```

### 4. Run Data Quality Tests
```bash
dbt test --project-dir dbt
```
