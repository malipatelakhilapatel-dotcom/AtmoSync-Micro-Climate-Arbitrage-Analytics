-- =====================================================================
-- AtmoSync: Raw Landing Tables DDL
-- Maintained by: Member 2 (Snowflake & dbt)
-- =====================================================================

USE DATABASE ATMOSYNC_DB;
USE SCHEMA RAW;

-- 1. Raw IoT Sensor Telemetry Table
CREATE TABLE IF NOT EXISTS SENSOR_TELEMETRY (
    record_id              VARCHAR(64) DEFAULT UUID_STRING(),
    ingestion_timestamp    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    timestamp              TIMESTAMP_NTZ NOT NULL,
    container_id           VARCHAR(50) NOT NULL,
    shipment_id            VARCHAR(50) NOT NULL,
    commodity              VARCHAR(50) NOT NULL,
    temperature_c          NUMBER(5, 2) NOT NULL,
    humidity_pct           NUMBER(5, 2) NOT NULL,
    vibration_g            NUMBER(5, 2) NOT NULL,
    latitude               NUMBER(9, 6) NOT NULL,
    longitude              NUMBER(9, 6) NOT NULL,
    origin                 VARCHAR(100) NOT NULL,
    destination            VARCHAR(100) NOT NULL,
    quantity_kg            NUMBER(10, 0) NOT NULL,
    speed_kmh              NUMBER(5, 2) DEFAULT 0.0,
    door_open              BOOLEAN DEFAULT FALSE,
    compressor_status      VARCHAR(20) DEFAULT 'NORMAL',
    anomaly_mode           VARCHAR(50) DEFAULT 'NORMAL'
)
COMMENT = 'Streaming container telemetry ingested from Kafka topic sensor-data';

-- 2. Commodity Wholesale Spot Prices Table
CREATE TABLE IF NOT EXISTS COMMODITY_PRICES (
    market                 VARCHAR(100) NOT NULL,
    commodity              VARCHAR(50) NOT NULL,
    price_per_kg           NUMBER(10, 2) NOT NULL,
    currency               VARCHAR(10) DEFAULT 'INR',
    last_updated           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (market, commodity)
)
COMMENT = 'Wholesale spot prices by market terminal and agricultural commodity';

-- 3. Markets Reference Table
CREATE TABLE IF NOT EXISTS MARKETS (
    market_id                  VARCHAR(20) PRIMARY KEY,
    market_name                VARCHAR(100) NOT NULL UNIQUE,
    latitude                   NUMBER(9, 6) NOT NULL,
    longitude                  NUMBER(9, 6) NOT NULL,
    state                      VARCHAR(100) NOT NULL,
    tier                       VARCHAR(20) NOT NULL,
    handling_fee_pct           NUMBER(4, 3) DEFAULT 0.020,
    cold_storage_capacity_tons NUMBER(10, 0) DEFAULT 3000
)
COMMENT = 'Wholesale terminal markets, locations, handling fees, and storage capacities';

-- 4. Routes Logistics Corridor Table
CREATE TABLE IF NOT EXISTS ROUTES (
    route_id                   VARCHAR(50) PRIMARY KEY,
    origin                     VARCHAR(100) NOT NULL,
    destination                VARCHAR(100) NOT NULL,
    distance_km                NUMBER(8, 2) NOT NULL,
    estimated_transit_hours    NUMBER(6, 2) NOT NULL,
    toll_cost_inr              NUMBER(10, 2) DEFAULT 0.0,
    road_condition_index       NUMBER(4, 2) DEFAULT 0.85
)
COMMENT = 'Standard transit corridors with baseline distance and estimated transit time';
