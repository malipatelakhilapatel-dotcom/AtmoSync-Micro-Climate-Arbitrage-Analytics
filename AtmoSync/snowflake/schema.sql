-- =====================================================================
-- AtmoSync: Schema Provisioning
-- Maintained by: Member 2 (Snowflake & dbt)
-- =====================================================================

USE DATABASE ATMOSYNC_DB;

-- Schema for raw ingested telemetry and reference seed tables
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw landing zone for Kafka IoT telemetry and reference datasets';

-- Schema for dbt cleaned and standardized models
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned, typed, and normalized staging tables and views';

-- Schema for intermediate calculations (health scores, spoilage kinetics)
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'Intermediate business logic, risk indices, and corridor projections';

-- Schema for production analytics marts and Superset dashboards
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Dimensional models and Spoilage Arbitrage fact tables consumed by Superset';
