-- =====================================================================
-- AtmoSync: Database & Warehouse Provisioning
-- Maintained by: Member 2 (Snowflake & dbt)
-- =====================================================================

-- 1. Create Virtual Warehouse
CREATE WAREHOUSE IF NOT EXISTS ATMOSYNC_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Virtual warehouse for AtmoSync ingestion and dbt transformations';

-- 2. Create Database
CREATE DATABASE IF NOT EXISTS ATMOSYNC_DB
    COMMENT = 'AtmoSync IoT Micro-Climate Arbitrage Analytics Data Warehouse';

-- 3. Set Context
USE WAREHOUSE ATMOSYNC_WH;
USE DATABASE ATMOSYNC_DB;
