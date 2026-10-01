-- =============================================================================
-- AtmoSync: Micro-Climate Arbitrage Analytics — Superset Production SQL Queries
-- Module: Superset & BI Layer (Member 3)
-- Target Database: ATMOSYNC_DB
-- Target Warehouse: ATMOSYNC_WH
-- Target Role: ATMOSYNC_ROLE
-- Target Schemas: ANALYTICS, INTERMEDIATE, RAW
-- =============================================================================
--
-- This script contains all ready-to-use production SQL queries powering the
-- AtmoSync Micro-Climate Arbitrage Console in Apache Superset.
--
-- TABLE OF CONTENTS:
-- 1. Database & Schema Initialization Context
-- 2. Top KPI Ribbon Queries (Cards 1 to 7 + Consolidated Executive Summary)
-- 3. Core Visualizations & Chart Queries (Charts 1 to 7)
--    - Chart 1: Container Health Geographic Scatter Map (Deck.gl / Mapbox)
--    - Chart 2: Micro-Climate Core Temperature Trends (Time-Series Line)
--    - Chart 3: Micro-Climate Relative Humidity Trends (Time-Series Line)
--    - Chart 4: Spoilage Risk Leaderboard (Ranked Horizontal Bar)
--    - Chart 5: Real-Time Spoilage Arbitrage Opportunity Matrix (Interactive Grid)
--    - Chart 6: Multi-Market Revenue Realization Comparison (Grouped Bar)
--    - Chart 7: Logistics Corridor Route Performance & Vulnerability (Bubble Plot)
-- 4. Native Filter Bar Queries (Dropdown Select Datasets)
-- 5. Curated Virtual Datasets for Superset SQL Lab & Slices
-- 6. Operational Trader & Dispatch Action Queries
-- =============================================================================


-- =============================================================================
-- 1. DATABASE & SCHEMA CONTEXT
-- =============================================================================
USE DATABASE ATMOSYNC_DB;
USE SCHEMA ANALYTICS;


-- =============================================================================
-- 2. TOP KPI METRIC QUERIES (KPI RIBBON)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- KPI 1: Total Active Monitored Containers
-- Description: Total number of active cold-chain containers tracked across corridors.
-- Visualization: Big Number Total
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT container_id) AS total_active_containers
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE;


-- -----------------------------------------------------------------------------
-- KPI 2: Containers At Risk (High or Critical Risk)
-- Description: Units exhibiting biological thermal/mechanical degradation.
-- Visualization: Big Number Total (Amber Alert)
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT container_id) AS containers_at_risk
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE risk_category IN ('High Risk', 'Critical Risk');


-- -----------------------------------------------------------------------------
-- KPI 3: Critical Containers Requiring Immediate Intervention
-- Description: Units at imminent risk of total commercial write-off.
-- Visualization: Big Number Total (Red Pulse)
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT container_id) AS critical_containers
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE risk_category = 'Critical Risk';


-- -----------------------------------------------------------------------------
-- KPI 4: Fleet Average Micro-Climate Temperature
-- Description: Average internal cargo temperature across all active containers.
-- Visualization: Numeric Gauge / Big Number (°C)
-- Target Datasource: ANALYTICS.FCT_CONTAINER_HEALTH
-- -----------------------------------------------------------------------------
SELECT
    ROUND(AVG(temperature_c), 1) AS avg_temperature_c
FROM ANALYTICS.FCT_CONTAINER_HEALTH;


-- -----------------------------------------------------------------------------
-- KPI 5: Fleet Average Micro-Climate Humidity
-- Description: Average relative humidity inside containers (condensation proxy).
-- Visualization: Numeric Gauge / Big Number (%)
-- Target Datasource: ANALYTICS.FCT_CONTAINER_HEALTH
-- -----------------------------------------------------------------------------
SELECT
    ROUND(AVG(humidity_pct), 1) AS avg_humidity_pct
FROM ANALYTICS.FCT_CONTAINER_HEALTH;


-- -----------------------------------------------------------------------------
-- KPI 6: Total Net Realizable Spoilage Arbitrage Opportunity
-- Description: Aggregate financial gain unlocked by executing recommended reroutes.
-- Visualization: Big Number Total (Currency: INR ₹)
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    ROUND(SUM(spoilage_arbitrage_inr), 0) AS total_arbitrage_opportunity_inr
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE is_reroute_recommended = TRUE;


-- -----------------------------------------------------------------------------
-- KPI 7: Total Cargo Value Preserved / Loss Avoided
-- Description: Financial value of agricultural cargo saved from spoilage write-off.
-- Visualization: Big Number Total (Currency: INR ₹)
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    ROUND(SUM(spoilage_avoided_value_inr), 0) AS total_loss_avoided_inr
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE is_reroute_recommended = TRUE;


-- -----------------------------------------------------------------------------
-- Bonus KPI Query: Consolidated Executive Fleet Overview (Single Row)
-- Description: Provides a single-query summary card combining all 7 core KPIs.
-- -----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT a.container_id) AS total_active_containers,
    COUNT(DISTINCT CASE WHEN a.risk_category IN ('High Risk', 'Critical Risk') THEN a.container_id END) AS containers_at_risk,
    COUNT(DISTINCT CASE WHEN a.risk_category = 'Critical Risk' THEN a.container_id END) AS critical_containers,
    ROUND(AVG(h.temperature_c), 1) AS fleet_avg_temp_c,
    ROUND(AVG(h.humidity_pct), 1) AS fleet_avg_humidity_pct,
    ROUND(COALESCE(SUM(CASE WHEN a.is_reroute_recommended = TRUE THEN a.spoilage_arbitrage_inr ELSE 0 END), 0), 0) AS total_arbitrage_opportunity_inr,
    ROUND(COALESCE(SUM(CASE WHEN a.is_reroute_recommended = TRUE THEN a.spoilage_avoided_value_inr ELSE 0 END), 0), 0) AS total_loss_avoided_inr
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE a
LEFT JOIN ANALYTICS.FCT_CONTAINER_HEALTH h
    ON a.container_id = h.container_id;


-- =============================================================================
-- 3. CORE VISUALIZATION CHARTS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Chart 1: Container Health Geographic Scatter Map
-- Description: Geospatial fleet positioning, risk category color markers, and
--              rerouting target destinations.
-- Visualization: Deck.gl Scatterplot / Mapbox Geospatial View
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    container_id,
    shipment_id,
    commodity,
    latitude,
    longitude,
    risk_category,
    spoilage_risk_score,
    time_to_spoilage_hours,
    current_destination,
    recommended_destination,
    spoilage_arbitrage_inr,
    is_reroute_recommended
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE;


-- -----------------------------------------------------------------------------
-- Chart 2: Micro-Climate Core Temperature Trends
-- Description: Multi-container time-series tracking internal reefer temperature
--              excursions across transit timelines.
-- Visualization: ECharts Time-Series Multi-Line Chart
-- Target Datasource: ANALYTICS.FCT_CONTAINER_HEALTH
-- -----------------------------------------------------------------------------
SELECT
    telemetry_timestamp,
    container_id,
    commodity,
    temperature_c,
    is_temperature_alert
FROM ANALYTICS.FCT_CONTAINER_HEALTH
ORDER BY telemetry_timestamp ASC;


-- -----------------------------------------------------------------------------
-- Chart 3: Micro-Climate Relative Humidity Trends
-- Description: Multi-container time-series tracking humidity spikes and
--              mold/condensation danger thresholds (>95%).
-- Visualization: ECharts Time-Series Multi-Line Chart
-- Target Datasource: ANALYTICS.FCT_CONTAINER_HEALTH
-- -----------------------------------------------------------------------------
SELECT
    telemetry_timestamp,
    container_id,
    commodity,
    humidity_pct,
    is_humidity_alert
FROM ANALYTICS.FCT_CONTAINER_HEALTH
ORDER BY telemetry_timestamp ASC;


-- -----------------------------------------------------------------------------
-- Chart 4: Spoilage Risk Leaderboard
-- Description: Ranked horizontal bar chart visualizing units by degradation risk score.
-- Visualization: ECharts Horizontal Bar Chart
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    container_id || ' (' || commodity || ')' AS container_label,
    container_id,
    commodity,
    spoilage_risk_score,
    risk_category,
    time_to_spoilage_hours
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY spoilage_risk_score DESC;


-- -----------------------------------------------------------------------------
-- Chart 5: Real-Time Spoilage Arbitrage Opportunity Matrix
-- Description: Operational matrix for commodity desks comparing planned vs rerouted
--              destinations, remaining shelf life, and net financial arbitrage benefit.
-- Visualization: Interactive Table with Conditional Color Formatting
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    container_id AS "Container",
    commodity AS "Cargo",
    risk_category AS "Risk State",
    time_to_spoilage_hours AS "Shelf Life (Hrs)",
    current_destination AS "Current Dest",
    current_expected_value_inr AS "Current NRV (₹)",
    recommended_destination AS "Recommended Dest",
    reroute_expected_value_inr AS "Reroute NRV (₹)",
    spoilage_arbitrage_inr AS "Arbitrage Benefit (₹)",
    recommendation_reason AS "Trader Recommendation"
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY spoilage_arbitrage_inr DESC;


-- -----------------------------------------------------------------------------
-- Chart 6: Multi-Market Revenue Realization Comparison
-- Description: Side-by-side grouped bar chart comparing Net Realizable Value
--              at the original destination vs candidate rerouted terminal.
-- Visualization: ECharts Grouped Bar Chart
-- Target Datasource: ANALYTICS.FCT_SPOILAGE_ARBITRAGE
-- -----------------------------------------------------------------------------
SELECT
    container_id,
    commodity,
    current_expected_value_inr AS "Current Planned Destination",
    reroute_expected_value_inr AS "Alternative Rerouted Destination",
    spoilage_arbitrage_inr AS "Net Arbitrage Uplift"
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY container_id ASC;


-- -----------------------------------------------------------------------------
-- Chart 7: Logistics Corridor Route Performance & Transit Vulnerability
-- Description: Transit corridor risk evaluation mapping travel distance, transit hours,
--              cargo quantity, and projected arrival spoilage percentage.
-- Visualization: ECharts Bubble Plot / Scatter
-- Target Datasource: INTERMEDIATE.INT_MARKET_OPTIONS
-- -----------------------------------------------------------------------------
SELECT
    container_id,
    commodity,
    candidate_market AS destination,
    distance_km,
    estimated_transit_hours,
    ROUND(projected_spoilage_rate * 100, 1) AS projected_spoilage_pct,
    quantity_kg,
    is_feasible_within_shelf_life
FROM INTERMEDIATE.INT_MARKET_OPTIONS;


-- =============================================================================
-- 4. NATIVE FILTER BAR QUERIES
-- =============================================================================

-- Filter 1: Container ID Filter Dropdown
SELECT DISTINCT
    container_id
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY container_id ASC;


-- Filter 2: Commodity Category Filter Dropdown
SELECT DISTINCT
    commodity
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY commodity ASC;


-- Filter 3: Origin Hub Filter Dropdown
SELECT DISTINCT
    origin
FROM ANALYTICS.FCT_CONTAINER_HEALTH
ORDER BY origin ASC;


-- Filter 4: Destination Market Filter Dropdown
SELECT DISTINCT
    destination
FROM ANALYTICS.FCT_CONTAINER_HEALTH
ORDER BY destination ASC;


-- Filter 5: Spoilage Risk Category Filter Dropdown
SELECT DISTINCT
    risk_category
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY
    CASE risk_category
        WHEN 'Critical Risk' THEN 1
        WHEN 'High Risk'     THEN 2
        WHEN 'Moderate Risk' THEN 3
        WHEN 'Low Risk'      THEN 4
        ELSE 5
    END ASC;


-- =============================================================================
-- 5. CURATED VIRTUAL DATASETS FOR SUPERSET SQL LAB & SLICES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Virtual Dataset 1: Consolidated Real-Time Fleet & Arbitrage View
-- Purpose: Primary dimensional dataset combining container state, current telemetry,
--          and destination market metadata.
-- -----------------------------------------------------------------------------
SELECT
    f.container_id,
    f.shipment_id,
    f.commodity,
    f.quantity_kg,
    f.latitude,
    f.longitude,
    f.spoilage_risk_score,
    f.risk_category,
    f.time_to_spoilage_hours,
    f.current_destination,
    f.current_transit_hours,
    f.current_market_price,
    f.current_expected_value_inr,
    f.current_potential_loss_inr,
    f.recommended_destination,
    f.reroute_distance_km,
    f.reroute_transit_hours,
    f.reroute_market_price,
    f.reroute_freight_cost_inr,
    f.reroute_expected_value_inr,
    f.spoilage_arbitrage_inr,
    f.spoilage_avoided_value_inr,
    f.is_reroute_recommended,
    f.recommendation_reason,
    m.state AS destination_state,
    m.tier AS destination_market_tier,
    m.cold_storage_capacity_tons
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE f
LEFT JOIN ANALYTICS.DIM_MARKET m
    ON f.recommended_destination = m.market_name;


-- -----------------------------------------------------------------------------
-- Virtual Dataset 2: Active Environmental Excursions & Hardware Alert Feed
-- Purpose: Real-time feed of sensor telemetry records that triggered threshold alerts.
-- -----------------------------------------------------------------------------
SELECT
    record_id,
    telemetry_timestamp,
    container_id,
    commodity,
    origin,
    destination,
    temperature_c,
    humidity_pct,
    vibration_g,
    compressor_status,
    door_open,
    is_temperature_alert,
    is_humidity_alert,
    is_vibration_alert,
    is_mechanical_alert,
    instant_risk_score,
    instant_risk_category
FROM ANALYTICS.FCT_CONTAINER_HEALTH
WHERE is_temperature_alert = 1
   OR is_humidity_alert = 1
   OR is_vibration_alert = 1
   OR is_mechanical_alert = 1
ORDER BY telemetry_timestamp DESC;


-- -----------------------------------------------------------------------------
-- Virtual Dataset 3: Commodity Portfolio Value-at-Risk Summary
-- Purpose: Aggregated breakdown of fleet exposure, risk levels, and arbitrage upside
--          segmented by crop commodity.
-- -----------------------------------------------------------------------------
SELECT
    commodity,
    COUNT(DISTINCT container_id) AS total_containers,
    SUM(quantity_kg) AS total_cargo_kg,
    ROUND(AVG(spoilage_risk_score), 1) AS avg_spoilage_risk,
    SUM(CASE WHEN risk_category IN ('High Risk', 'Critical Risk') THEN 1 ELSE 0 END) AS at_risk_container_count,
    ROUND(SUM(current_expected_value_inr), 0) AS total_current_nrv_inr,
    ROUND(SUM(current_potential_loss_inr), 0) AS total_potential_loss_inr,
    ROUND(SUM(CASE WHEN is_reroute_recommended = TRUE THEN spoilage_arbitrage_inr ELSE 0 END), 0) AS total_reroute_arbitrage_inr,
    ROUND(SUM(CASE WHEN is_reroute_recommended = TRUE THEN spoilage_avoided_value_inr ELSE 0 END), 0) AS total_spoilage_prevented_inr
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
GROUP BY commodity
ORDER BY total_potential_loss_inr DESC;


-- -----------------------------------------------------------------------------
-- Virtual Dataset 4: Logistics Corridor Vulnerability Index
-- Purpose: Evaluates route corridors combining distance, travel times, and
--          road condition indices against cold-chain cargo decay.
-- -----------------------------------------------------------------------------
SELECT
    r.route_id,
    r.origin,
    r.destination,
    r.distance_km,
    r.estimated_transit_hours,
    r.toll_cost_inr,
    r.road_condition_index,
    COUNT(DISTINCT h.container_id) AS active_shipments_in_corridor,
    ROUND(AVG(h.temperature_c), 1) AS avg_corridor_temp_c,
    ROUND(AVG(h.humidity_pct), 1) AS avg_corridor_humidity_pct
FROM RAW.ROUTES r
LEFT JOIN ANALYTICS.FCT_CONTAINER_HEALTH h
    ON r.origin = h.origin AND r.destination = h.destination
GROUP BY
    r.route_id,
    r.origin,
    r.destination,
    r.distance_km,
    r.estimated_transit_hours,
    r.toll_cost_inr,
    r.road_condition_index
ORDER BY r.distance_km ASC;


-- =============================================================================
-- 6. OPERATIONAL TRADER & DISPATCH ACTION QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Query: Urgent Reroute Dispatch Manifest
-- Purpose: Instant operational query providing truck drivers and dispatchers
--          with exact GPS coordinates, target terminal, and economic justification.
-- -----------------------------------------------------------------------------
SELECT
    f.container_id,
    f.shipment_id,
    f.commodity,
    f.quantity_kg,
    f.latitude AS current_latitude,
    f.longitude AS current_longitude,
    f.current_destination,
    f.recommended_destination,
    m.latitude AS target_market_lat,
    m.longitude AS target_market_lon,
    m.state AS target_state,
    m.tier AS target_tier,
    f.reroute_distance_km,
    f.reroute_transit_hours,
    f.time_to_spoilage_hours AS remaining_shelf_life_hours,
    ROUND(f.spoilage_arbitrage_inr, 0) AS net_arbitrage_profit_inr,
    f.recommendation_reason
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE f
JOIN ANALYTICS.DIM_MARKET m
    ON f.recommended_destination = m.market_name
WHERE f.is_reroute_recommended = TRUE
ORDER BY f.spoilage_arbitrage_inr DESC;


-- -----------------------------------------------------------------------------
-- Query: Container In-Depth Lifecycle Audit Trail
-- Purpose: Detailed chronological history of a specific container asset for
--          claims adjustment and post-transit route forensics.
-- -----------------------------------------------------------------------------
SELECT
    record_id,
    telemetry_timestamp,
    container_id,
    shipment_id,
    commodity,
    temperature_c,
    humidity_pct,
    vibration_g,
    latitude,
    longitude,
    speed_kmh,
    door_open,
    compressor_status,
    anomaly_mode,
    instant_risk_score,
    instant_risk_category
FROM ANALYTICS.FCT_CONTAINER_HEALTH
ORDER BY container_id, telemetry_timestamp ASC;