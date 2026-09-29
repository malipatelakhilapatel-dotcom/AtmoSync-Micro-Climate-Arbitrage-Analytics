-- =====================================================================
-- AtmoSync: Seed Data Population
-- Maintained by: Member 2 (Snowflake & dbt)
-- =====================================================================

USE DATABASE ATMOSYNC_DB;
USE SCHEMA RAW;

-- 1. Insert Markets
MERGE INTO MARKETS AS target
USING (
    SELECT 'MKT_MUM' AS market_id, 'Mumbai' AS market_name, 19.0760 AS latitude, 72.8777 AS longitude, 'Maharashtra' AS state, 'Tier-1' AS tier, 0.025 AS handling_fee_pct, 5000 AS cold_storage_capacity_tons UNION ALL
    SELECT 'MKT_PUN', 'Pune', 18.5204, 73.8567, 'Maharashtra', 'Tier-1', 0.020, 3500 UNION ALL
    SELECT 'MKT_HYD', 'Hyderabad', 17.3850, 78.4867, 'Telangana', 'Tier-1', 0.022, 4000 UNION ALL
    SELECT 'MKT_BLR', 'Bengaluru', 12.9716, 77.5946, 'Karnataka', 'Tier-1', 0.025, 4800 UNION ALL
    SELECT 'MKT_MAA', 'Chennai', 13.0827, 80.2707, 'Tamil Nadu', 'Tier-1', 0.023, 3800 UNION ALL
    SELECT 'MKT_DEL', 'Delhi', 28.6139, 77.2090, 'Delhi', 'Tier-1', 0.028, 6500 UNION ALL
    SELECT 'MKT_SUR', 'Surat', 21.1702, 72.8311, 'Gujarat', 'Tier-2', 0.018, 2500 UNION ALL
    SELECT 'MKT_NSK', 'Nashik', 19.9975, 73.7898, 'Maharashtra', 'Tier-2', 0.015, 3000 UNION ALL
    SELECT 'MKT_NGP', 'Nagpur', 21.1458, 79.0882, 'Maharashtra', 'Tier-2', 0.019, 2800
) AS source
ON target.market_id = source.market_id
WHEN MATCHED THEN
    UPDATE SET market_name = source.market_name, latitude = source.latitude, longitude = source.longitude
WHEN NOT MATCHED THEN
    INSERT (market_id, market_name, latitude, longitude, state, tier, handling_fee_pct, cold_storage_capacity_tons)
    VALUES (source.market_id, source.market_name, source.latitude, source.longitude, source.state, source.tier, source.handling_fee_pct, source.cold_storage_capacity_tons);

-- 2. Insert Commodity Prices
MERGE INTO COMMODITY_PRICES AS target
USING (
    SELECT 'Mumbai' AS market, 'Avocado' AS commodity, 180.00 AS price_per_kg, 'INR' AS currency UNION ALL
    SELECT 'Mumbai', 'Banana', 35.00, 'INR' UNION ALL
    SELECT 'Mumbai', 'Tomato', 40.00, 'INR' UNION ALL
    SELECT 'Mumbai', 'Mango', 140.00, 'INR' UNION ALL
    SELECT 'Mumbai', 'Apple', 130.00, 'INR' UNION ALL
    SELECT 'Pune', 'Avocado', 195.00, 'INR' UNION ALL
    SELECT 'Pune', 'Banana', 32.00, 'INR' UNION ALL
    SELECT 'Pune', 'Tomato', 38.00, 'INR' UNION ALL
    SELECT 'Pune', 'Mango', 145.00, 'INR' UNION ALL
    SELECT 'Pune', 'Apple', 125.00, 'INR' UNION ALL
    SELECT 'Hyderabad', 'Avocado', 190.00, 'INR' UNION ALL
    SELECT 'Hyderabad', 'Banana', 30.00, 'INR' UNION ALL
    SELECT 'Hyderabad', 'Tomato', 45.00, 'INR' UNION ALL
    SELECT 'Hyderabad', 'Mango', 130.00, 'INR' UNION ALL
    SELECT 'Hyderabad', 'Apple', 135.00, 'INR' UNION ALL
    SELECT 'Bengaluru', 'Avocado', 210.00, 'INR' UNION ALL
    SELECT 'Bengaluru', 'Banana', 38.00, 'INR' UNION ALL
    SELECT 'Bengaluru', 'Tomato', 42.00, 'INR' UNION ALL
    SELECT 'Bengaluru', 'Mango', 150.00, 'INR' UNION ALL
    SELECT 'Bengaluru', 'Apple', 140.00, 'INR' UNION ALL
    SELECT 'Chennai', 'Avocado', 185.00, 'INR' UNION ALL
    SELECT 'Chennai', 'Banana', 34.00, 'INR' UNION ALL
    SELECT 'Chennai', 'Tomato', 44.00, 'INR' UNION ALL
    SELECT 'Chennai', 'Mango', 135.00, 'INR' UNION ALL
    SELECT 'Chennai', 'Apple', 138.00, 'INR' UNION ALL
    SELECT 'Delhi', 'Avocado', 220.00, 'INR' UNION ALL
    SELECT 'Delhi', 'Banana', 40.00, 'INR' UNION ALL
    SELECT 'Delhi', 'Tomato', 50.00, 'INR' UNION ALL
    SELECT 'Delhi', 'Mango', 160.00, 'INR' UNION ALL
    SELECT 'Delhi', 'Apple', 150.00, 'INR'
) AS source
ON target.market = source.market AND target.commodity = source.commodity
WHEN MATCHED THEN
    UPDATE SET price_per_kg = source.price_per_kg, last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (market, commodity, price_per_kg, currency, last_updated)
    VALUES (source.market, source.commodity, source.price_per_kg, source.currency, CURRENT_TIMESTAMP());

-- 3. Insert Routes
MERGE INTO ROUTES AS target
USING (
    SELECT 'RT_NSK_MUM' AS route_id, 'Nashik' AS origin, 'Mumbai' AS destination, 165.0 AS distance_km, 4.5 AS estimated_transit_hours, 450.0 AS toll_cost_inr, 0.88 AS road_condition_index UNION ALL
    SELECT 'RT_NSK_PUN', 'Nashik', 'Pune', 210.0, 5.5, 520.0, 0.85 UNION ALL
    SELECT 'RT_NSK_SUR', 'Nashik', 'Surat', 240.0, 6.0, 600.0, 0.82 UNION ALL
    SELECT 'RT_PUN_MUM', 'Pune', 'Mumbai', 150.0, 3.5, 400.0, 0.95 UNION ALL
    SELECT 'RT_PUN_HYD', 'Pune', 'Hyderabad', 560.0, 11.0, 1200.0, 0.84 UNION ALL
    SELECT 'RT_HYD_BLR', 'Hyderabad', 'Bengaluru', 575.0, 10.5, 1300.0, 0.90 UNION ALL
    SELECT 'RT_BLR_MAA', 'Bengaluru', 'Chennai', 350.0, 7.0, 850.0, 0.92 UNION ALL
    SELECT 'RT_DEL_MUM', 'Delhi', 'Mumbai', 1400.0, 28.0, 3200.0, 0.87 UNION ALL
    SELECT 'RT_NGP_MUM', 'Nagpur', 'Mumbai', 820.0, 16.0, 1800.0, 0.86 UNION ALL
    SELECT 'RT_NGP_HYD', 'Nagpur', 'Hyderabad', 500.0, 10.0, 1100.0, 0.85 UNION ALL
    SELECT 'RT_SUR_MUM', 'Surat', 'Mumbai', 280.0, 6.0, 650.0, 0.89
) AS source
ON target.route_id = source.route_id
WHEN MATCHED THEN
    UPDATE SET distance_km = source.distance_km, estimated_transit_hours = source.estimated_transit_hours
WHEN NOT MATCHED THEN
    INSERT (route_id, origin, destination, distance_km, estimated_transit_hours, toll_cost_inr, road_condition_index)
    VALUES (source.route_id, source.origin, source.destination, source.distance_km, source.estimated_transit_hours, source.toll_cost_inr, source.road_condition_index);

-- 4. Initial Sample Telemetry
INSERT INTO SENSOR_TELEMETRY (
    timestamp, container_id, shipment_id, commodity, temperature_c, humidity_pct,
    vibration_g, latitude, longitude, origin, destination, quantity_kg, speed_kmh,
    door_open, compressor_status, anomaly_mode
) VALUES
('2026-09-21 10:00:00', 'CONT_001', 'SHIP_001', 'Avocado', 5.80, 86.40, 0.08, 19.9975, 73.7898, 'Nashik', 'Mumbai', 12000, 62.5, FALSE, 'NORMAL', 'GRADUAL_THERMAL_RISE'),
('2026-09-21 11:00:00', 'CONT_001', 'SHIP_001', 'Avocado', 8.50, 91.20, 0.12, 19.5500, 73.4500, 'Nashik', 'Mumbai', 12000, 58.0, FALSE, 'DEGRADED', 'GRADUAL_THERMAL_RISE'),
('2026-09-21 12:00:00', 'CONT_001', 'SHIP_001', 'Avocado', 13.80, 96.50, 0.28, 19.2000, 73.2000, 'Nashik', 'Mumbai', 12000, 45.0, FALSE, 'FAILURE', 'GRADUAL_THERMAL_RISE'),
('2026-09-21 12:00:00', 'CONT_002', 'SHIP_002', 'Banana', 13.90, 92.00, 0.05, 18.5204, 73.8567, 'Pune', 'Hyderabad', 15000, 65.0, FALSE, 'NORMAL', 'NORMAL'),
('2026-09-21 12:00:00', 'CONT_003', 'SHIP_003', 'Mango', 11.20, 88.00, 0.09, 21.1458, 79.0882, 'Nagpur', 'Mumbai', 9500, 70.0, FALSE, 'NORMAL', 'NORMAL'),
('2026-09-21 12:00:00', 'CONT_004', 'SHIP_004', 'Tomato', 11.50, 88.50, 0.10, 19.9975, 73.7898, 'Nashik', 'Pune', 14000, 60.0, FALSE, 'NORMAL', 'NORMAL'),
('2026-09-21 12:00:00', 'CONT_005', 'SHIP_005', 'Apple', 2.50, 93.00, 0.07, 17.3850, 78.4867, 'Hyderabad', 'Bengaluru', 18000, 68.0, FALSE, 'NORMAL', 'NORMAL');
