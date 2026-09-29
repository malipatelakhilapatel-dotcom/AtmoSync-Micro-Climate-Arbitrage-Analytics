-- Staging Model: stg_sensor_telemetry
-- Cleans, typecasts, normalizes timestamps, and tags preliminary threshold violations

WITH source_data AS (
    SELECT
        record_id,
        TRY_TO_TIMESTAMP_NTZ(timestamp) AS telemetry_timestamp,
        TRIM(container_id) AS container_id,
        TRIM(shipment_id) AS shipment_id,
        TRIM(commodity) AS commodity,
        CAST(temperature_c AS FLOAT) AS temperature_c,
        CAST(humidity_pct AS FLOAT) AS humidity_pct,
        CAST(vibration_g AS FLOAT) AS vibration_g,
        CAST(latitude AS FLOAT) AS latitude,
        CAST(longitude AS FLOAT) AS longitude,
        TRIM(origin) AS origin,
        TRIM(destination) AS destination,
        CAST(quantity_kg AS INTEGER) AS quantity_kg,
        CAST(COALESCE(speed_kmh, 50.0) AS FLOAT) AS speed_kmh,
        COALESCE(door_open, FALSE) AS door_open,
        COALESCE(TRIM(compressor_status), 'NORMAL') AS compressor_status,
        COALESCE(TRIM(anomaly_mode), 'NORMAL') AS anomaly_mode,
        ingestion_timestamp
    FROM {{ source('raw', 'sensor_telemetry') }}
    WHERE container_id IS NOT NULL
      AND timestamp IS NOT NULL
)

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
    origin,
    destination,
    quantity_kg,
    speed_kmh,
    door_open,
    compressor_status,
    anomaly_mode,
    ingestion_timestamp,

    -- Commodity-specific threshold evaluations
    CASE
        WHEN commodity = 'Avocado' AND (temperature_c < 2.0 OR temperature_c > 12.0) THEN 1
        WHEN commodity = 'Banana'  AND (temperature_c < 11.5 OR temperature_c > 18.0) THEN 1
        WHEN commodity = 'Tomato'  AND (temperature_c < 8.0 OR temperature_c > 18.0) THEN 1
        WHEN commodity = 'Mango'   AND (temperature_c < 9.0 OR temperature_c > 16.0) THEN 1
        WHEN commodity = 'Apple'   AND (temperature_c < -1.0 OR temperature_c > 8.0) THEN 1
        ELSE 0
    END AS is_temperature_alert,

    CASE
        WHEN commodity = 'Avocado' AND (humidity_pct < 75.0 OR humidity_pct > 95.0) THEN 1
        WHEN commodity = 'Banana'  AND (humidity_pct < 80.0 OR humidity_pct > 98.0) THEN 1
        WHEN commodity = 'Tomato'  AND (humidity_pct < 75.0 OR humidity_pct > 95.0) THEN 1
        WHEN commodity = 'Mango'   AND (humidity_pct < 75.0 OR humidity_pct > 95.0) THEN 1
        WHEN commodity = 'Apple'   AND (humidity_pct < 80.0 OR humidity_pct > 98.0) THEN 1
        ELSE 0
    END AS is_humidity_alert,

    CASE
        WHEN vibration_g >= 0.20 THEN 1
        ELSE 0
    END AS is_vibration_alert,

    -- Composite immediate risk trigger
    CASE
        WHEN compressor_status IN ('DEGRADED', 'FAILURE') THEN 1
        WHEN vibration_g >= 0.25 THEN 1
        ELSE 0
    END AS is_mechanical_alert

FROM source_data
