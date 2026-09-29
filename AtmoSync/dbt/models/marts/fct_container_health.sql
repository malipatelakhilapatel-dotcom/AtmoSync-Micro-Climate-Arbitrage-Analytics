-- Mart Model: fct_container_health
-- Fact table tracking container sensor metrics and environmental health over time

WITH raw_telemetry AS (
    SELECT * FROM {{ ref('stg_sensor_telemetry') }}
),

scored AS (
    SELECT
        t.record_id,
        t.telemetry_timestamp,
        t.container_id,
        t.shipment_id,
        t.commodity,
        t.temperature_c,
        t.humidity_pct,
        t.vibration_g,
        t.latitude,
        t.longitude,
        t.origin,
        t.destination,
        t.quantity_kg,
        t.speed_kmh,
        t.door_open,
        t.compressor_status,
        t.anomaly_mode,
        t.is_temperature_alert,
        t.is_humidity_alert,
        t.is_vibration_alert,
        t.is_mechanical_alert,

        -- Quick instantaneous score for time-series charts
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    (t.is_temperature_alert * 45.0) +
                    (t.is_humidity_alert * 20.0) +
                    (t.is_vibration_alert * 20.0) +
                    (CASE t.compressor_status WHEN 'FAILURE' THEN 25.0 WHEN 'DEGRADED' THEN 12.0 ELSE 0.0 END)
                )
            ),
            1
        ) AS instant_risk_score
    FROM raw_telemetry t
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
    is_temperature_alert,
    is_humidity_alert,
    is_vibration_alert,
    is_mechanical_alert,
    instant_risk_score,
    CASE
        WHEN instant_risk_score <= 25.0 THEN 'Low Risk'
        WHEN instant_risk_score <= 50.0 THEN 'Moderate Risk'
        WHEN instant_risk_score <= 75.0 THEN 'High Risk'
        ELSE 'Critical Risk'
    END AS instant_risk_category
FROM scored
