-- Intermediate Model: int_spoilage_risk
-- Computes the deterministic Spoilage Risk Score (0-100) and Estimated Hours to Spoilage

WITH health AS (
    SELECT * FROM {{ ref('int_container_health') }}
),

scored_health AS (
    SELECT
        h.*,

        -- 1. Thermal excursion penalty (up to 50 pts)
        LEAST(50.0, h.temp_excursion_c * 8.5) AS penalty_temp,

        -- 2. Humidity deviation penalty (up to 20 pts)
        CASE WHEN h.is_humidity_alert = 1 THEN 20.0 ELSE 0.0 END AS penalty_humidity,

        -- 3. Mechanical shock penalty (up to 20 pts)
        LEAST(20.0, GREATEST(0.0, (h.vibration_g - 0.15) * 60.0)) AS penalty_vibration,

        -- 4. Active refrigeration component penalty (up to 25 pts)
        CASE h.compressor_status
            WHEN 'FAILURE'  THEN 25.0
            WHEN 'DEGRADED' THEN 12.0
            ELSE 0.0
        END AS penalty_compressor
    FROM health h
),

computed_risk AS (
    SELECT
        s.*,
        LEAST(
            100.0,
            GREATEST(
                0.0,
                s.penalty_temp + s.penalty_humidity + s.penalty_vibration + s.penalty_compressor
            )
        ) AS raw_spoilage_score
    FROM scored_health s
)

SELECT
    c.record_id,
    c.telemetry_timestamp,
    c.container_id,
    c.shipment_id,
    c.commodity,
    c.temperature_c,
    c.humidity_pct,
    c.vibration_g,
    c.latitude,
    c.longitude,
    c.origin,
    c.destination,
    c.quantity_kg,
    c.speed_kmh,
    c.compressor_status,
    c.anomaly_mode,
    c.baseline_shelf_life_hours,
    c.temp_excursion_c,

    ROUND(c.raw_spoilage_score, 1) AS spoilage_risk_score,

    -- Standardized Risk Category
    CASE
        WHEN c.raw_spoilage_score <= 25.0 THEN 'Low Risk'
        WHEN c.raw_spoilage_score <= 50.0 THEN 'Moderate Risk'
        WHEN c.raw_spoilage_score <= 75.0 THEN 'High Risk'
        ELSE 'Critical Risk'
    END AS risk_category,

    -- Deterministic Time-to-Spoilage Hours based on Arrhenius-type degradation curve
    ROUND(
        GREATEST(
            1.0,
            c.baseline_shelf_life_hours * POWER(GREATEST(0.01, 1.0 - (c.raw_spoilage_score / 100.0)), 1.85)
        ),
        1
    ) AS time_to_spoilage_hours

FROM computed_risk c
