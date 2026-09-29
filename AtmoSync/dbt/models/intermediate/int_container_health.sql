-- Intermediate Model: int_container_health
-- Evaluates current micro-climate state, excursions, and mechanical stability per container

WITH telemetry AS (
    SELECT * FROM {{ ref('stg_sensor_telemetry') }}
),

ranked_telemetry AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY container_id
            ORDER BY telemetry_timestamp DESC
        ) AS recency_rank
    FROM telemetry
),

latest_state AS (
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
        is_mechanical_alert
    FROM ranked_telemetry
    WHERE recency_rank = 1
)

SELECT
    l.*,

    -- Commodity baseline biological envelope thresholds
    CASE l.commodity
        WHEN 'Avocado' THEN 5.5
        WHEN 'Banana'  THEN 14.0
        WHEN 'Tomato'  THEN 11.5
        WHEN 'Mango'   THEN 11.5
        WHEN 'Apple'   THEN 2.0
        ELSE 10.0
    END AS optimal_temp_center,

    CASE l.commodity
        WHEN 'Avocado' THEN 7.0
        WHEN 'Banana'  THEN 15.0
        WHEN 'Tomato'  THEN 13.0
        WHEN 'Mango'   THEN 13.0
        WHEN 'Apple'   THEN 4.0
        ELSE 12.0
    END AS optimal_temp_max,

    CASE l.commodity
        WHEN 'Avocado' THEN 4.0
        WHEN 'Banana'  THEN 13.0
        WHEN 'Tomato'  THEN 10.0
        WHEN 'Mango'   THEN 10.0
        WHEN 'Apple'   THEN 0.0
        ELSE 8.0
    END AS optimal_temp_min,

    CASE l.commodity
        WHEN 'Avocado' THEN 168.0  -- 7 days
        WHEN 'Banana'  THEN 120.0  -- 5 days
        WHEN 'Tomato'  THEN 144.0  -- 6 days
        WHEN 'Mango'   THEN 120.0  -- 5 days
        WHEN 'Apple'   THEN 720.0  -- 30 days
        ELSE 168.0
    END AS baseline_shelf_life_hours,

    -- Absolute thermal excursion beyond optimal envelope
    GREATEST(
        0.0,
        l.temperature_c - (
            CASE l.commodity
                WHEN 'Avocado' THEN 7.0
                WHEN 'Banana'  THEN 15.0
                WHEN 'Tomato'  THEN 13.0
                WHEN 'Mango'   THEN 13.0
                WHEN 'Apple'   THEN 4.0
                ELSE 12.0
            END
        ),
        (
            CASE l.commodity
                WHEN 'Avocado' THEN 4.0
                WHEN 'Banana'  THEN 13.0
                WHEN 'Tomato'  THEN 10.0
                WHEN 'Mango'   THEN 10.0
                WHEN 'Apple'   THEN 0.0
                ELSE 8.0
            END
        ) - l.temperature_c
    ) AS temp_excursion_c

FROM latest_state l
