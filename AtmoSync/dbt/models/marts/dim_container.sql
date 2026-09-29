-- Mart Model: dim_container
-- Master dimension table representing containers, assigned routes, and cargo specifications

WITH ranked_telemetry AS (
    SELECT
        container_id,
        shipment_id,
        commodity,
        quantity_kg,
        origin,
        destination,
        compressor_status,
        telemetry_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY container_id
            ORDER BY telemetry_timestamp DESC
        ) AS rn
    FROM {{ ref('stg_sensor_telemetry') }}
)

SELECT
    container_id,
    shipment_id,
    commodity,
    quantity_kg,
    origin,
    destination,
    compressor_status,
    telemetry_timestamp AS last_reported_at
FROM ranked_telemetry
WHERE rn = 1