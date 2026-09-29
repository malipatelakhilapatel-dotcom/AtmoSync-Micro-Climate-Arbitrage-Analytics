-- Staging Model: stg_markets
-- Standardizes wholesale market metadata, geographic coordinates, and fee structures

WITH source_markets AS (
    SELECT
        TRIM(market_id) AS market_id,
        TRIM(market_name) AS market_name,
        CAST(latitude AS FLOAT) AS latitude,
        CAST(longitude AS FLOAT) AS longitude,
        TRIM(state) AS state,
        TRIM(tier) AS tier,
        CAST(COALESCE(handling_fee_pct, 0.02) AS FLOAT) AS handling_fee_pct,
        CAST(COALESCE(cold_storage_capacity_tons, 3000) AS INTEGER) AS cold_storage_capacity_tons
    FROM {{ source('raw', 'markets') }}
    WHERE market_id IS NOT NULL
)

SELECT
    market_id,
    market_name,
    latitude,
    longitude,
    state,
    tier,
    handling_fee_pct,
    cold_storage_capacity_tons
FROM source_markets