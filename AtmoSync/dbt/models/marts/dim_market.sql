-- Mart Model: dim_market
-- Master dimension table for terminal markets, tier, coordinates, and storage capacities

SELECT
    market_id,
    market_name,
    latitude,
    longitude,
    state,
    tier,
    handling_fee_pct,
    cold_storage_capacity_tons
FROM {{ ref('stg_markets') }}
