-- Staging Model: stg_routes
-- Standardizes origin-destination logistics corridors, distances, and transit hours

WITH source_routes AS (
    SELECT
        TRIM(route_id) AS route_id,
        TRIM(origin) AS origin,
        TRIM(destination) AS destination,
        CAST(distance_km AS FLOAT) AS distance_km,
        CAST(estimated_transit_hours AS FLOAT) AS estimated_transit_hours,
        CAST(COALESCE(toll_cost_inr, 0.0) AS FLOAT) AS toll_cost_inr,
        CAST(COALESCE(road_condition_index, 0.85) AS FLOAT) AS road_condition_index
    FROM {{ source('raw', 'routes') }}
    WHERE route_id IS NOT NULL
)

SELECT
    route_id,
    origin,
    destination,
    distance_km,
    estimated_transit_hours,
    toll_cost_inr,
    road_condition_index
FROM source_routes
