-- Intermediate Model: int_market_options
-- Cross-evaluates active containers against potential market destinations, transit feasibility, and net realizable value

WITH containers AS (
    SELECT * FROM {{ ref('int_spoilage_risk') }}
),

markets AS (
    SELECT * FROM {{ ref('stg_markets') }}
),

prices AS (
    SELECT * FROM {{ ref('stg_commodity_prices') }}
),

market_candidates AS (
    SELECT
        c.container_id,
        c.shipment_id,
        c.commodity,
        c.origin,
        c.destination AS current_destination,
        c.quantity_kg,
        c.latitude AS current_lat,
        c.longitude AS current_lon,
        c.speed_kmh,
        c.spoilage_risk_score,
        c.risk_category,
        c.time_to_spoilage_hours,

        m.market_id AS candidate_market_id,
        m.market_name AS candidate_market,
        m.latitude AS candidate_lat,
        m.longitude AS candidate_lon,
        m.tier AS market_tier,
        cp.price_per_kg AS candidate_price_per_kg,

        -- Approximate distance in km using equirectangular projection
        ROUND(
            111.0 * SQRT(
                POWER(m.latitude - c.latitude, 2) +
                POWER((m.longitude - c.longitude) * COS(c.latitude * 3.14159265359 / 180.0), 2)
            ),
            1
        ) AS distance_km

    FROM containers c
    CROSS JOIN markets m
    INNER JOIN prices cp
        ON cp.market = m.market_name
       AND cp.commodity = c.commodity
    WHERE m.market_name != c.origin  -- Do not turn around back to origin farm/packhouse
),

evaluated_candidates AS (
    SELECT
        mc.*,

        -- Transit hours to candidate terminal at current container velocity
        ROUND(
            GREATEST(0.5, mc.distance_km / GREATEST(25.0, mc.speed_kmh)),
            1
        ) AS estimated_transit_hours,

        -- Incremental freight & refrigeration fuel cost (₹0.015 per km-kg)
        ROUND(mc.distance_km * 0.015 * mc.quantity_kg, 2) AS estimated_freight_cost_inr

    FROM market_candidates mc
),

projected_outcomes AS (
    SELECT
        ec.*,

        -- Projected spoilage loss percentage upon arrival at candidate terminal
        CASE
            WHEN ec.estimated_transit_hours >= ec.time_to_spoilage_hours THEN
                LEAST(
                    1.0,
                    0.35 + (0.65 * (ec.estimated_transit_hours - ec.time_to_spoilage_hours) / ec.estimated_transit_hours)
                )
            ELSE
                ROUND((ec.spoilage_risk_score / 100.0) * 0.15, 3)
        END AS projected_spoilage_rate,

        -- Flag if market is reachable within safe remaining shelf life
        CASE
            WHEN ec.estimated_transit_hours < ec.time_to_spoilage_hours THEN 1
            ELSE 0
        END AS is_feasible_within_shelf_life

    FROM evaluated_candidates ec
)

SELECT
    po.*,

    -- Projected Gross Cargo Value (accounting for spoilage decay)
    ROUND(
        po.quantity_kg * po.candidate_price_per_kg * (1.0 - po.projected_spoilage_rate),
        2
    ) AS projected_gross_value_inr,

    -- Net Realizable Value = Gross Cargo Value - Incremental Freight Cost
    ROUND(
        (po.quantity_kg * po.candidate_price_per_kg * (1.0 - po.projected_spoilage_rate)) - po.estimated_freight_cost_inr,
        2
    ) AS net_realizable_value_inr

FROM projected_outcomes po
