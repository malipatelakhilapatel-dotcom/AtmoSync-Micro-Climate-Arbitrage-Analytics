-- Mart Model: fct_spoilage_arbitrage
-- Core Business Engine: Identifies at-risk containers, compares current vs alternative terminals,
-- and calculates Spoilage Arbitrage financial benefits and recommended reroute decisions.

WITH market_options AS (
    SELECT * FROM {{ ref('int_market_options') }}
),

-- 1. Current Destination Performance
current_destination_perf AS (
    SELECT
        container_id,
        shipment_id,
        commodity,
        quantity_kg,
        current_destination,
        spoilage_risk_score,
        risk_category,
        time_to_spoilage_hours,
        current_lat,
        current_lon,
        candidate_price_per_kg AS current_market_price,
        estimated_transit_hours AS current_transit_hours,
        projected_spoilage_rate AS current_spoilage_rate,
        projected_gross_value_inr AS current_gross_value_inr,
        net_realizable_value_inr AS current_expected_value_inr,
        ROUND(quantity_kg * candidate_price_per_kg * projected_spoilage_rate, 2) AS current_potential_loss_inr
    FROM market_options
    WHERE candidate_market = current_destination
),

-- 2. Ranked Alternative Destinations (Excluding Current Destination)
alternative_ranked AS (
    SELECT
        container_id,
        candidate_market AS alt_market,
        candidate_price_per_kg AS alt_price_per_kg,
        distance_km AS alt_distance_km,
        estimated_transit_hours AS alt_transit_hours,
        estimated_freight_cost_inr AS alt_freight_cost_inr,
        projected_spoilage_rate AS alt_spoilage_rate,
        net_realizable_value_inr AS alt_expected_value_inr,
        is_feasible_within_shelf_life,
        ROW_NUMBER() OVER (
            PARTITION BY container_id
            ORDER BY
                is_feasible_within_shelf_life DESC,
                net_realizable_value_inr DESC
        ) AS rank_alt
    FROM market_options
    WHERE candidate_market != current_destination
),

-- 3. Top Alternative Destination per Container
best_alternative AS (
    SELECT *
    FROM alternative_ranked
    WHERE rank_alt = 1
)

SELECT
    curr.container_id,
    curr.shipment_id,
    curr.commodity,
    curr.quantity_kg,
    curr.current_lat AS latitude,
    curr.current_lon AS longitude,
    curr.spoilage_risk_score,
    curr.risk_category,
    curr.time_to_spoilage_hours,

    -- Current Destination Logistics & Economics
    curr.current_destination,
    curr.current_transit_hours,
    curr.current_market_price,
    curr.current_expected_value_inr,
    curr.current_potential_loss_inr,

    -- Recommended Alternative Destination
    alt.alt_market AS recommended_destination,
    alt.alt_distance_km AS reroute_distance_km,
    alt.alt_transit_hours AS reroute_transit_hours,
    alt.alt_price_per_kg AS reroute_market_price,
    alt.alt_freight_cost_inr AS reroute_freight_cost_inr,
    alt.alt_expected_value_inr AS reroute_expected_value_inr,

    -- Financial Spoilage Arbitrage Opportunity
    ROUND(
        GREATEST(0.0, alt.alt_expected_value_inr - curr.current_expected_value_inr),
        2
    ) AS spoilage_arbitrage_inr,

    -- Total Potential Spoilage Loss Avoided
    ROUND(
        GREATEST(0.0, curr.current_potential_loss_inr - (curr.quantity_kg * alt.alt_price_per_kg * alt.alt_spoilage_rate)),
        2
    ) AS spoilage_avoided_value_inr,

    -- Actionable Decision Flag
    CASE
        WHEN (alt.alt_expected_value_inr - curr.current_expected_value_inr) > 1000.0
         AND curr.risk_category IN ('High Risk', 'Critical Risk')
         AND alt.is_feasible_within_shelf_life = 1
        THEN TRUE
        ELSE FALSE
    END AS is_reroute_recommended,

    -- Human-Readable Decision Rationale
    CASE
        WHEN (alt.alt_expected_value_inr - curr.current_expected_value_inr) > 1000.0
         AND curr.risk_category IN ('High Risk', 'Critical Risk')
         AND alt.is_feasible_within_shelf_life = 1
        THEN 'Closer terminal (' || alt.alt_market || ' in ' || CAST(alt.alt_transit_hours AS VARCHAR) || 'h) prevents decay; unlocks +' || CAST(ROUND(alt.alt_expected_value_inr - curr.current_expected_value_inr, 0) AS VARCHAR) || ' INR net arbitrage.'
        WHEN curr.risk_category IN ('High Risk', 'Critical Risk') AND alt.is_feasible_within_shelf_life = 0
        THEN 'Critical deterioration but alternative terminal transit exceeds remaining shelf life. Expedite current delivery.'
        ELSE 'Conditions nominal. Proceed along primary planned corridor.'
    END AS recommendation_reason

FROM current_destination_perf curr
INNER JOIN best_alternative alt
    ON curr.container_id = alt.container_id
