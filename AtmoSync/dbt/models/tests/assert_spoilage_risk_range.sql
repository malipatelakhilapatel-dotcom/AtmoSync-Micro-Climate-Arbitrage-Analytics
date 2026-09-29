-- Singular Test: assert_arbitrage_non_negative_for_reroute
-- Fails if any container is recommended for rerouting with a non-positive arbitrage benefit

SELECT
    container_id,
    recommended_destination,
    spoilage_arbitrage_inr,
    is_reroute_recommended
FROM {{ ref('fct_spoilage_arbitrage') }}
WHERE is_reroute_recommended = TRUE
  AND spoilage_arbitrage_inr <= 0
