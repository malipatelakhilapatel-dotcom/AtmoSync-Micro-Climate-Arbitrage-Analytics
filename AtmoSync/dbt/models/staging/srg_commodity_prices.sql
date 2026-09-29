-- Staging Model: stg_commodity_prices
-- Normalizes market wholesale prices and formats timestamps

WITH source_prices AS (
    SELECT
        TRIM(market) AS market,
        TRIM(commodity) AS commodity,
        CAST(price_per_kg AS FLOAT) AS price_per_kg,
        COALESCE(TRIM(currency), 'INR') AS currency,
        TRY_TO_TIMESTAMP_NTZ(last_updated) AS price_timestamp
    FROM {{ source('raw', 'commodity_prices') }}
    WHERE market IS NOT NULL
      AND commodity IS NOT NULL
      AND price_per_kg > 0
)

SELECT
    market,
    commodity,
    price_per_kg,
    currency,
    COALESCE(price_timestamp, CURRENT_TIMESTAMP()) AS price_timestamp
FROM source_prices
