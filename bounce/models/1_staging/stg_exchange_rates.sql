WITH source AS (
    SELECT *
    FROM {{ ref('exchange_rates') }}
),
cleaned AS (
    SELECT
        date,
        currency,
        rate_to_usd
    FROM source
)
SELECT *
FROM cleaned