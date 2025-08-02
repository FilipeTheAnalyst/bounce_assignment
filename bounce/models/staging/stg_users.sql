WITH source AS (
    SELECT * 
    FROM {{ ref('raw_events') }}
),
cleaned AS (
    SELECT
        user_id
    FROM source
    GROUP BY
        user_id
)
SELECT *
FROM cleaned