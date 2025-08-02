WITH source AS (
    SELECT *
    FROM {{ ref('raw_events') }}
),
cleaned AS (
    SELECT
        event_id,
        user_id,
        session_id,
        TO_TIMESTAMP_NTZ(REPLACE(event_time, ' UTC', '')) AS event_time,
        revenue,
        currency,
        utm_source
    FROM source
)

SELECT * FROM cleaned