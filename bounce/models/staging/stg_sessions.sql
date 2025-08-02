WITH source AS (
    SELECT *
    FROM {{ ref('raw_events') }}
),
cleaned AS (
    SELECT
        session_id,
        session,
        MIN(TO_TIMESTAMP_NTZ(REPLACE(event_time, ' UTC', ''))) AS session_start,
        MAX(TO_TIMESTAMP_NTZ(REPLACE(event_time, ' UTC', ''))) AS session_end
    FROM source
    GROUP BY
        session_id,
        session
)
SELECT * 
FROM cleaned