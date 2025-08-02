WITH sessions AS (
    SELECT *
    FROM {{ ref('stg_sessions') }}
)
SELECT *
FROM sessions