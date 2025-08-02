WITH events AS (
    SELECT *
    FROM {{ ref('stg_events') }}
)
, sessions AS (
    SELECT *
    FROM {{ ref('dim_sessions') }}
)
SELECT
    e.event_id,
    e.user_id,
    e.session_id,
    s.session,
    e.event_time,
    e.revenue,
    e.currency,
    e.utm_source,
    s.session_start,
    s.session_end
FROM events AS e
LEFT JOIN sessions AS s
    ON e.session_id = s.session_id