WITH events AS (
    SELECT *
    FROM {{ ref('stg_events') }}
)
, sessions AS (
    SELECT *
    FROM {{ ref('dim_sessions') }}
)
, exchange_rates AS (
    SELECT *
    FROM {{ ref('stg_exchange_rates') }}
)
, user_regions AS (
    SELECT *
    FROM {{ ref('dim_users') }}
)
, final AS (
    SELECT
        e.event_id,
        e.user_id,
        e.session_id,
        s.session,
        e.event_type,
        e.event_time,
        e.revenue,
        ROUND(er.rate_to_usd, 2) AS rate_to_usd,
        ROUND(e.revenue * er.rate_to_usd, 2) AS revenue_usd,
        e.currency,
        e.utm_source,
        s.session_start,
        s.session_end,
        u.region
    FROM events AS e
        LEFT JOIN sessions AS s
        ON e.session_id = s.session_id

        LEFT JOIN exchange_rates AS er
        ON e.event_time::DATE = er.date
            AND e.currency = er.currency

        LEFT JOIN user_regions AS u
        ON e.user_id = u.user_id
            AND e.event_time BETWEEN u.valid_from AND u.valid_to
)
SELECT *
FROM final