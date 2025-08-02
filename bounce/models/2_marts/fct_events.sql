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
, final AS (
    SELECT
        e.event_id,
        e.user_id,
        e.session_id,
        s.session,
        e.event_time,
        e.revenue,
        er.rate_to_usd,
        e.revenue * er.rate_to_usd AS revenue_usd,
        e.currency,
        e.utm_source,
        s.session_start,
        s.session_end
    FROM events AS e
        LEFT JOIN sessions AS s
        ON e.session_id = s.session_id

        LEFT JOIN exchange_rates AS er
        ON e.event_time::DATE = er.date
            AND e.currency = er.currency
)
SELECT *
FROM final