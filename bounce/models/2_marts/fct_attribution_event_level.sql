WITH base_events AS (
    SELECT *
    FROM {{ ref('fct_events') }}
)
-- Step 1: Determine session start and end per session
, session_bounds AS (
    SELECT
        session_id,
        user_id,
        MIN(event_time) AS session_start,
        MAX(event_time) AS session_end
    FROM base_events
    GROUP BY 
        session_id,
        user_id
)
-- Step 2: Assign session_utm_source as first utm_source in the session
, session_first_utm AS (
    SELECT
        session_id,
        utm_source,
        ROW_NUMBER() OVER (
            PARTITION BY
                session_id
            ORDER BY
                event_time
        ) AS rn
    FROM base_events
    WHERE utm_source IS NOT NULL
)
, session_sources AS (
    SELECT
        b.session_id,
        b.user_id,
        b.session_start,
        b.session_end,
        f.utm_source AS session_utm_source
    FROM session_bounds AS b
    LEFT JOIN session_first_utm AS f
      ON b.session_id = f.session_id
    QUALIFY f.rn = 1
)
-- 3. Add session source to each event
, events_with_session_source AS (
    SELECT
        e.*,
        s.session_utm_source
    FROM base_events AS e
    LEFT JOIN session_sources AS s
      ON e.session_id = s.session_id
)
-- 4. Revenue events = Order Completed with revenue
, revenue_events AS (
    SELECT 
        b.*,
        s.session_utm_source
    FROM base_events AS b
    LEFT JOIN session_sources AS s
      ON b.session_id = s.session_id
    WHERE b.event_type = 'Order Completed'
)
-- 5. Last-touch: most recent prior session with non-null utm_source
, last_touch_candidates AS (
    SELECT
        r.event_id,
        r.user_id,
        r.event_time,
        s.session_start,
        s.session_end,
        s.session_utm_source,
        ROW_NUMBER() OVER (
            PARTITION BY r.event_id
            ORDER BY s.session_end DESC
        ) AS rn
    FROM revenue_events AS r
    INNER JOIN session_sources AS s
    ON r.user_id = s.user_id
        AND s.session_start < r.event_time
        AND s.session_utm_source IS NOT NULL
)
, last_touch_from_prior_session AS (
    SELECT
        event_id,
        user_id,
        session_utm_source AS last_touch_utm_source
    FROM last_touch_candidates
    QUALIFY rn = 1
)
-- 6. Fallback: use utm_source from the order itself
, last_touch_fallback AS (
    SELECT
        event_id,
        utm_source AS last_touch_utm_source
    FROM revenue_events
)
-- 7. First-touch: first session within 30 days before order, ordered by session_start ASC
, first_touch_candidates AS (
    SELECT
        r.event_id,
        s.session_utm_source,
        s.session_start,
        ROW_NUMBER() OVER (
            PARTITION BY r.event_id
            ORDER BY s.session_start ASC
        ) AS rn
    FROM revenue_events AS r
    INNER JOIN session_sources AS s
      ON r.user_id = s.user_id
     AND s.session_start BETWEEN r.event_time - INTERVAL '30 days' AND r.event_time
     AND s.session_utm_source IS NOT NULL
)

, first_touch_from_prior_sessions AS (
    SELECT
        event_id,
        session_utm_source AS first_touch_utm_source
    FROM first_touch_candidates
    QUALIFY rn = 1
)
-- 8. Final output: combine revenue events with both attributions
, final AS (
    SELECT
        r.*,
        COALESCE(lt.last_touch_utm_source, lf.last_touch_utm_source) AS final_last_touch_utm_source,
        ft.first_touch_utm_source AS final_first_touch_utm_source
    FROM revenue_events AS r
        LEFT JOIN last_touch_from_prior_session AS lt
        ON r.event_id = lt.event_id
    
        LEFT JOIN last_touch_fallback AS lf
        ON r.event_id = lf.event_id
    
        LEFT JOIN first_touch_from_prior_sessions AS ft
        ON r.event_id = ft.event_id
)
SELECT *
FROM final