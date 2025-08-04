WITH source AS (
    SELECT * 
    FROM {{ ref('raw_events') }}
),

currency_region_map AS (
    SELECT 'USD' AS currency, 'United States' AS region
    UNION ALL
    SELECT 'EUR', 'Europe'
    UNION ALL
    SELECT 'AUD', 'Australia'
),

-- Step 1: All users (regardless of currency)
all_users AS (
    SELECT DISTINCT user_id
    FROM source
),

-- Step 2: Events with currency info
user_currency_events AS (
    SELECT
        user_id,
        currency,
        TO_TIMESTAMP_NTZ(REPLACE(event_time, ' UTC', '')) AS event_time,
    FROM source
    WHERE currency IS NOT NULL
),

-- Step 3: Deduplicate user-date-currency
deduplicated AS (
    SELECT DISTINCT
        user_id,
        currency,
        CAST(event_time AS DATE) AS event_date
    FROM user_currency_events
),

-- Step 4: Rank and find change points
ranked_events AS (
    SELECT
        user_id,
        currency,
        event_date,
        LAG(currency) OVER (PARTITION BY user_id ORDER BY event_date) AS previous_currency
    FROM deduplicated
),

change_points AS (
    SELECT
        user_id,
        currency,
        event_date AS valid_from
    FROM ranked_events
    WHERE currency != previous_currency OR previous_currency IS NULL
),

scd_windows AS (
    SELECT
        c.user_id,
        c.currency,
        c.valid_from,
        LEAD(c.valid_from) OVER (
            PARTITION BY c.user_id ORDER BY c.valid_from
        ) - INTERVAL '1 day' AS valid_to
    FROM change_points c
),

-- Step 5: Add region
scd_enriched AS (
    SELECT
        w.user_id,
        m.region,
        w.currency,
        w.valid_from,
        COALESCE(w.valid_to, '9999-12-31') AS valid_to
    FROM scd_windows AS w
    LEFT JOIN currency_region_map AS m
      ON w.currency = m.currency
),

-- Step 6: Find users with no currency history
users_with_currency AS (
    SELECT DISTINCT user_id FROM user_currency_events
),

users_without_currency AS (
    SELECT
        u.user_id,
        NULL AS currency,
        'Unknown' AS region,
        MIN(TO_TIMESTAMP_NTZ(REPLACE(s.event_time, ' UTC', '')))::DATE AS valid_from,
        '9999-12-31' AS valid_to
    FROM all_users AS u
        LEFT JOIN source AS s 
        ON u.user_id = s.user_id
    WHERE u.user_id NOT IN (SELECT user_id FROM users_with_currency)
    GROUP BY u.user_id
),

-- Step 7: Combine both
final AS (
    SELECT * FROM scd_enriched
    UNION ALL
    SELECT * FROM users_without_currency
)

SELECT *
FROM final