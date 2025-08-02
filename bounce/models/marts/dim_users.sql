WITH users AS (
    SELECT *
    FROM {{ ref('stg_users') }}
)
SELECT *
FROM users