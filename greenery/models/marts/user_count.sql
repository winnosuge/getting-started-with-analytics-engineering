SELECT count(DISTINCT user_id) AS value
FROM {{ ref('stg_greenery__users') }}