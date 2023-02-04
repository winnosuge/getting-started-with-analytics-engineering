SELECT COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id ELSE NULL END)/CAST(COUNT(DISTINCT session_id) AS float) AS add_to_cart_rate
  FROM {{ ref('stg_greenery__events') }}