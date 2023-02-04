 WITH users_with_purchases AS (
    SELECT user_id 
          , count(order_id) as orders
          , CASE
              WHEN count(order_id) = 1 THEN 'single_purchase'
              WHEN count(order_id) >= 2 THEN 'repeat_purchases'
          END AS purchase_group
    FROM {{ ref('stg_greenery__orders') }}
    GROUP BY 1
)

SELECT COUNT(CASE WHEN purchase_group = 'repeat_purchases' THEN user_id ELSE NULL END)/CAST(COUNT(user_id) AS float) as users_purchase_rate
FROM users_with_purchases