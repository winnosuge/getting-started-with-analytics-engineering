 WITH users_with_purchases AS
  (SELECT user_id ,
          CASE
              WHEN count(order_id) = 1 THEN 'one_purchase'
              WHEN count(order_id) = 2 THEN 'two_purchases'
              WHEN count(order_id) >= 3 THEN 'three_plus_purchases'
          END AS purchase
   FROM {{ ref('stg_greenery__orders') }}
   GROUP BY 1)
SELECT purchase ,
       count(1) AS user_count
FROM users_with_purchases
GROUP BY 1