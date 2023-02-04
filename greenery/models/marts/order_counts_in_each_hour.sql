WITH order_counts_in_each_hour AS
  ( SELECT date_trunc('hour', created_at) ,
           count(order_id) AS order_count
   FROM {{ ref('stg_greenery__orders') }}
   GROUP BY 1 )
SELECT avg(order_count)
FROM order_counts_in_each_hour